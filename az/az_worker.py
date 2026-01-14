import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import subprocess
import shutil
import tempfile

from common.sheets import (
    read_pending_rows,
    get_max_assigned_number,
    update_row
)
from common.archive import upload_file

TAB_NAME = "AZ"

def az_download_logic(title, link, number, identifier):
    temp_dir = tempfile.mkdtemp(prefix="az_")

    base_url = "https://www.aznude.com"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        # Step 1: extract video pages
        resp = requests.get(link, headers=headers, timeout=30)
        soup = BeautifulSoup(resp.text, "html.parser")

        video_pages = [
            urljoin(base_url, a["href"])
            for a in soup.find_all("a", href=True)
            if a["href"].startswith("/azncdn/")
        ]

        if not video_pages:
            return False, "No video pages found"

        # Step 2: extract mp4 links
        mp4_links = []
        for vp in video_pages:
            r = requests.get(vp, headers=headers, timeout=30)
            s = BeautifulSoup(r.text, "html.parser")
            for a in s.find_all("a", href=True):
                if a.find("button", class_="single-video-download-btn"):
                    mp4_links.append(a["href"])

        if not mp4_links:
            return False, "No MP4 links found"

        # Step 3: download parts
        parts = []
        for i, url in enumerate(mp4_links):
            path = os.path.join(temp_dir, f"part_{i:02d}.mp4")
            r = requests.get(url, stream=True, timeout=60)
            with open(path, "wb") as f:
                for chunk in r.iter_content(1024 * 1024):
                    f.write(chunk)
            parts.append(path)

        # Step 4: merge (quiet ffmpeg)
        list_file = os.path.join(temp_dir, "files.txt")
        with open(list_file, "w") as f:
            for p in parts:
                f.write(f"file '{p}'\n")

        output = os.path.join(
            temp_dir, f"{number:02d} - {title}.mp4"
        )

        subprocess.run(
            [
                "ffmpeg",
                "-loglevel", "error",
                "-f", "concat",
                "-safe", "0",
                "-i", list_file,
                "-c", "copy",
                output
            ],
            check=True
        )

        # Step 5: upload
        return upload_file(output, identifier)

    except Exception as e:
        return False, str(e)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

def main():
    rows = read_pending_rows(TAB_NAME)
    current_number = get_max_assigned_number(TAB_NAME)

    for r in rows:
        row_num = r["row"]
        title = r["Title"]
        link = r["Link"]
        identifier = r["Identifier"]

        next_number = current_number + 1

        success, msg = az_download_logic(
            title, link, next_number, identifier
        )

        if success:
            update_row(
                TAB_NAME,
                row_num,
                "DONE",
                next_number,
                ""
            )
            current_number += 1
        else:
            update_row(
                TAB_NAME,
                row_num,
                "FAILED",
                "",
                msg
            )

if __name__ == "__main__":
    main()
