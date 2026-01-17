import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
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


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

BASE_URL = "https://www.aznude.com"


def get_video_detail_pages(main_html):
    """
    Extract only HTML video detail pages from the main content container
    (2026 more reliable version - ignores images, focuses on video items)
    """
    soup = BeautifulSoup(main_html, "html.parser")

    # Try to limit scope to main content area
    container = soup.find("div", class_="single-page_content-container")
    if not container:
        container = soup  # fallback

    candidates = set()

    for a in container.find_all("a", href=True):
        href = a["href"].strip()

        # Immediately skip obvious image links
        if href.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
            continue

        # Strongest indicators of video detail page
        if a.get("data-type") == "video":
            candidates.add(href)
            continue

        eid = a.get("eid", "")
        if eid and "/embed/" in eid:
            clean_path = eid.lstrip("/")
            if clean_path.endswith('.html'):
                candidates.add(clean_path)
            continue

        # Only consider links that end with .html
        if href.endswith('.html'):
            # Hash-based video pages (very common now)
            if re.search(r'[a-f0-9]{8,}-?hd', href, re.I):
                candidates.add(href)
            # mrskin style paths
            elif '/mrskin/' in href:
                candidates.add(href)
            # old azncdn style (still exists sometimes)
            elif '/azncdn/' in href:
                candidates.add(href)

    # Convert to full URLs + remove duplicates
    full_urls = []
    seen = set()
    for path in candidates:
        full = urljoin(BASE_URL, path)
        if full not in seen:
            seen.add(full)
            full_urls.append(full)

    return full_urls


def find_mp4_on_detail_page(detail_url):
    """
    Try to find direct .mp4 download link from video detail page
    Returns (mp4_url or None, error_message or None)
    """
    try:
        r = requests.get(detail_url, headers=HEADERS, timeout=20)
        if r.status_code >= 400:
            return None, f"HTTP {r.status_code}"

        s = BeautifulSoup(r.text, "html.parser")

        # Pattern 1 - Most common in 2025-2026: download button inside <a>
        for link in s.find_all("a", href=True):
            if link.find("button", class_=re.compile(r"(?i)download|single-video-download")):
                href = link["href"]
                if ".mp4" in href.lower():
                    return href, None

        # Pattern 2 - Direct .mp4 link anywhere reasonable
        mp4_tag = s.find("a", href=re.compile(r"\.mp4(?:$|\?|#)", re.I))
        if mp4_tag:
            return mp4_tag["href"], None

        # Pattern 3 - Button with download class → parent link
        for btn in s.find_all("button", class_=re.compile(r"(?i)download|btn.*down")):
            parent = btn.find_parent("a")
            if parent and parent.has_attr("href"):
                href = parent["href"]
                if ".mp4" in href.lower():
                    return href, None

        return None, "No MP4 link pattern found"

    except Exception as e:
        return None, str(e)[:70]


def az_download_logic(title, link, number, identifier):
    temp_dir = tempfile.mkdtemp(prefix="az_2026_")
    
    try:
        # ─── Step 1: Get main page & find video detail pages (improved) ───
        resp = requests.get(link, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        video_detail_urls = get_video_detail_pages(resp.text)

        if not video_detail_urls:
            return False, "No video detail HTML pages found"

        # ─── Step 2: Extract mp4 links from each detail page ───
        mp4_links = []
        for detail_url in video_detail_urls:
            mp4_url, error = find_mp4_on_detail_page(detail_url)
            if mp4_url:
                # Make absolute URL
                full_mp4 = urljoin("https://", mp4_url) if mp4_url.startswith("/") else mp4_url
                mp4_links.append(full_mp4)

        if not mp4_links:
            return False, f"No MP4 links found after checking {len(video_detail_urls)} pages"

        # ─── Step 3: Download parts (same as before) ───
        parts = []
        for i, url in enumerate(mp4_links):
            path = os.path.join(temp_dir, f"part_{i:02d}.mp4")
            r = requests.get(url, stream=True, headers=HEADERS, timeout=60)
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
            parts.append(path)

        # ─── Step 4: Merge with ffmpeg (unchanged) ───
        list_file = os.path.join(temp_dir, "files.txt")
        with open(list_file, "w", encoding="utf-8") as f:
            for p in parts:
                f.write(f"file '{p}'\n")

        output = os.path.join(temp_dir, f"{number:02d} - {title.replace('/', '_')}.mp4")

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

        # ─── Step 5: Upload (unchanged) ───
        return upload_file(output, identifier)

    except Exception as e:
        import traceback
        traceback.print_exc()  # helpful for debugging
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
            update_row(TAB_NAME, row_num, "DONE", next_number, "")
            current_number += 1
        else:
            update_row(TAB_NAME, row_num, "FAILED", "", msg)


if __name__ == "__main__":
    main()
