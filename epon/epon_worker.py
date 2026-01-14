import os
import subprocess
import tempfile
import shutil
from common.sheets import (
    read_pending_rows,
    get_max_assigned_number,
    update_row
)
from common.archive import upload_file

TAB_NAME = "EPON"

def download_with_ytdlp(url, output_path):
    """First attempt: yt-dlp with eporner referer"""
    cmd = [
        "yt-dlp",
        "--no-part",
        "--no-warnings",
        "--user-agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "--referer",
        "https://www.eporner.com/",
        "-o",
        output_path,
        url
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def download_with_aria2_standard(url, output_dir, filename):
    """Second attempt: standard aria2c (like before)"""
    cmd = [
        "aria2c",
        "-x", "16",
        "-s", "16",
        "--file-allocation=trunc",
        "--auto-file-renaming=false",
        "--allow-overwrite=true",
        "--dir", output_dir,
        "-o", filename,
        "--header=User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "--header=Referer: https://www.eporner.com/",
        "--header=Accept: */*",
        url
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def download_with_aria2_xtits_aggressive(url, output_dir, filename):
    """Third attempt: aggressive aria2c with xtits.xxx referer + more options"""
    cmd = [
        "aria2c",
        "-x", "16",
        "-s", "16",
        "--file-allocation=trunc",
        "--summary-interval=2",
        "--console-log-level=info",
        "--show-console-readout=true",
        "--auto-file-renaming=false",
        "--allow-overwrite=true",
        "--dir", output_dir,
        "-o", filename,
        "--header=User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "--header=Referer: https://www.xtits.xxx/",
        "--header=Accept: */*",
        "--header=Accept-Language: en-US,en;q=0.9",
        "--header=Connection: keep-alive",
        "--check-certificate=false",
        "--remote-time=true",
        url
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def epon_download_logic(title, link, number, identifier):
    temp_dir = tempfile.mkdtemp(prefix="epon_")
    filename = f"{number:02d} - {title}.mp4"          # ← same naming as you had
    output_path = os.path.join(temp_dir, filename)

    success = False
    error_msg = ""

    try:
        # 1. Try yt-dlp first
        try:
            download_with_ytdlp(link, output_path)
            success = True
            print(f"Success with yt-dlp: {filename}")
        except subprocess.CalledProcessError as e:
            error_msg = f"yt-dlp failed: {e.stderr.strip() or str(e)}"
            print(error_msg)

        # 2. If failed → standard aria2c
        if not success:
            try:
                download_with_aria2_standard(link, temp_dir, filename)
                success = True
                print(f"Success with standard aria2c: {filename}")
            except subprocess.CalledProcessError as e:
                error_msg += f" | standard aria2c failed: {e.stderr.strip() or str(e)}"
                print(error_msg)

        # 3. If still failed → aggressive aria2c with xtits referer
        if not success:
            try:
                download_with_aria2_xtits_aggressive(link, temp_dir, filename)
                success = True
                print(f"Success with xtits-aggressive aria2c: {filename}")
            except subprocess.CalledProcessError as e:
                error_msg += f" | xtits-aggressive aria2c failed: {e.stderr.strip() or str(e)}"
                print(error_msg)

        if success:
            # Upload only if download succeeded
            return upload_file(output_path, identifier), ""
        else:
            return False, error_msg

    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

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

        success, msg = epon_download_logic(
            title,
            link,
            next_number,
            identifier
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
            print(f"Row {row_num} marked DONE - number {next_number}")
        else:
            update_row(
                TAB_NAME,
                row_num,
                "FAILED",
                "",
                msg
            )
            print(f"Row {row_num} FAILED: {msg}")


if __name__ == "__main__":
    main()
