import os
import subprocess
import tempfile
import shutil

from common.sheets import read_pending_rows, update_row
from common.archive import upload_file

TAB_NAME = "MISSAV"

def missav_download_logic(title, m3u8_url, identifier):
    temp_dir = tempfile.mkdtemp(prefix="missav_")

    try:
        output = os.path.join(temp_dir, f"{title}.mp4")

        subprocess.run(
            [
                "ffmpeg",
                "-loglevel", "error",
                "-i", m3u8_url,
                "-c", "copy",
                output
            ],
            check=True
        )

        return upload_file(output, identifier)

    except Exception as e:
        return False, str(e)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    rows = read_pending_rows(TAB_NAME)

    for r in rows:
        row_num = r["row"]
        title = r["Title"]
        link = r["Link"]          # THIS IS ALREADY m3u8
        identifier = r["Identifier"]

        success, msg = missav_download_logic(
            title,
            link,
            identifier
        )

        if success:
            update_row(
                TAB_NAME,
                row_num,
                "DONE",
                "",
                ""
            )
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
