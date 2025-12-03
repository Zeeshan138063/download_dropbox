# import os
# import io
# import zipfile
# from pathlib import Path
#
# import requests
#
# # === CONFIGURATION ===
# SHARED_LINK = "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AKdFSCwG8GEjFJIcrgnULiw?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&st=80ira577&dl=0"
# OUTPUT_BASE = Path("./dropbox_wavs")  # where BA_1979-1, BA_1979-2, ... will be created
# # =====================
#
#
# def to_zip_url(shared_link: str) -> str:
#     # Ensure we force "download zip" behavior
#     if "dl=1" in shared_link:
#         return shared_link
#     if "dl=0" in shared_link:
#         return shared_link.replace("dl=0", "dl=1")
#     sep = "&" if "?" in shared_link else "?"
#     return shared_link + sep + "dl=1"
#
#
# def download_zip(shared_link: str) -> bytes:
#     url = to_zip_url(shared_link)
#     print(f"Downloading ZIP from: {url}")
#     resp = requests.get(url, stream=True)
#     resp.raise_for_status()
#     data = io.BytesIO()
#     for chunk in resp.iter_content(chunk_size=1024 * 1024):
#         if chunk:
#             data.write(chunk)
#     data.seek(0)
#     return data.read()
#
#
# def assign_child_index(parent_to_children, parent, child_name):
#     """
#     Maintain a stable mapping:
#         parent -> [child_name_1, child_name_2]
#     So we can map to parent-1 or parent-2.
#     """
#     children = parent_to_children.setdefault(parent, [])
#     if child_name not in children:
#         children.append(child_name)
#     return children.index(child_name) + 1  # 1-based index
#
#
# def main():
#     OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
#
#     zip_bytes = download_zip(SHARED_LINK)
#     zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
#
#     parent_to_children = {}
#
#     for info in zf.infolist():
#         if info.is_dir():
#             continue
#
#         path = info.filename  # e.g. "BA_1979/childX/file.wav"
#         parts = path.strip("/").split("/")
#         if len(parts) < 3:
#             # not in expected parent/child/file structure
#             continue
#
#         parent_dir, child_dir_name, filename = parts[0], parts[1], parts[-1]
#
#         if not filename.lower().endswith(".wav"):
#             continue
#
#         child_index = assign_child_index(parent_to_children, parent_dir, child_dir_name)
#         local_dir = OUTPUT_BASE / f"{parent_dir}-{child_index}"
#         local_dir.mkdir(parents=True, exist_ok=True)
#
#         local_path = local_dir / filename
#         if local_path.exists():
#             print(f"Already exists, skipping: {local_path}")
#             continue
#
#         print(f"Extracting {path} -> {local_path}")
#         file_data = zf.read(info)
#         with open(local_path, "wb") as f:
#             f.write(file_data)
#
#     print("Done.")
#
#
# if __name__ == "__main__":
#     main()



import io
import zipfile
from pathlib import Path

import requests

# Shared folder link you gave (BA_1979)
SHARED_LINK = "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/APcAgxw44y8vlwkMb89Oi2k/BA_1979?dl=0&rlkey=vbdoqb32gbvdibzzu4ytuw7jy&subfolder_nav_tracking=1"
OUTPUT_BASE = Path("./dropbox_wavs")  # will create BA_1979-1, BA_1979-2 here


def to_zip_url(shared_link: str) -> str:
    # Force ZIP download
    if "dl=0" in shared_link:
        return shared_link.replace("dl=0", "dl=1")
    if "dl=1" in shared_link:
        return shared_link
    sep = "&" if "?" in shared_link else "?"
    return shared_link + sep + "dl=1"


def assign_child_index(parent_to_children, parent, child_name):
    # Map first child dir → index 1, second → index 2
    children = parent_to_children.setdefault(parent, [])
    if child_name not in children:
        children.append(child_name)
    return children.index(child_name) + 1  # 1-based


def main():
    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

    url = to_zip_url(SHARED_LINK)
    print(f"Downloading ZIP from: {url}")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    parent_to_children = {}

    for info in zf.infolist():
        if info.is_dir():
            continue

        path = info.filename  # e.g. "BA_1979/child_dir/file.wav"
        parts = path.strip("/").split("/")
        if len(parts) < 3:
            continue

        parent_dir, child_dir_name, filename = parts[0], parts[1], parts[-1]
        if not filename.lower().endswith(".wav"):
            continue

        child_index = assign_child_index(parent_to_children, parent_dir, child_dir_name)
        local_dir = OUTPUT_BASE / f"{parent_dir}-{child_index}"
        local_dir.mkdir(parents=True, exist_ok=True)

        local_path = local_dir / filename
        if local_path.exists():
            print(f"Skipping existing: {local_path}")
            continue

        print(f"Extracting {path} -> {local_path}")
        with open(local_path, "wb") as f:
            f.write(zf.read(info))

    print("Done.")


if __name__ == "__main__":
    main()
