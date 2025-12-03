import io
import logging
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import requests
from tqdm import tqdm

DROPBOX_URLS = [
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/APcAgxw44y8vlwkMb89Oi2k/BA_1979?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/ALVkA5n-2jhaO9ZI4XZXeQQ/BC_1985?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/ADdWAeo_5Q4ObuX5VFSJxCU/BL_1964?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AEbjp4h_WWZ81iVhGmw-oF0/FT_1977?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AFqWUR0CgugDFgdgdeoPq4E/HC_1968?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AE6TRijsEG0GEQXpX_OG4oY/HW_1975?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AP2dGXP3hUjPf92IIYgZpcY/KA_1971?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AC7IHOYpUiygrf4fijFtru8/KJ_1989?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AJgxHAKdrKsdiPqC8jD9TWY/LT_1971?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AL6_xPm4GQUMvDIhZIsyoPk/MA_1969?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AFVf_ywkFdi2h18tpPif4v8/MM_1967?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AL1E-Thv49rAOzBh4KXA1tI/PI_1970?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AJNZZ6TCaSJMmGnhRNP17ls/PM_1983?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/ABipqvD-EMc6Kv136t86Oyw/PT_1987?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AESX5aZGFMB8nybjVzk4ouo/RN_1958?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0"
]

OUTPUT_BASE = Path("/home/zeeshan/audios/download_dropbox/dropbox_wavs")
LOG_LEVEL = logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def to_zip_url(shared_link: str) -> str:
    if "dl=0" in shared_link:
        return shared_link.replace("dl=0", "dl=1")
    if "dl=1" in shared_link:
        return shared_link
    sep = "&" if "?" in shared_link else "?"
    return shared_link + sep + "dl=1"


def parent_name_from_url(url: str) -> str:
    # Take the last path segment that looks like XX_YYYY
    parts = [p for p in urlparse(url).path.split("/") if p]
    for part in reversed(parts):
        if "_" in part:
            return part.split("?")[0]
    return "unknown"


def download_zip(url: str) -> io.BytesIO:
    logging.info("Downloading ZIP from %s", url)
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    total = int(resp.headers.get("Content-Length", 0))
    data = io.BytesIO()
    pbar = tqdm(total=total, unit="B", unit_scale=True, desc="Downloading")
    for chunk in resp.iter_content(chunk_size=1024 * 1024):
        if chunk:
            data.write(chunk)
            pbar.update(len(chunk))
    pbar.close()

    data.seek(0)
    logging.info("Download complete (%.2f MB)", len(data.getbuffer()) / (1024 * 1024))
    return data


def extract_wavs(zf: zipfile.ZipFile, parent_folder: str):
    # Map each distinct child folder to parent-1 / parent-2
    child_map = {}
    wav_entries = [
        info for info in zf.infolist()
        if not info.is_dir() and info.filename.lower().endswith(".wav")
    ]
    logging.info("Found %d .wav files", len(wav_entries))

    if wav_entries:
        logging.info("Sample entries:")
        for info in wav_entries[:3]:
            logging.info("  %s", info.filename)

    pbar = tqdm(wav_entries, desc="Extracting", unit="file")
    for info in pbar:
        parts = info.filename.strip("/").split("/")
        if len(parts) < 3:
            logging.debug("Skipping unexpected path %s", info.filename)
            continue

        child_dir = parts[-2]
        filename = parts[-1]

        if child_dir not in child_map:
            child_map[child_dir] = len(child_map) + 1
        child_index = child_map[child_dir]
        target_dir = OUTPUT_BASE / f"{parent_folder}-{child_index}"
        target_dir.mkdir(parents=True, exist_ok=True)

        target_path = target_dir / filename
        logging.info("Writing %s", target_path)
        with open(target_path, "wb") as f:
            f.write(zf.read(info))
    pbar.close()


def main():
    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
    logging.info("Starting download of %d folders", len(DROPBOX_URLS))
    for idx, url in enumerate(DROPBOX_URLS, 1):
        logging.info("=== Folder %d/%d ===", idx, len(DROPBOX_URLS))
        parent = parent_name_from_url(url)
        zip_data = download_zip(to_zip_url(url))
        with zipfile.ZipFile(zip_data) as zf:
            extract_wavs(zf, parent)
        logging.info("Finished %s", parent)
    logging.info("All done. Files stored under %s", OUTPUT_BASE)


if __name__ == "__main__":
    main()
