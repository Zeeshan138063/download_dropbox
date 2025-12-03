import io
import logging
import shutil
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
    "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/AESX5aZGFMB8nybjVzk4ouo/RN_1958?rlkey=vbdoqb32gbvdibzzu4ytuw7jy&dl=0",
]

OUTPUT_BASE = Path("/home/zeeshan/audios/download_dropbox/dropbox_wavs")
RAW_ARCHIVES = Path("/home/zeeshan/audios/download_dropbox/raw_zip")
RAW_EXTRACTS = Path("/home/zeeshan/audios/download_dropbox/raw_extracted")
LOG_LEVEL = logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

RAW_ARCHIVES.mkdir(parents=True, exist_ok=True)
RAW_EXTRACTS.mkdir(parents=True, exist_ok=True)
OUTPUT_BASE.mkdir(parents=True, exist_ok=True)


def to_zip_url(shared_link: str) -> str:
    if "dl=0" in shared_link:
        return shared_link.replace("dl=0", "dl=1")
    if "dl=1" in shared_link:
        return shared_link
    sep = "&" if "?" in shared_link else "?"
    return shared_link + sep + "dl=1"


def parent_name_from_url(url: str) -> str:
    parts = [p for p in urlparse(url).path.split("/") if p]
    for part in reversed(parts):
        if "_" in part:
            return part.split("?")[0]
    return "unknown"


def cache_zip(url: str, parent: str) -> Path:
    target = RAW_ARCHIVES / f"{parent}.zip"
    if target.exists():
        logging.info("ZIP cached: %s", target)
        return target

    logging.info("Downloading ZIP for %s", parent)
    resp = requests.get(to_zip_url(url), stream=True)
    resp.raise_for_status()

    total = int(resp.headers.get("Content-Length", 0))
    pbar = tqdm(total=total, unit="B", unit_scale=True, desc=f"Downloading {parent}")
    with open(target, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                fh.write(chunk)
                pbar.update(len(chunk))
    pbar.close()
    logging.info("ZIP saved to %s (%.2f MB)", target, target.stat().st_size / (1024 * 1024))
    return target


def extract_full_zip(zip_path: Path, parent: str) -> Path:
    target_dir = RAW_EXTRACTS / parent
    if target_dir.exists():
        logging.info("Raw extract already exists: %s", target_dir)
        return target_dir

    logging.info("Extracting full ZIP for %s", parent)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(target_dir)
    logging.info("Raw files available at %s", target_dir)
    return target_dir


def collect_wavs(raw_dir: Path, parent: str):
    child_map = {}
    wav_paths = list(raw_dir.rglob("*.wav"))

    logging.info("%s: found %d .wav files", parent, len(wav_paths))
    pbar = tqdm(wav_paths, desc=f"Collecting {parent}", unit="file")

    for wav_path in pbar:
        rel = wav_path.relative_to(raw_dir)
        parts = rel.parts
        if len(parts) < 2:
            logging.warning("Skipping %s (not enough path depth)", wav_path)
            continue

        child_dir = parts[0]  # e.g., 2018-05-28
        if child_dir not in child_map:
            child_map[child_dir] = len(child_map) + 1

        child_index = child_map[child_dir]
        target_dir = OUTPUT_BASE / f"{parent}-{child_index}"
        target_dir.mkdir(parents=True, exist_ok=True)

        target_path = target_dir / wav_path.name
        logging.info("Copying %s -> %s", wav_path, target_path)
        shutil.copy2(wav_path, target_path)

    pbar.close()


def process_folder(url: str):
    parent = parent_name_from_url(url)
    logging.info("=== Processing %s ===", parent)

    zip_path = cache_zip(url, parent)
    raw_dir = extract_full_zip(zip_path, parent)
    collect_wavs(raw_dir, parent)

    logging.info("Finished %s", parent)


def main():
    logging.info("Starting batch download (%d folders)", len(DROPBOX_URLS))
    for idx, url in enumerate(DROPBOX_URLS, start=1):
        logging.info("Folder %d/%d", idx, len(DROPBOX_URLS))
        process_folder(url)
    logging.info("Done. WAVs under %s, raw zips under %s", OUTPUT_BASE, RAW_ARCHIVES)


if __name__ == "__main__":
    main()
