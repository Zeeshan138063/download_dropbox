import io
import logging
import zipfile
from pathlib import Path

import requests
from tqdm import tqdm

# === CONFIGURATION ===
SHARED_LINK = "https://www.dropbox.com/scl/fo/n16h7edl07zz1ow89sr0g/APcAgxw44y8vlwkMb89Oi2k/BA_1979?dl=0&rlkey=vbdoqb32gbvdibzzu4ytuw7jy&subfolder_nav_tracking=1"
OUTPUT_BASE = Path("./dropbox_wavs")  # creates BA_1979-1, BA_1979-2
LOG_LEVEL = logging.INFO
# =====================

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


def assign_child_index(parent_to_children, parent, child_name):
    children = parent_to_children.setdefault(parent, [])
    if child_name not in children:
        children.append(child_name)
    return children.index(child_name) + 1  # 1-based


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

    data.seek(0)  # Reset to beginning
    logging.info("Download complete (%.2f MB)", len(data.getbuffer()) / (1024 * 1024))
    return data  # Return BytesIO object, not bytes


def extract_wavs(zf: zipfile.ZipFile):
    parent_to_children = {}
    wav_entries = [
        info for info in zf.infolist()
        if not info.is_dir() and info.filename.lower().endswith(".wav")
    ]

    logging.info("Found %d .wav files inside the ZIP", len(wav_entries))
    pbar = tqdm(wav_entries, desc="Extracting", unit="file")

    for info in pbar:
        path = info.filename
        parts = path.strip("/").split("/")
        if len(parts) < 3:
            logging.debug("Skipping unexpected path %s", path)
            continue

        parent_dir, child_dir_name, filename = parts[0], parts[1], parts[-1]
        child_index = assign_child_index(parent_to_children, parent_dir, child_dir_name)

        local_dir = OUTPUT_BASE / f"{parent_dir}-{child_index}"
        local_dir.mkdir(parents=True, exist_ok=True)

        local_path = local_dir / filename
        if local_path.exists():
            logging.debug("Skipping existing %s", local_path)
            continue

        with open(local_path, "wb") as f:
            f.write(zf.read(info))

    pbar.close()
    logging.info("Extraction complete")


def main():
    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
    url = to_zip_url(SHARED_LINK)

    zip_data = download_zip(url)  # Returns BytesIO object
    with zipfile.ZipFile(zip_data) as zf:
        extract_wavs(zf)

    logging.info("All done! Files are under %s", OUTPUT_BASE.resolve())


if __name__ == "__main__":
    main()
