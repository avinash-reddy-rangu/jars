#!/usr/bin/env python3
import os
import time
import zipfile
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple

from openpyxl import load_workbook

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ============== CONFIG ==============
EXCEL_PATH = "sharepoint_folders.xlsx"   # Excel file
URL_HEADER  = "FolderURL"                # column header containing hyperlinks
QID_HEADER  = "QID"                      # column header for QID
SHEET_NAME  = None                       # e.g. "Sheet1" or None for first sheet

OUTPUT_DIR = Path("OUTPUT")              # final destination base
RUN_DOWNLOAD_DIR = Path("session_downloads")  # temp download dir per run (auto-created/cleaned)

# Your Chrome profile directory (macOS example)
CHROME_USER_DATA_DIR = os.path.expanduser("~/Library/Application Support/Google/Chrome")
CHROME_PROFILE_DIR_NAME = "Default"  # or "Profile 1", etc.

# Timeouts / waits (seconds)
PAGE_LOAD_TIMEOUT = 60
CLICK_WAIT_TIMEOUT = 40
DOWNLOAD_MAX_WAIT = 600   # max per row
DOWNLOAD_STABLE_SECONDS = 3

# When a folder is downloaded, SharePoint returns a ZIP.
# Extract it into QID folder and remove the zip.
DELETE_ZIP_AFTER_EXTRACT = True
# =====================================


# ---------- Excel helpers ----------
def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def load_url_qid_rows(xlsx_path: str, url_header: str, qid_header: str, sheet_name: Optional[str]) -> List[Tuple[str, str]]:
    """Read (url, qid) rows preferring hyperlink targets."""
    wb = load_workbook(filename=xlsx_path, data_only=True, read_only=False)
    ws = wb[sheet_name] if sheet_name else wb.active

    # map headers
    header_map = {}
    for idx, cell in enumerate(ws[1], start=1):
        if cell.value is not None:
            header_map[_norm(str(cell.value))] = idx

    if _norm(url_header) not in header_map or _norm(qid_header) not in header_map:
        raise ValueError(f"Could not find '{url_header}'/'{qid_header}' in headers: {list(header_map.keys())}")

    url_col = header_map[_norm(url_header)]
    qid_col = header_map[_norm(qid_header)]

    rows: List[Tuple[str, str]] = []
    for r in range(2, ws.max_row + 1):
        url_cell = ws.cell(row=r, column=url_col)
        qid_cell = ws.cell(row=r, column=qid_col)

        qid_val = qid_cell.value
        if qid_val is None or str(qid_val).strip() == "":
            continue

        # Prefer hyperlink target
        url_val = None
        if url_cell.hyperlink and getattr(url_cell.hyperlink, "target", None):
            url_val = url_cell.hyperlink.target
        elif isinstance(url_cell.value, str):
            raw = url_cell.value.strip()
            if raw.lower().startswith("http://") or raw.lower().startswith("https://"):
                url_val = raw

        if url_val:
            rows.append((url_val, str(qid_val).strip()))

    wb.close()
    return rows


# ---------- Chrome / Selenium setup ----------
def build_driver(download_dir: Path) -> webdriver.Chrome:
    download_dir.mkdir(parents=True, exist_ok=True)

    chrome_opts = Options()
    # Use existing profile (reuses cookies/sessions!)
    chrome_opts.add_argument(f"--user-data-dir={CHROME_USER_DATA_DIR}")
    chrome_opts.add_argument(f"--profile-directory={CHROME_PROFILE_DIR_NAME}")

    # Set downloads dir & disable prompts
    prefs = {
        "download.default_directory": str(download_dir.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        # Avoid opening PDFs in Chrome viewer, force download
        "plugins.always_open_pdf_externally": True,
    }
    chrome_opts.add_experimental_option("prefs", prefs)

    # Optional: keep visible (comment next line if you want headful UI)
    # chrome_opts.add_argument("--headless=new")

    driver = webdriver.Chrome(options=chrome_opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


# ---------- SharePoint click helpers ----------
DOWNLOAD_XPATHS = [
    # Common modern SharePoint / OneDrive buttons
    "//button[@aria-label='Download']",
    "//button[@title='Download']",
    "//*[@data-automationid='download']",
    "//div[@role='menuitem' and contains(@aria-label,'Download')]",
    "//span[normalize-space(text())='Download']/ancestor::button[1]",
    "//span[contains(.,'Download')]/ancestor::button[1]",
]

def try_click_download(driver: webdriver.Chrome) -> None:
    """
    Try several selectors; if the page is a folder, clicking 'Download' should start a .zip;
    if it's a single file, it should download that file.
    """
    wait = WebDriverWait(driver, CLICK_WAIT_TIMEOUT)
    last_exc = None
    for xp in DOWNLOAD_XPATHS:
        try:
            el = wait.until(EC.element_to_be_clickable((By.XPATH, xp)))
            el.click()
            return
        except Exception as e:
            last_exc = e
            # try next
    # If all failed, try pressing 'd' then Return as a last resort (sometimes bound by SharePoint)
    try:
        driver.switch_to.active_element.send_keys("d")
        time.sleep(1)
        driver.switch_to.active_element.send_keys("\n")
    except Exception:
        pass
    if last_exc:
        raise last_exc


# ---------- Download monitoring ----------
def list_download_candidates(download_dir: Path) -> list[Path]:
    return [p for p in download_dir.iterdir() if p.is_file()]

def has_in_progress(download_dir: Path) -> bool:
    # Chrome uses .crdownload while in progress
    for p in download_dir.iterdir():
        if p.name.endswith(".crdownload"):
            return True
    return False

def newest_after(download_dir: Path, after_ts: float) -> Optional[Path]:
    best = None
    best_m = after_ts
    for p in download_dir.iterdir():
        try:
            m = p.stat().st_mtime
            if m > best_m:
                best = p
                best_m = m
        except Exception:
            pass
    return best

def wait_for_download_complete(download_dir: Path, start_ts: float, timeout: int, stable_seconds: int) -> Optional[Path]:
    """
    Wait until we see a new file after start_ts and it stops changing size (no .crdownload remains).
    Returns the finished file path or None on timeout.
    """
    deadline = time.time() + timeout
    candidate = None
    last_size = -1
    stable_since = None

    while time.time() < deadline:
        # Check new file
        newest = newest_after(download_dir, after_ts=start_ts - 1)
        if newest:
            candidate = newest

        # If still downloading, wait
        if has_in_progress(download_dir):
            time.sleep(0.5)
            continue

        if candidate and candidate.exists():
            size = candidate.stat().st_size
            if size != last_size:
                last_size = size
                stable_since = time.time()
            else:
                if stable_since and (time.time() - stable_since) >= stable_seconds:
                    return candidate

        time.sleep(0.5)

    return None


# ---------- Post-processing ----------
def ensure_qid_folder(base: Path, qid: str) -> Path:
    target = base / f"QID_{qid}"
    target.mkdir(parents=True, exist_ok=True)
    return target

def extract_zip_to(zip_path: Path, dest_dir: Path):
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(dest_dir)

def move_single_item(item_path: Path, dest_dir: Path) -> Path:
    dest = dest_dir / item_path.name
    # If exists, add suffix
    if dest.exists():
        stem = dest.stem
        ext = dest.suffix
        i = 2
        while True:
            cand = dest_dir / f"{stem} ({i}){ext}"
            if not cand.exists():
                dest = cand
                break
            i += 1
    shutil.move(str(item_path), str(dest))
    return dest


# ---------- Main ----------
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if RUN_DOWNLOAD_DIR.exists():
        # clean previous leftovers
        for p in RUN_DOWNLOAD_DIR.iterdir():
            if p.is_file():
                p.unlink()
            elif p.is_dir():
                shutil.rmtree(p)
    RUN_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    rows = load_url_qid_rows(EXCEL_PATH, URL_HEADER, QID_HEADER, SHEET_NAME)
    print(f"Found {len(rows)} rows. Starting…")

    driver = build_driver(RUN_DOWNLOAD_DIR)

    try:
        success = 0
        failures: list[str] = []
        for i, (url, qid) in enumerate(rows, 1):
            print(f"\n[{i}/{len(rows)}] QID={qid}")
            started = time.time()
            try:
                driver.get(url)
            except Exception as e:
                failures.append(f"QID={qid} get() failed: {e}")
                continue

            # Try clicking 'Download'
            try:
                try_click_download(driver)
            except Exception as e:
                failures.append(f"QID={qid} could not find Download button: {e}")
                continue

            # Wait for the download to complete
            finished = wait_for_download_complete(
                RUN_DOWNLOAD_DIR, start_ts=started,
                timeout=DOWNLOAD_MAX_WAIT, stable_seconds=DOWNLOAD_STABLE_SECONDS
            )

            if not finished:
                failures.append(f"QID={qid} download timeout.")
                continue

            qid_dir = ensure_qid_folder(OUTPUT_DIR, qid)

            # If it's a ZIP, extract; else move file
            if finished.suffix.lower() == ".zip":
                try:
                    extract_zip_to(finished, qid_dir)
                    if DELETE_ZIP_AFTER_EXTRACT:
                        finished.unlink(missing_ok=True)
                    print(f"  -> Extracted folder to {qid_dir}")
                    success += 1
                except Exception as e:
                    failures.append(f"QID={qid} unzip failed: {e}")
            else:
                try:
                    dest = move_single_item(finished, qid_dir)
                    print(f"  -> Saved file to {dest}")
                    success += 1
                except Exception as e:
                    failures.append(f"QID={qid} move file failed: {e}")

        print(f"\nDone. Success: {success}/{len(rows)}")
        if failures:
            print("\nFailures:")
            for f in failures:
                print(" -", f)

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
