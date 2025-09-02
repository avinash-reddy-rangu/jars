#!/usr/bin/env python3
import os
import time
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional
import urllib.parse as urlparse
import zipfile
import shutil
import pandas as pd

# --------------- CONFIG ---------------
EXCEL_PATH = "sharepoint_folders.xlsx"   # your Excel file
COLUMN_URL = "FolderURL"                 # column with SharePoint/OneDrive folder links
COLUMN_QID = "QID"                       # column with QID values
OUTPUT_BASE = Path("output")             # where extracted content goes
DOWNLOADS_DIR = Path.home() / "Downloads"
PER_FOLDER_TIMEOUT = 600                 # seconds to wait for each folder ZIP
STABLE_SECONDS = 4                       # how long a file size must be stable
OPEN_IN_BACKGROUND = False               # don't bring Safari to front if True
CLOSE_TAB_AFTER = True                   # try to close tab after triggering download
RENAME_FILES_REPLACE_SPACES = True       # rename extracted files: " " -> "_"
# --------------------------------------


def force_download_param(u: str) -> str:
    """
    For OneDrive/SharePoint folder links, appending download=1 often triggers a ZIP download.
    """
    try:
        parsed = urlparse.urlparse(u)
        q = urlparse.parse_qsl(parsed.query, keep_blank_values=True)
        if not any(k.lower() == "download" for k, _ in q):
            q.append(("download", "1"))
        new_q = urlparse.urlencode(q)
        return urlparse.urlunparse(parsed._replace(query=new_q))
    except Exception:
        return u


def applescript_open_in_safari(url: str, activate: bool = True) -> None:
    script = f'''
    set theURL to "{url.replace('"', '%22')}"
    tell application "Safari"
        if (count of windows) = 0 then
            make new document with properties {{URL:theURL}}
        else
            tell window 1 to set current tab to (make new tab with properties {{URL:theURL}})
        end if
        {"activate" if activate else ""}
    end tell
    '''
    subprocess.run(["osascript", "-e", script], check=True)


def applescript_close_active_tab() -> None:
    script = '''
    tell application "Safari"
        if (count of windows) > 0 then
            tell window 1
                if (count of tabs) > 0 then
                    close current tab
                end if
            end tell
        end if
    end tell
    '''
    subprocess.run(["osascript", "-e", script], check=True)


def newest_path(dirpath: Path, after_ts: float) -> Optional[Path]:
    newest = None
    newest_mtime = after_ts
    for p in dirpath.iterdir():
        try:
            m = p.stat().st_mtime
            if m > newest_mtime:
                newest_mtime = m
                newest = p
        except Exception:
            pass
    return newest


def looks_in_progress(p: Path) -> bool:
    """
    Safari shows in-progress downloads as a .download package (a directory).
    Treat non-regular files or *.download as 'in progress'.
    """
    if p.suffix.lower() == ".download":
        return True
    if not p.is_file():
        return True
    return False


def wait_until_stable(file_path: Path, stable_seconds: int, timeout_s: int) -> bool:
    start = time.time()
    last_size = -1
    last_change = time.time()
    while time.time() - start < timeout_s:
        if not file_path.exists():
            time.sleep(0.5)
            continue
        try:
            size = file_path.stat().st_size
        except Exception:
            time.sleep(0.5)
            continue

        if size != last_size:
            last_size = size
            last_change = time.time()
        else:
            if time.time() - last_change >= stable_seconds:
                return True
        time.sleep(0.5)
    return False


def unzip_to_target(zip_path: Path, target_dir: Path):
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(target_dir)

    if RENAME_FILES_REPLACE_SPACES:
        # Walk and rename any files/dirs with spaces -> underscores
        for root, dirs, files in os.walk(target_dir, topdown=False):
            for name in files:
                if " " in name:
                    src = Path(root) / name
                    dst = Path(root) / name.replace(" ", "_")
                    if not dst.exists():
                        src.rename(dst)
            for name in dirs:
                if " " in name:
                    src = Path(root) / name
                    dst = Path(root) / name.replace(" ", "_")
                    if not dst.exists():
                        src.rename(dst)


def process_folder(url: str, qid_value) -> tuple[bool, str]:
    """
    Open SharePoint/OneDrive folder link in Safari, trigger ZIP download, extract to QID folder.
    """
    # Normalize QID-based output folder
    qid_str = str(qid_value).strip()
    if not qid_str:
        return False, f"Empty QID for URL: {url}"
    target_dir = OUTPUT_BASE / f"QID_{qid_str}"

    start_ts = time.time()
    folder_url = force_download_param(url)

    try:
        applescript_open_in_safari(folder_url, activate=(not OPEN_IN_BACKGROUND))
    except subprocess.CalledProcessError as e:
        return False, f"Safari open failed: {e}"

    deadline = start_ts + PER_FOLDER_TIMEOUT
    candidate: Optional[Path] = None

    # Wait for a new .zip file to finish downloading
    while time.time() < deadline:
        newest = newest_path(DOWNLOADS_DIR, after_ts=start_ts - 1)
        if newest is None:
            time.sleep(0.5)
            continue

        # If it's an in-progress .download bundle, keep waiting
        if looks_in_progress(newest):
            candidate = newest
            time.sleep(0.5)
            continue

        # We want a ZIP file
        if newest.suffix.lower() != ".zip":
            # Not a zip—could be a single file if folder contained one item.
            # Still handle it: move it under the QID folder.
            try:
                target_dir.mkdir(parents=True, exist_ok=True)
                dest = target_dir / newest.name
                # Wait stable then move
                wait_until_stable(newest, STABLE_SECONDS, timeout_s=int(deadline - time.time()))
                shutil.move(str(newest), str(dest))
                if RENAME_FILES_REPLACE_SPACES and " " in dest.name:
                    dest.rename(dest.with_name(dest.name.replace(" ", "_")))
                if CLOSE_TAB_AFTER:
                    try: applescript_close_active_tab()
                    except Exception: pass
                return True, f"Saved single file -> {dest}"
            except Exception as e:
                if CLOSE_TAB_AFTER:
                    try: applescript_close_active_tab()
                    except Exception: pass
                return False, f"Non-zip download handling failed: {e}"

        # ZIP: wait until size stable, then unzip
        if wait_until_stable(newest, STABLE_SECONDS, timeout_s=int(deadline - time.time())):
            try:
                unzip_to_target(newest, target_dir)
                # Optionally remove the zip after extract
                newest.unlink(missing_ok=True)
                if CLOSE_TAB_AFTER:
                    try: applescript_close_active_tab()
                    except Exception: pass
                return True, f"Extracted to {target_dir}"
            except Exception as e:
                if CLOSE_TAB_AFTER:
                    try: applescript_close_active_tab()
                    except Exception: pass
                return False, f"Unzip failed: {e}"
        else:
            break

    if CLOSE_TAB_AFTER:
        try: applescript_close_active_tab()
        except Exception: pass
    return False, f"Timeout waiting for folder ZIP from: {url}"


def main():
    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

    if not Path(EXCEL_PATH).exists():
        print(f"Excel not found: {EXCEL_PATH}")
        return

    df = pd.read_excel(EXCEL_PATH)
    for col in (COLUMN_URL, COLUMN_QID):
        if col not in df.columns:
            print(f"Column '{col}' not found. Available: {list(df.columns)}")
            return

    rows = df[[COLUMN_URL, COLUMN_QID]].dropna(subset=[COLUMN_URL, COLUMN_QID]).values.tolist()
    print(f"Found {len(rows)} rows with folder links and QIDs. Starting…")

    ok = 0
    fails = []
    for i, (url, qid) in enumerate(rows, 1):
        t0 = datetime.now().strftime("%H:%M:%S")
        print(f"[{i}/{len(rows)} {t0}] QID={qid} :: {url}")
        success, msg = process_folder(str(url).strip(), qid)
        print("  ->", msg)
        ok += int(success)
        if not success:
            fails.append(f"QID={qid} :: {url} :: {msg}")

    print(f"\nDone. Success: {ok}/{len(rows)}")
    if fails:
        print("\nFailures (check manually or adjust):")
        for f in fails:
            print(" -", f)


if __name__ == "__main__":
    main()
