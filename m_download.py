#!/usr/bin/env python3
import subprocess, time
from openpyxl import load_workbook

EXCEL_PATH = "sharepoint_folders.xlsx"
COLUMN_URL_HEADER = "FolderURL"
COLUMN_QID_HEADER = "QID"

def open_in_safari(url: str):
    script = f'''
    set theURL to "{url.replace('"', '%22')}"
    tell application "Safari"
        if (count of windows) = 0 then
            make new document with properties {{URL:theURL}}
        else
            tell window 1 to set current tab to (make new tab with properties {{URL:theURL}})
        end if
        activate
    end tell
    '''
    subprocess.run(["osascript", "-e", script], check=True)

def load_rows(xlsx_path, url_header, qid_header):
    wb = load_workbook(filename=xlsx_path, data_only=True)
    ws = wb.active
    headers = {str(c.value).strip().lower(): c.column for c in ws[1] if c.value}
    url_col = headers[url_header.lower()]
    qid_col = headers[qid_header.lower()]
    rows = []
    for r in range(2, ws.max_row + 1):
        url_cell = ws.cell(row=r, column=url_col)
        qid_cell = ws.cell(row=r, column=qid_col)
        url = None
        if url_cell.hyperlink:
            url = url_cell.hyperlink.target
        elif isinstance(url_cell.value, str):
            url = url_cell.value
        if url and qid_cell.value:
            rows.append((url, str(qid_cell.value)))
    wb.close()
    return rows

def main():
    rows = load_rows(EXCEL_PATH, COLUMN_URL_HEADER, COLUMN_QID_HEADER)
    print(f"Found {len(rows)} links.")
    for i, (url, qid) in enumerate(rows, 1):
        print(f"\n[{i}/{len(rows)}] QID={qid}")
        open_in_safari(url)
        input("👉 Download the files manually, then press Enter to continue...")

if __name__ == "__main__":
    main()
