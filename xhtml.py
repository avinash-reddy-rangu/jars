# process_cases.py
"""
Process case folders from an Excel, convert PDFs->DOCX and DOCX->XHTML, and copy outputs
into QID-specific folders.

Dependencies:
  pip install pandas pdf2docx mammoth lxml openpyxl tqdm

Usage:
  python process_cases.py \
      --excel /path/to/cases.xlsx \
      --input-dir /path/to/input_dir \
      --output-dir /path/to/output_dir \
      --sheet-name Sheet1

Excel columns required (case-insensitive): "QID", "Name of Case"
Behavior:
  - Finds a subfolder in input_dir whose name matches "Name of Case" (case-insensitive,
    tolerant to spaces/underscores/hyphens).
  - In that folder:
      * For each .pdf -> convert to .docx (pdf2docx), then .docx -> .xhtml (mammoth + lxml)
      * For each .docx -> .xhtml
  - Copies the resulting .docx and .xhtml into output_dir/QID_{QID}, ensuring filenames have
    no spaces (spaces -> underscores) and removing illegal characters.
  - Writes a manifest.csv summarizing work done.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from tqdm import tqdm

# Third-party converters
from pdf2docx import Converter as PDF2DOCXConverter
import mammoth
from lxml import html as lxml_html
from lxml import etree


# -------- Utils --------

ILLEGAL_CHARS = r'<>:"/\\|?*\n\r\t'

def sanitize_filename(name: str) -> str:
    """
    Make a safe filename:
      - Replace spaces with underscores
      - Remove illegal characters
      - Collapse multiple underscores
      - Strip leading/trailing underscores and dots
    """
    if not name:
        return "unnamed"
    # Replace spaces with underscores
    name = name.replace(" ", "_")
    # Remove illegal characters
    name = re.sub(f"[{re.escape(ILLEGAL_CHARS)}]", "", name)
    # Collapse multiple underscores
    name = re.sub(r"_+", "_", name)
    # Remove trailing dots (Windows)
    name = name.strip("._")
    return name or "unnamed"


def norm_key(s: str) -> str:
    """
    Normalization used for folder matching:
      - casefold()
      - remove spaces, underscores, and hyphens
    """
    return re.sub(r"[ _\-]+", "", s.casefold())


def map_subdirs_by_norm(root: Path) -> Dict[str, Path]:
    """
    Build a dictionary mapping normalized folder names to their actual Path.
    If duplicates normalize to the same key, last one wins (we also warn).
    """
    mapping: Dict[str, Path] = {}
    for p in root.iterdir():
        if p.is_dir():
            k = norm_key(p.name)
            if k in mapping and mapping[k] != p:
                # You could log a warning here if desired
                pass
            mapping[k] = p
    return mapping


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_manifest(manifest_rows: List[Dict], out_dir: Path) -> None:
    if not manifest_rows:
        return
    df = pd.DataFrame(manifest_rows)
    df.to_csv(out_dir / "manifest.csv", index=False)


# -------- Converters --------

def pdf_to_docx(pdf_path: Path, out_docx: Path) -> Tuple[bool, Optional[str]]:
    """Convert a single PDF to DOCX using pdf2docx."""
    try:
        ensure_dir(out_docx.parent)
        cv = PDF2DOCXConverter(str(pdf_path))
        cv.convert(str(out_docx))  # converts all pages
        cv.close()
        return True, None
    except Exception as e:
        return False, f"pdf_to_docx failed: {e}"


def docx_to_xhtml(docx_path: Path, out_xhtml: Path) -> Tuple[bool, Optional[str]]:
    """
    Convert DOCX -> (X)HTML using mammoth (HTML5) then serialize as XHTML via lxml.
    """
    try:
        ensure_dir(out_xhtml.parent)
        with open(docx_path, "rb") as f:
            result = mammoth.convert_to_html(f)
        html_str = result.value or ""

        # Make a full XHTML document
        # If mammoth returns a fragment, wrap it.
        try:
            doc = lxml_html.fromstring(html_str)
        except etree.ParserError:
            # Wrap fragment
            doc = lxml_html.fromstring(f"<div>{html_str}</div>")

        # Build root html > head+body if needed
        if doc.tag.lower() not in ("html", "body", "div"):
            wrapper = lxml_html.Element("div")
            wrapper.append(doc)
            doc = wrapper

        root_html = lxml_html.Element("html")
        head = lxml_html.Element("head")
        meta = lxml_html.Element("meta", charset="utf-8")
        head.append(meta)
        body = lxml_html.Element("body")

        # Move doc under body
        body.append(doc)
        root_html.append(head)
        root_html.append(body)

        # Serialize as XHTML (XML method)
        xhtml_bytes = etree.tostring(
            root_html,
            pretty_print=True,
            method="xml",
            encoding="utf-8",
            xml_declaration=True,
            doctype='<!DOCTYPE html>'
        )
        with open(out_xhtml, "wb") as f:
            f.write(xhtml_bytes)

        return True, None
    except Exception as e:
        return False, f"docx_to_xhtml failed: {e}"


# -------- Core processing --------

def process_case_folder(
    case_dir: Path,
    qid: str,
    case_name: str,
    tmp_work_dir: Path,
    dest_qid_dir: Path,
    manifest: List[Dict],
) -> None:
    """
    For a given case folder:
      - Convert PDFs -> DOCX
      - Convert all DOCX -> XHTML
      - Copy DOCX + XHTML to dest_qid_dir with safe filenames
    """
    ensure_dir(tmp_work_dir)
    ensure_dir(dest_qid_dir)

    # Gather files
    pdfs = list(case_dir.rglob("*.pdf"))
    docxs = list(case_dir.rglob("*.docx"))

    # 1) Convert PDFs -> DOCX
    generated_docx_paths: List[Path] = []
    for pdf in pdfs:
        base = sanitize_filename(pdf.stem)
        out_docx = tmp_work_dir / f"{base}.docx"
        ok, err = pdf_to_docx(pdf, out_docx)
        manifest.append({
            "qid": qid,
            "case_name": case_name,
            "source_file": str(pdf),
            "action": "pdf->docx",
            "output_file": str(out_docx) if ok else "",
            "status": "ok" if ok else "error",
            "error": err or ""
        })
        if ok:
            generated_docx_paths.append(out_docx)

    # Combine with original docx files
    all_docx = [*docxs, *generated_docx_paths]

    # 2) Convert all DOCX -> XHTML
    docx_to_xhtml_map: List[Tuple[Path, Path]] = []
    for docx in all_docx:
        base = sanitize_filename(Path(docx).stem)
        out_xhtml = tmp_work_dir / f"{base}.xhtml"
        ok, err = docx_to_xhtml(docx, out_xhtml)
        manifest.append({
            "qid": qid,
            "case_name": case_name,
            "source_file": str(docx),
            "action": "docx->xhtml",
            "output_file": str(out_xhtml) if ok else "",
            "status": "ok" if ok else "error",
            "error": err or ""
        })
        if ok:
            docx_to_xhtml_map.append((Path(docx), out_xhtml))

    # 3) Copy DOCX + XHTML into QID folder, filenames without spaces
    #    (We copy original DOCX if it exists, otherwise the generated one)
    # Build a set of docx paths we want to copy: originals and generated
    docx_to_copy = set(all_docx)
    for docx in docx_to_copy:
        src = Path(docx)
        base = sanitize_filename(src.stem)
        dst = dest_qid_dir / f"{base}.docx"
        try:
            shutil.copy2(src, dst)
            manifest.append({
                "qid": qid,
                "case_name": case_name,
                "source_file": str(src),
                "action": "copy-docx",
                "output_file": str(dst),
                "status": "ok",
                "error": ""
            })
        except Exception as e:
            manifest.append({
                "qid": qid,
                "case_name": case_name,
                "source_file": str(src),
                "action": "copy-docx",
                "output_file": str(dst),
                "status": "error",
                "error": str(e)
            })

    for _, xhtml in docx_to_xhtml_map:
        src = Path(xhtml)
        base = sanitize_filename(src.stem)
        dst = dest_qid_dir / f"{base}.xhtml"
        try:
            shutil.copy2(src, dst)
            manifest.append({
                "qid": qid,
                "case_name": case_name,
                "source_file": str(src),
                "action": "copy-xhtml",
                "output_file": str(dst),
                "status": "ok",
                "error": ""
            })
        except Exception as e:
            manifest.append({
                "qid": qid,
                "case_name": case_name,
                "source_file": str(src),
                "action": "copy-xhtml",
                "output_file": str(dst),
                "status": "error",
                "error": str(e)
            })


def find_case_dir_for_name(case_name: str, input_dir: Path, subdir_map: Dict[str, Path]) -> Optional[Path]:
    """
    Try exact match first; then normalized match; then loose contains match.
    """
    # Exact (case-sensitive)
    exact = input_dir / case_name
    if exact.is_dir():
        return exact

    # Case-insensitive normalized match
    nk = norm_key(case_name)
    if nk in subdir_map:
        return subdir_map[nk]

    # Loose: look for any folder whose normalized name contains the normalized query
    candidates = [p for k, p in subdir_map.items() if nk in k]
    if len(candidates) == 1:
        return candidates[0]
    elif len(candidates) > 1:
        # If ambiguous, prefer the shortest name (often the “main” folder)
        candidates.sort(key=lambda p: len(p.name))
        return candidates[0]

    return None


def load_excel(excel_path: Path, sheet_name: Optional[str]) -> pd.DataFrame:
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    # Normalize columns (case-insensitive)
    cols = {c.casefold(): c for c in df.columns}
    required = ["qid", "name of case"]
    missing = [r for r in required if r not in cols]
    if missing:
        raise ValueError(
            f"Missing required columns in Excel (case-insensitive): {missing}. "
            f"Found columns: {list(df.columns)}"
        )
    # Select only needed columns, keep original casing
    qid_col = cols["qid"]
    name_col = cols["name of case"]
    out = df[[qid_col, name_col]].copy()
    out.columns = ["QID", "Name of Case"]
    # Drop rows with NaN in these columns
    out = out.dropna(subset=["QID", "Name of Case"])
    return out


def main():
    parser = argparse.ArgumentParser(description="Process case folders from Excel.")
    parser.add_argument("--excel", required=True, type=Path, help="Path to input .xlsx")
    parser.add_argument("--input-dir", required=True, type=Path, help="Path to input_dir containing case folders")
    parser.add_argument("--output-dir", required=True, type=Path, help="Path to output_dir")
    parser.add_argument("--sheet-name", default=None, help="Excel sheet name (optional)")
    args = parser.parse_args()

    excel_path: Path = args.excel
    input_dir: Path = args.input_dir
    output_dir: Path = args.output_dir
    sheet_name: Optional[str] = args.sheet_name

    if not excel_path.exists():
        raise FileNotFoundError(f"Excel not found: {excel_path}")
    if not input_dir.exists():
        raise FileNotFoundError(f"input_dir not found: {input_dir}")

    ensure_dir(output_dir)

    # temp working area for conversions (inside output_dir/.work)
    work_dir = output_dir / ".work"
    ensure_dir(work_dir)

    df = load_excel(excel_path, sheet_name)

    # Pre-map subdirectories for faster lookups
    subdir_map = map_subdirs_by_norm(input_dir)

    manifest: List[Dict] = []
    missing_cases: List[Tuple[str, str]] = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing rows"):
        qid = str(row["QID"]).strip()
        case_name = str(row["Name of Case"]).strip()

        case_dir = find_case_dir_for_name(case_name, input_dir, subdir_map)
        if not case_dir:
            manifest.append({
                "qid": qid,
                "case_name": case_name,
                "source_file": "",
                "action": "find-case-folder",
                "output_file": "",
                "status": "error",
                "error": "Case folder not found"
            })
            missing_cases.append((qid, case_name))
            continue

        # QID destination folder
        dest_qid_dir = output_dir / f"QID_{sanitize_filename(qid)}"
        tmp_work_dir = work_dir / f"{sanitize_filename(qid)}"
        process_case_folder(
            case_dir=case_dir,
            qid=qid,
            case_name=case_name,
            tmp_work_dir=tmp_work_dir,
            dest_qid_dir=dest_qid_dir,
            manifest=manifest
        )

    write_manifest(manifest, output_dir)

    # Helpful console summary
    print("\nDone.")
    print(f"Output written under: {output_dir}")
    print(f"Manifest: {output_dir / 'manifest.csv'}")
    if missing_cases:
        print("\nCases with no matching folder in input_dir:")
        for qid, name in missing_cases:
            print(f"  - QID {qid}: '{name}'")
        print("Tip: Check for spelling/case/space/underscore differences. The matcher tolerates spaces/underscores/hyphens and is case-insensitive.")

if __name__ == "__main__":
    main()
