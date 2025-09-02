# predict_to_html_batch.py

from pathlib import Path
import json
import re
from typing import Any, Dict, List, Optional
import requests
import pandas as pd

# ========= CONFIG =========

PREDICT_URL = "http://0.0.0.0:8080/predict"
SKYVAULT_URL = ""   # your SkyVault base
HEADERS_IN_PAYLOAD = {"X-LN-Application": "0", "x-ln-request": "0", "X-LN-Session": "0"}
ANSWER_LOCATOR = True
STREAMING = False
PDMFID = "1537339"

EXCEL_PATH = "./argument_inputs.xlsx"        # <-- put your workbook here
OUTPUT_DIR = Path("./output_arg")            # output: ./output_arg/argument_QID_{qid}.html
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TEMPLATE_SEPARATOR = "======================================================================================================"


# ========= PREDICT CALLS & PARSERS =========

def build_payload(query: str, corpus_triplet: str) -> Dict[str, Any]:
    return {
        "data_source": [{"type": "dbotf", "corpus": [corpus_triplet]}],
        "query": query,
        "streaming": bool(STREAMING),
        "headers": HEADERS_IN_PAYLOAD or {},
        "feature_flags": {"answerLocator": bool(ANSWER_LOCATOR)},
    }


def call_predict_text(endpoint: str, payload: Dict[str, Any], timeout: Optional[float] = None) -> str:
    headers = {"Content-Type": "application/json"}
    r = requests.post(endpoint, data=json.dumps(payload), headers=headers, timeout=timeout, stream=False)
    r.raise_for_status()
    return r.text


def extract_json_blocks_from_text(text: str) -> List[dict]:
    """Extract JSON objects from SSE-like 'data: {...}' blocks; fallback to whole body JSON."""
    blocks: List[dict] = []

    # SSE-style
    for m in re.finditer(r'data:\s*{', text):
        start = text.find('{', m.start())
        if start < 0:
            continue
        depth = 0
        for j in range(start, len(text)):
            ch = text[j]
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    raw = text[start:j+1]
                    try:
                        blocks.append(json.loads(raw))
                    except Exception:
                        pass
                    break

    # Single JSON fallback
    if not blocks:
        try:
            blocks.append(json.loads(text))
        except Exception:
            pass

    return blocks


def find_pid_texts_from_promptbuilder(blocks: List[dict]) -> Dict[str, str]:
    """Get PID→text from the PromptBuilder:Argument Questions task."""
    pid_text_map: Dict[str, str] = {}

    for ev in blocks:
        if not isinstance(ev, dict):
            continue
        if ev.get("type") == "task" and "PromptBuilder:Argument Questions" in (ev.get("name") or ""):
            content = ev.get("content") if isinstance(ev.get("content"), dict) else {}
            prompt_text = None
            for k in ("prompt", "ai_prompt", "builder_prompt", "content"):
                val = content.get(k)
                if isinstance(val, str) and "pid-" in val:
                    prompt_text = val
                    break
            if prompt_text:
                extract_pid_texts_into_map(prompt_text, pid_text_map)

    return pid_text_map


def extract_pid_texts_into_map(text: str, pid_text_map: Dict[str, str]) -> None:
    """
    Supports:
      1) <pid-12> ... </pid-12>  (preferred)
      2) [pid-12: text...]
      3) [pid-12] text until next [pid-..]
    Keeps the longest text per pid.
    """
    # 1) XML-style
    for m in re.finditer(r'<pid-(\d+)>\s*(.*?)\s*</pid-\1>', text, flags=re.DOTALL | re.IGNORECASE):
        pid = f"pid-{m.group(1)}"
        seg = m.group(2).strip()
        if seg and len(seg) > len(pid_text_map.get(pid, "")):
            pid_text_map[pid] = seg

    # 2) [pid-12: text...]
    for m in re.finditer(r'\[pid-(\d+)\s*:\s*([^\]]+)\]', text):
        pid = f"pid-{m.group(1)}"
        payload = m.group(2).strip()
        if payload and len(payload) > len(pid_text_map.get(pid, "")):
            pid_text_map[pid] = payload

    # 3) [pid-12] <text up to next pid token>
    for m in re.finditer(r'\[pid-(\d+)\]\s*((?:(?!\[pid-\d+\]).)*)', text, flags=re.DOTALL):
        pid = f"pid-{m.group(1)}"
        seg = m.group(2).strip()
        if seg and len(seg) > len(pid_text_map.get(pid, "")):
            pid_text_map[pid] = seg


def get_final_content(blocks: List[dict]) -> Dict[str, Any]:
    chosen = None
    for ev in blocks:
        if ev.get("type") == "conversational-manager-message-finished" or ev.get("finished") is True:
            chosen = ev
    if not chosen and blocks:
        chosen = blocks[-1]
    if isinstance(chosen, dict):
        content = chosen.get("content")
        return content if isinstance(content, dict) else {}
    return {}


# ========= HTML HELPERS =========

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def insert_numeric_anchors(anchors: List[Dict[str, Any]], message: str) -> str:
    if not anchors:
        return message
    anchors_sorted = sorted(anchors, key=lambda a: int(a.get("offset", 0)))
    out = message
    shift = 0
    for a in anchors_sorted:
        try:
            idx = int(a.get("id", 0)) + 1
            off = int(a.get("offset", 0)) + shift
            tag = f"[{idx}]"
            out = out[:off] + tag + out[off:]
            shift += len(tag)
        except Exception:
            continue
    return out


def wrap_pid_token(pid_num: str,
                   upload_id: Optional[str],
                   corpus_triplet: str,
                   pid_text: str) -> str:
    """
    Render [pid-#] as clickable + tooltip + dropdown.
    The click calls JS to fetch a fresh presigned URL on demand.
    """
    safe_text = html_escape(pid_text or "")
    # parse triplet
    try:
        customer, database, table = corpus_triplet.split("/", 2)
    except ValueError:
        customer = database = table = ""
    # If missing upload_id, leave a disabled anchor
    if not upload_id:
        return (f'<span class="pid-wrap">'
                f'<a class="pid-anchor" href="#" title="{safe_text}">[pid-{pid_num}]</a>'
                f'<button class="pid-toggle" onclick="togglePid(this)" aria-label="Show source" title="Show source">▾</button>'
                f'<span class="pid-tooltip">{safe_text}</span>'
                f'<div class="pid-dropdown" hidden>{safe_text}</div>'
                f'</span>')
    # otherwise clickable anchor that requests a presigned URL at click time
    return (
        f'<span class="pid-wrap">'
        f'<a class="pid-anchor" href="#" '
        f'data-upload="{html_escape(upload_id)}" '
        f'data-customer="{html_escape(customer)}" '
        f'data-database="{html_escape(database)}" '
        f'data-table="{html_escape(table)}" '
        f'onclick="return openPidDoc(this)" '
        f'title="{safe_text}">[pid-{pid_num}]</a>'
        f'<button class="pid-toggle" onclick="togglePid(this)" aria-label="Show source" title="Show source">▾</button>'
        f'<span class="pid-tooltip">{safe_text}</span>'
        f'<div class="pid-dropdown" hidden>{safe_text}</div>'
        f'</span>'
    )


def hyperlink_pids_with_ui(message_html_escaped: str,
                           mappings: Dict[str, List[Dict[str, Any]]],
                           corpus_triplet: str,
                           pid_text_map: Dict[str, str]) -> str:
    """
    Replace [pid-#] in the **escaped** message with interactive markup.
    """

    def repl(m):
        pid_num = m.group(1)
        pid_key = f"pid-{pid_num}"
        entries = mappings.get(pid_key) or []
        upload_id = entries[0].get("upload_identifier") if entries else None
        pid_text = pid_text_map.get(pid_key, "")
        return wrap_pid_token(pid_num, upload_id, corpus_triplet, pid_text)

    return re.sub(r'\[pid-(\d+)\]', repl, message_html_escaped)


def render_html(qid: str,
                query: str,
                content_type: Optional[str],
                message: str,
