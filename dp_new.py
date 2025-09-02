# predict_to_html_batch.py

from pathlib import Path
import json
import re
from typing import Any, Dict, List, Optional
import requests
import pandas as pd

# ========= CONFIG =========

PREDICT_URL = "http://0.0.0.0:8080/predict"
SKYVAULT_URL = "https://cdc7c-euw2-skyvault.route53.lexis.com"   # your SkyVault base
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
                ref_anchors: List[Dict[str, Any]],
                ref_documents: List[Dict[str, Any]],
                mappings: Dict[str, List[Dict[str, Any]]],
                pdmfid: str,
                pid_text_map: Dict[str, str],
                corpus_triplet: str) -> str:

    # Escape first, then inject PID HTML
    msg = insert_numeric_anchors(ref_anchors, message)
    base = html_escape(msg).replace("\n", "<br>")
    message_html = hyperlink_pids_with_ui(base, mappings, corpus_triplet, pid_text_map)

    # JS for on-demand presigned URL fetching
    headers_js = json.dumps({"Content-Type": "application/json", **HEADERS_IN_PAYLOAD})
    skyvault_base_js = json.dumps(SKYVAULT_URL.rstrip("/"))

    head = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{html_escape(qid)}</title>
<style>
  body {{ font-family: system-ui,-apple-system,Segoe UI,Roboto,sans-serif; line-height: 1.45; padding: 16px; }}
  .pid-wrap {{ position: relative; display: inline-flex; align-items: center; gap: 4px; }}
  .pid-anchor {{ text-decoration: none; padding: 0 2px; border-bottom: 1px dashed #888; }}
  .pid-toggle {{ border: none; background: #f1f1f1; cursor: pointer; padding: 0 6px; border-radius: 6px; font-size: .85em; }}
  .pid-toggle:hover {{ background: #e2e2e2; }}
  .pid-tooltip {{ position: absolute; z-index: 20; left: 0; top: 1.7em; max-width: 520px; background: #111; color: #fff; padding: 8px 10px; border-radius: 8px; box-shadow: 0 6px 18px rgba(0,0,0,.2); opacity: 0; pointer-events: none; transform: translateY(-4px); transition: all .12s ease; }}
  .pid-wrap:hover .pid-tooltip {{ opacity: 1; transform: translateY(0); }}
  .pid-dropdown {{ margin-top: 6px; padding: 10px; background: #fafafa; border: 1px solid #eee; border-radius: 8px; }}
  h2 {{ margin-top: 24px; }}
  .sep {{ color:#888; }}
  .muted {{ color:#666; font-size:.9em; }}
</style>
<script>
  const SKYVAULT_BASE = {skyvault_base_js};
  const LN_HEADERS = {headers_js};

  function togglePid(btn) {{
    const wrap = btn.closest('.pid-wrap');
    const dd = wrap.querySelector('.pid-dropdown');
    dd.hidden = !dd.hidden;
  }}

  async function openPidDoc(a) {{
    const upload = a.getAttribute('data-upload');
    const cust   = a.getAttribute('data-customer');
    const db     = a.getAttribute('data-database');
    const table  = a.getAttribute('data-table');
    if (!upload || !cust || !db || !table) return false;

    const url = SKYVAULT_BASE + "/generate_presigned_urls_xhtml/" + encodeURIComponent(cust) + "/" + encodeURIComponent(db) + "/" + encodeURIComponent(table) + "/documents";
    try {{
      const resp = await fetch(url, {{
        method: "POST",
        headers: LN_HEADERS,
        body: JSON.stringify({{ document_ids: [upload] }})
      }});
      const data = await resp.json();
      const map = (data && (data.presigned_urls_xhtml || data.presigned_urls)) || {{}};
      const target = map[upload];
      if (target) {{
        window.open(target, "_blank");
      }} else {{
        alert("Could not retrieve a presigned URL for this PID.");
      }}
    }} catch (e) {{
      alert("Error generating presigned URL: " + e);
    }}
    return false; // prevent default
  }}
</script>
</head>
<body>
"""

    parts = []
    parts.append(f"<div>QID: {html_escape(qid)}</div>")
    parts.append("<div class='sep'>=====</div><br>")
    parts.append("<h2>User Prompt</h2>")
    parts.append(f"<div>{html_escape(query)}</div>")
    if content_type:
        parts.append(f"<div class='muted'>Type: {html_escape(str(content_type))}</div>")
    parts.append("<h2>AI Response</h2>")
    parts.append(f"<div>{message_html}</div><br>")

    # PID References
    if mappings:
        parts.append("<h2>PID References</h2>")
        for pid_key, entries in mappings.items():
            parts.append(f"<div><strong>{html_escape(pid_key)}</strong></div>")
            for ent in entries or []:
                upload_id = ent.get("upload_identifier", "")
                xpaths = ent.get("xpaths") or []
                parts.append(f"<div>&nbsp;&nbsp;• upload_id: {html_escape(upload_id)}</div>")
                if xpaths:
                    parts.append("<div>&nbsp;&nbsp;&nbsp;&nbsp;xpaths:</div>")
                    for xp in xpaths:
                        parts.append(f"<div>&nbsp;&nbsp;&nbsp;&nbsp;- {html_escape(xp)}</div>")
            parts.append("<br>")

    # Citations
    parts.append("<h2>Citations</h2>")
    parts.append("<div class='muted'>Links open in Lexis+ (UK).</div>")
    for d in (final_docs := (final_docs if (final_docs := None) else None)) or ref_documents:
        lni = d.get("lni")
        ctype = d.get("content_type", "")
        doc_name = d.get("document_name", "")
        passage = d.get("passage_text", "")
        link = f"https://plus.lexis.com/uk/document/?pdmfid={pdmfid}&pddocfullpath=%2Fshared%2Fdocument%2F{ctype}%2Furn%3AcontentItem%3A{lni}" if lni and ctype else "#"
        try:
            idx = int(d.get("id", 0)) + 1
        except Exception:
            idx = d.get("id", 0)
        parts.append("<div>Document_name:</div>")
        parts.append(f'<div>[{idx}] <a target="_blank" href="{html_escape(link)}">{html_escape(doc_name)}</a></div>')
        parts.append("<div>Passage_text:</div>")
        parts.append(f"<div>{html_escape(passage)}</div>")
        parts.append(f"<div class='sep'>{TEMPLATE_SEPARATOR}</div>")

    parts.append("<br>")
    parts.append(f"<div class='sep'>{TEMPLATE_SEPARATOR}</div>")
    parts.append("</body></html>")
    return head + "\n".join(parts)


# ========= BATCH DRIVER =========

def run_for_row(qid_val: int, prompt_query: str) -> None:
    qid_str = str(qid_val).strip()
    corpus_triplet = f"ln/reddy/QID_{qid_str}"
    payload = build_payload(prompt_query, corpus_triplet)

    # 1) call predict + parse blocks
    text = call_predict_text(PREDICT_URL, payload)
    blocks = extract_json_blocks_from_text(text)

    # 2) get pid texts from prompt builder
    pid_text_map = find_pid_texts_from_promptbuilder(blocks)

    # 3) final content
    final_content = get_final_content(blocks)
    message = final_content.get("message", "") if isinstance(final_content, dict) else ""
    content_type = final_content.get("type") if isinstance(final_content, dict) else None
    ref_anchors = final_content.get("ref_anchors") or []
    ref_documents = final_content.get("ref_documents") or []
    mappings = final_content.get("mappings") or {}

    # 4) render html (JS fetch will generate presigned URLs at click time)
    html = render_html(
        qid=f"QID {qid_str}",
        query=prompt_query,
        content_type=content_type,
        message=message,
        ref_anchors=ref_anchors,
        ref_documents=ref_documents,
        mappings=mappings,
        pdmfid=PDMFID,
        pid_text_map=pid_text_map,
        corpus_triplet=corpus_triplet,
    )

    out_path = OUTPUT_DIR / f"argument_QID_{qid_str}.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"[OK] wrote {out_path.resolve()}")


def main():
    df = pd.read_excel(EXCEL_PATH)
    # expect columns: QID, Prompt Query
    if not {"QID", "Prompt Query"}.issubset(df.columns):
        raise ValueError("Excel must contain columns: 'QID', 'Prompt Query'")

    for _, row in df.iterrows():
        qid = row["QID"]
        prompt_query = str(row["Prompt Query"])
        if pd.isna(qid) or pd.isna(prompt_query) or not str(prompt_query).strip():
            continue
        try:
            run_for_row(int(qid), prompt_query)
        except Exception as e:
            print(f"[WARN] QID {qid}: {e}")

if __name__ == "__main__":
    main()
