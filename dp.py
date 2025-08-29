from pathlib import Path
import json
import re
from typing import Any, Dict, List, Optional, Tuple
import requests

# =========================
# ==== CONFIG CONSTANTS ====
# =========================

# Predict API
PREDICT_URL = "http://0.0.0.0:8080/predict"

# Query + Corpus (dbotf triplet = customerId/database/table)
QID = "QID 5"
QUERY = (
    "Using the details provided in the two letters of correspondence between the solicitors "
    "and the client in the case of Sarah Thompson v Mark Evans, draft an argument that might "
    "be made against accepting the Defendant's Part 36 offer of £45,000."
)
CORPUS_TRIPLET = "4fd33ae9-9d1a-46d2-b6c6-8a4f805d4acc/c562387b-a3f6-4f15-b356-6564bc24398d/eb2c47ca-89f3-4826-aa68-23de925b3bfc"
HEADERS_IN_PAYLOAD = {"X-LN-Application": "0", "x-ln-request": "0", "X-LN-Session": "0"}

# Do NOT stream; we will parse the SSE-style blocks from the full response text
ANSWER_LOCATOR = True
STREAMING = False

# SkyVault presigned XHTML URL endpoint (required for PID links)
SKYVAULT_URL = "https://skyvault.example/api"   # <-- set to your real base URL

# PDMFID for Lexis Plus links
PDMFID = "1537339"

# Output HTML path
OUTPUT_PATH = Path("/mnt/data/output/draft_transactional_QID 5.html")


# =========================
# ====== UTILITIES =========
# =========================

TEMPLATE_SEPARATOR = "======================================================================================================"


def build_payload(query: str,
                  corpora: List[str],
                  headers_kv: Dict[str, str],
                  answer_locator: bool,
                  streaming: bool) -> Dict[str, Any]:
    return {
        "data_source": [{"type": "dbotf", "corpus": corpora}],
        "query": query,
        "streaming": bool(streaming),
        "headers": headers_kv or {},
        "feature_flags": {"answerLocator": bool(answer_locator)},
    }


def call_predict_text(endpoint: str, payload: Dict[str, Any], timeout: Optional[float] = None) -> str:
    headers = {"Content-Type": "application/json"}
    r = requests.post(endpoint, data=json.dumps(payload), headers=headers, timeout=timeout, stream=False)
    r.raise_for_status()
    # Some deployments return application/json (single object). Others return SSE-style text.
    # Always return text; the parser will handle both.
    return r.text


def _extract_json_blocks_from_text(text: str) -> List[dict]:
    """
    Extract JSON objects from the response text.
    Supports:
      - SSE-like sequences: lines beginning with 'data: { ... }'
      - Plain JSON (single object)
    Uses a brace-balanced scanner after each 'data:' marker.
    """
    blocks: List[dict] = []

    # 1) Try SSE style: 'data: { ... }'
    starts = list(re.finditer(r'data:\s*{', text))
    for m in starts:
        i = m.start()
        start = text.find('{', i)
        if start < 0:
            continue
        depth = 0
        j = start
        while j < len(text):
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
            j += 1

    # 2) If no SSE blocks found, try to parse the whole text as one JSON object
    if not blocks:
        try:
            obj = json.loads(text)
            blocks.append(obj)
        except Exception:
            pass

    return blocks


def find_pid_texts_from_promptbuilder(blocks: List[dict]) -> Dict[str, str]:
    """
    Locate the task event named 'PromptBuilder:Argument Questions' and extract PID→text pairs
    from its content.prompt (or similar prompt field).
    """
    pid_text_map: Dict[str, str] = {}

    for ev in blocks:
        if not isinstance(ev, dict):
            continue
        # Match event by type 'task' and name containing 'PromptBuilder:Argument Questions'
        t = ev.get("type")
        n = ev.get("name") or ev.get("task") or ""
        if (t == "task") and isinstance(n, str) and "PromptBuilder:Argument Questions" in n:
            content = ev.get("content") if isinstance(ev.get("content"), dict) else {}
            prompt_text = None
            # prefer 'prompt', then 'ai_prompt', then any string fields with pid tokens
            for k in ("prompt", "ai_prompt", "builder_prompt", "content"):
                val = content.get(k)
                if isinstance(val, str) and "[pid-" in val:
                    prompt_text = val
                    break
            if prompt_text:
                _extract_pid_texts_into_map(prompt_text, pid_text_map)

    return pid_text_map


def _extract_pid_texts_into_map(text: str, pid_text_map: Dict[str, str]) -> None:
    """
    Given a builder prompt text containing [pid-#] tokens, extract pid→text pairs.
    Supports:
      [pid-12: text...]
      [pid-12] text until the next [pid-..] or end.
    Keep the longest text seen for a given pid.
    """
    # Pattern B: [pid-12: text...]
    for m in re.finditer(r'\[pid-(\d+)\s*:\s*([^\]]+)\]', text):
        pid = f"pid-{m.group(1)}"
        payload = m.group(2).strip()
        if payload and len(payload) > len(pid_text_map.get(pid, "")):
            pid_text_map[pid] = payload

    # Pattern A: [pid-12] capture following text up to next pid token
    for m in re.finditer(r'\[pid-(\d+)\]\s*((?:(?!\[pid-\d+\]).)*)', text, flags=re.DOTALL):
        pid = f"pid-{m.group(1)}"
        seg = m.group(2).strip()
        if seg and len(seg) > len(pid_text_map.get(pid, "")):
            pid_text_map[pid] = seg


def get_final_content(blocks: List[dict]) -> Dict[str, Any]:
    """
    Choose the final content block:
      - Prefer objects with 'type' == 'conversational-manager-message-finished' or finished true
      - Else, last block
    Return that block's 'content' if present; else empty dict.
    """
    chosen: Optional[dict] = None
    for ev in blocks:
        if not isinstance(ev, dict):
            continue
        if ev.get("type") == "conversational-manager-message-finished" or ev.get("finished") is True:
            chosen = ev
    if not chosen and blocks:
        chosen = blocks[-1]

    if isinstance(chosen, dict):
        content = chosen.get("content")
        return content if isinstance(content, dict) else {}
    return {}


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


def collect_pid_upload_ids(mappings: Dict[str, List[Dict[str, Any]]]) -> List[str]:
    ids = set()
    for entries in mappings.values():
        for ent in entries or []:
            uid = ent.get("upload_identifier")
            if uid:
                ids.add(uid)
    return sorted(ids)


def fetch_skyvault_presigned_map(base_url: str,
                                 customer_id: str,
                                 database: str,
                                 table: str,
                                 upload_ids: List[str],
                                 headers_kv: Dict[str, str],
                                 timeout: Optional[float]) -> Dict[str, str]:
    """
    Returns {upload_identifier: presigned_xhtml_url}
    """
    if not (base_url and upload_ids):
        return {}
    url = f"{base_url.rstrip('/')}/generate_presigned_urls_xhtml/{customer_id}/{database}/{table}/documents"
    payload = {"document_ids": upload_ids}
    headers = {"Content-Type": "application/json"}
    headers.update(headers_kv or {})
    r = requests.post(url, data=json.dumps(payload), headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    return data.get("presigned_urls_xhtml", {}) or {}


def build_pid_link(pid_key: str,
                   mappings: Dict[str, List[Dict[str, Any]]],
                   skyvault_map: Dict[str, str]) -> Optional[str]:
    """
    Build a plain XHTML link for the PID (no fragments).
    """
    entries = mappings.get(pid_key) or []
    if not entries:
        return None
    upload_id = entries[0].get("upload_identifier")
    if not upload_id:
        return None
    return skyvault_map.get(upload_id)


def wrap_pid_token(pid_key: str,
                   pid_num: str,
                   url: Optional[str],
                   pid_text: str) -> str:
    """
    Produce markup:
      <span class="pid-wrap">
        <a class="pid-anchor" href="URL" target="_blank" title="PID TEXT">[pid-9]</a>
        <button class="pid-toggle" onclick="togglePid(this)" ...>▾</button>
        <span class="pid-tooltip">PID TEXT</span>
        <div class="pid-dropdown" hidden>PID TEXT</div>
      </span>
    """
    safe_text = html_escape(pid_text or "")
    safe_url = html_escape(url or "#")
    return (
        f'<span class="pid-wrap">'
        f'<a class="pid-anchor" href="{safe_url}" target="_blank" title="{safe_text}">[pid-{pid_num}]</a>'
        f'<button class="pid-toggle" onclick="togglePid(this)" aria-label="Show source" title="Show source">▾</button>'
        f'<span class="pid-tooltip">{safe_text}</span>'
        f'<div class="pid-dropdown" hidden>{safe_text}</div>'
        f'</span>'
    )


def hyperlink_pids_with_ui(message: str,
                           mappings: Dict[str, List[Dict[str, Any]]],
                           skyvault_map: Dict[str, str],
                           pid_text_map: Dict[str, str]) -> str:
    """
    Replace [pid-#] with interactive markup (anchor + tooltip + dropdown).
    If SkyVault URL missing, the link points to "#" but UI still shows the PID text.
    """

    def repl(m):
        pid_num = m.group(1)
        pid_key = f"pid-{pid_num}"
        url = build_pid_link(pid_key, mappings, skyvault_map)
        pid_text = pid_text_map.get(pid_key, "")
        return wrap_pid_token(pid_key, pid_num, url, pid_text)

    return re.sub(r'\[pid-(\d+)\]', repl, message)


def render_html(qid: str,
                query: str,
                content_type: Optional[str],
                message: str,
                ref_anchors: List[Dict[str, Any]],
                ref_documents: List[Dict[str, Any]],
                mappings: Dict[str, List[Dict[str, Any]]],
                skyvault_map: Dict[str, str],
                pdmfid: str,
                pid_text_map: Dict[str, str]) -> str:
    # Insert numeric anchors if supplied
    msg = insert_numeric_anchors(ref_anchors, message)
    # Replace pid tokens with interactive UI
    msg = hyperlink_pids_with_ui(msg, mappings, skyvault_map, pid_text_map)
    message_html = html_escape(msg).replace("\n", "<br>")

    head = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{html_escape(qid)}</title>
<style>
  body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; line-height: 1.45; padding: 16px; }}
  .pid-wrap {{ position: relative; display: inline-flex; align-items: center; gap: 4px; }}
  .pid-anchor {{ text-decoration: none; padding: 0 2px; border-bottom: 1px dashed #888; }}
  .pid-anchor[href="#"] {{ pointer-events: none; opacity: 0.6; }}
  .pid-toggle {{ border: none; background: #f1f1f1; cursor: pointer; padding: 0 6px; border-radius: 6px; font-size: 0.8em; }}
  .pid-toggle:hover {{ background: #e2e2e2; }}
  .pid-tooltip {{ position: absolute; z-index: 20; left: 0; top: 1.7em; max-width: 420px; background: #111; color: #fff; padding: 8px 10px; border-radius: 8px; box-shadow: 0 6px 18px rgba(0,0,0,.2); opacity: 0; pointer-events: none; transform: translateY(-4px); transition: all .12s ease; }}
  .pid-wrap:hover .pid-tooltip {{ opacity: 1; transform: translateY(0); }}
  .pid-dropdown {{ margin-top: 6px; padding: 10px; background: #fafafa; border: 1px solid #eee; border-radius: 8px; }}
  h2 {{ margin-top: 24px; }}
  .sep {{ color:#888; }}
</style>
<script>
  function togglePid(btn) {{
    const wrap = btn.closest('.pid-wrap');
    const dd = wrap.querySelector('.pid-dropdown');
    dd.hidden = !dd.hidden;
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
        parts.append(f"<div class='sep'>Type: {html_escape(str(content_type))}</div>")
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
                url = None
                if upload_id in skyvault_map:
                    url = skyvault_map[upload_id]
                if url:
                    parts.append(f'<div>&nbsp;&nbsp;• <a target="_blank" href="{html_escape(url)}">{html_escape(upload_id)}</a></div>')
                else:
                    parts.append(f"<div>&nbsp;&nbsp;• {html_escape(upload_id)}</div>")
                if xpaths:
                    parts.append("<div>&nbsp;&nbsp;&nbsp;&nbsp;xpaths:</div>")
                    for xp in xpaths:
                        parts.append(f"<div>&nbsp;&nbsp;&nbsp;&nbsp;- {html_escape(xp)}</div>")
            parts.append("<br>")

    # PID Texts
    if pid_text_map:
        parts.append("<h2>PID Texts</h2>")
        for k in sorted(pid_text_map.keys(), key=lambda x: int(re.sub(r'[^0-9]', '', x) or '0')):
            v = pid_text_map[k]
            parts.append(f"<div><strong>{html_escape(k)}</strong>: {html_escape(v)}</div>")
        parts.append("<br>")

    # Citations
    parts.append("<h2>Citations</h2>")
    for d in ref_documents or []:
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


def main():
    corpora = [CORPUS_TRIPLET]
    payload = build_payload(QUERY, corpora, HEADERS_IN_PAYLOAD, ANSWER_LOCATOR, STREAMING)

    # 1) Call predict (non-streaming), parse blocks
    text = call_predict_text(PREDICT_URL, payload)
    blocks = _extract_json_blocks_from_text(text)

    # 2) Extract PID texts from the prompt-builder task
    pid_text_map = find_pid_texts_from_promptbuilder(blocks)

    # 3) Get final content
    final_content = get_final_content(blocks)

    # 4) Pull fields from final content
    message = final_content.get("message", "") if isinstance(final_content, dict) else ""
    content_type = final_content.get("type") if isinstance(final_content, dict) else None
    ref_anchors = final_content.get("ref_anchors") or []
    ref_documents = final_content.get("ref_documents") or []
    mappings = final_content.get("mappings") or {}

    # 5) Fetch SkyVault presigned URLs (required for PID hyperlinks)
    try:
        customer_id, database, table = CORPUS_TRIPLET.split("/", 2)
    except ValueError:
        customer_id = database = table = ""

    skyvault_map: Dict[str, str] = {}
    upload_ids = collect_pid_upload_ids(mappings)
    if SKYVAULT_URL and customer_id and database and table and upload_ids:
        try:
            skyvault_map = fetch_skyvault_presigned_map(
                SKYVAULT_URL, customer_id, database, table, upload_ids, HEADERS_IN_PAYLOAD, timeout=None
            )
            print(f"[INFO] Fetched {len(skyvault_map)} presigned XHTML URL(s)")
        except Exception as e:
            print(f"[WARN] SkyVault presigned fetch failed: {e}")

    # 6) Render HTML
    html = render_html(
        qid=QID,
        query=QUERY,
        content_type=content_type,
        message=message,
        ref_anchors=ref_anchors,
        ref_documents=ref_documents,
        mappings=mappings,
        skyvault_map=skyvault_map,
        pdmfid=PDMFID,
        pid_text_map=pid_text_map,
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"[OK] Wrote {OUTPUT_PATH.resolve()}")

def _extract_pid_texts_into_map(text: str, pid_text_map: Dict[str, str]) -> None:
    """
    Given a builder prompt text containing PID tokens, extract pid→text pairs.

    Now supports:
      1) <pid-12> ... </pid-12>      <-- NEW (preferred per your latest format)
      2) [pid-12: text...]
      3) [pid-12] text until the next [pid-..] or end.

    For a given pid, we keep the longest text seen.
    """

    # Pattern 1 (preferred): <pid-12> ... </pid-12>
    for m in re.finditer(r'<pid-(\d+)>\s*(.*?)\s*</pid-\1>', text, flags=re.DOTALL | re.IGNORECASE):
        pid = f"pid-{m.group(1)}"
        seg = m.group(2).strip()
        if seg and len(seg) > len(pid_text_map.get(pid, "")):
            pid_text_map[pid] = seg

    # Pattern 2: [pid-12: text...]
    for m in re.finditer(r'\[pid-(\d+)\s*:\s*([^\]]+)\]', text):
        pid = f"pid-{m.group(1)}"
        payload = m.group(2).strip()
        if payload and len(payload) > len(pid_text_map.get(pid, "")):
            pid_text_map[pid] = payload

    # Pattern 3: [pid-12] capture following text up to the next [pid-..] token
    for m in re.finditer(r'\[pid-(\d+)\]\s*((?:(?!\[pid-\d+\]).)*)', text, flags=re.DOTALL):
        pid = f"pid-{m.group(1)}"
        seg = m.group(2).strip()
        if seg and len(seg) > len(pid_text_map.get(pid, "")):
            pid_text_map[pid] = seg

if __name__ == "__main__":
    main()
