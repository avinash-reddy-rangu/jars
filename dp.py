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

# Turn on streaming to capture intermediate events
ANSWER_LOCATOR = True
STREAMING = True

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


def stream_predict(endpoint: str, payload: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, Any]]:
    """
    Stream SSE from /predict and return:
      - pid_text_map: {'pid-9': 'text', ...} captured from intermediate prompt builder events
      - final_content: the last event's content (message, mappings, ref_documents, ref_anchors, type, etc.)
    """
    headers = {"Content-Type": "application/json"}
    pid_text_map: Dict[str, str] = {}
    final_content: Dict[str, Any] = {}

    with requests.post(endpoint, data=json.dumps(payload), headers=headers, stream=True) as r:
        r.raise_for_status()
        buf = ""
        for raw_line in r.iter_lines(decode_unicode=True):
            if raw_line is None:
                continue
            line = raw_line.strip()
            if not line:
                # end of one SSE event; try parse collected buf if needed
                buf = ""
                continue
            if line.startswith(":"):
                # SSE comment line, ignore
                continue
            if line.startswith("data:"):
                data_str = line[5:].strip()
                if not data_str:
                    continue
                try:
                    ev = json.loads(data_str)
                except Exception:
                    # Some servers may concatenate; try to extract the last JSON object
                    try:
                        m = re.search(r'({.*})', data_str, re.DOTALL)
                        if m:
                            ev = json.loads(m.group(1))
                        else:
                            continue
                    except Exception:
                        continue

                content = ev.get("content") if isinstance(ev, dict) else None
                if isinstance(content, dict):
                    # 1) Capture PID texts from prompt-like fields in intermediate events
                    for key in ("prompt", "ai_prompt", "builder_prompt", "content"):
                        val = content.get(key)
                        if isinstance(val, str) and "[pid-" in val:
                            extract_pid_texts_into_map(val, pid_text_map)

                    # 2) Track latest full content (final messages usually have is_turn_finished/finished flags)
                    if content.get("is_turn_finished") or ev.get("finished") or ev.get("type") == "conversational-manager-message-finished":
                        final_content = content

    return pid_text_map, final_content


def extract_pid_texts_into_map(text: str, pid_text_map: Dict[str, str]) -> None:
    """
    Given a builder prompt text containing [pid-#] tokens, extract pid→text pairs.
    We support both:
      [pid-12: text...]
      [pid-12] text until the next [pid-..] or end.
    Keep the longest text seen for a given pid.
    """
    # Pattern B: [pid-12: text...]
    for m in re.finditer(r'\\[pid-(\\d+)\\s*:\\s*([^\\]]+)\\]', text):
        pid = f"pid-{m.group(1)}"
        payload = m.group(2).strip()
        if payload:
            if len(payload) > len(pid_text_map.get(pid, "")):
                pid_text_map[pid] = payload

    # Pattern A: [pid-12] capture following text up to next pid token
    for m in re.finditer(r'\\[pid-(\\d+)\\]\\s*((?:(?!\\[pid-\\d+\\]).)*)', text, flags=re.DOTALL):
        pid = f"pid-{m.group(1)}"
        seg = m.group(2).strip()
        if seg:
            if len(seg) > len(pid_text_map.get(pid, "")):
                pid_text_map[pid] = seg


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
    Build a plain XHTML link for the PID (no fragments). No CDC fallback.
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

    # 1) Stream predict to capture pid texts + final content
    pid_text_map, final_content = stream_predict(PREDICT_URL, payload)

    # 2) Pull fields from final content
    message = final_content.get("message", "") if isinstance(final_content, dict) else ""
    content_type = final_content.get("type") if isinstance(final_content, dict) else None
    ref_anchors = final_content.get("ref_anchors") or []
    ref_documents = final_content.get("ref_documents") or []
    mappings = final_content.get("mappings") or {}

    # 3) Fetch SkyVault presigned URLs (required for PID hyperlinks)
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

    # 4) Render HTML
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


if __name__ == "__main__":
    main()
