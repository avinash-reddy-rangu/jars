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
ANSWER_LOCATOR = True

# SkyVault presigned XHTML URL endpoint (preferred for PID links)
SKYVAULT_URL = "https://skyvault.example/api"   # <-- change to your real base URL

# CDC viewer fallback (used only if SkyVault presigned call fails or SKYVAULT_URL is blank)
CDC_UPLOAD_BASE = "https://cdc7c-speuw2ukupload.route53.lexis.com"
CDC_SESSION = "YOUR_SESSION_ID"  # optional; leave empty to omit

# PDMFID for Lexis Plus links
PDMFID = "1537339"

# Response SSE/log file path for extracting PID texts and fallback mappings
RESPONSE_LOG_PATH = Path("/mnt/data/response_full")

# Output HTML path
OUTPUT_PATH = Path("/mnt/data/output/draft_transactional_QID 5.html")


# =========================
# ====== UTILITIES =========
# =========================

TEMPLATE_SEPARATOR = "======================================================================================================"


def build_payload(query: str,
                  corpora: List[str],
                  headers_kv: Dict[str, str],
                  answer_locator: bool) -> Dict[str, Any]:
    return {
        "data_source": [{"type": "dbotf", "corpus": corpora}],
        "query": query,
        "streaming": False,
        "headers": headers_kv or {},
        "feature_flags": {"answerLocator": bool(answer_locator)},
    }


def call_predict(endpoint: str, payload: Dict[str, Any], timeout: Optional[float] = None) -> Dict[str, Any]:
    r = requests.post(endpoint, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=timeout)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        # Try to extract SSE-style last JSON block if service replies in text/event-stream
        text = r.text
        matches = re.findall(r'data:\s*({.*?})(?=\n\s*data:|\Z)', text, re.DOTALL)
        if matches:
            return json.loads(matches[-1])
        raise RuntimeError("Predict API returned non-JSON and no SSE-style JSON block could be parsed.")


def parse_predict_response(resp_json: Dict[str, Any]) -> Dict[str, Any]:
    content = resp_json.get("content") if isinstance(resp_json, dict) else None
    if isinstance(content, dict):
        return {
            "type": content.get("type"),
            "message": content.get("message", "") or "",
            "ref_anchors": content.get("ref_anchors") or [],
            "ref_documents": content.get("ref_documents") or [],
            "mappings": content.get("mappings") or {},
        }

    # Fallback for older schema
    royalty_docs = resp_json.get("content", {}).get("royalty", {}).get("documents", [])
    msg = resp_json.get("content", {}).get("message", "")
    type_ = resp_json.get("content", {}).get("type")
    rd = []
    for i, d in enumerate(royalty_docs):
        rd.append({
            "id": d.get("id", i),
            "lni": d.get("lni"),
            "content_type": d.get("content_type"),
            "document_name": d.get("document_name"),
            "passage_text": d.get("passage_text", ""),
        })
    return {
        "type": type_,
        "message": msg or "",
        "ref_anchors": [],
        "ref_documents": rd,
        "mappings": {},
    }


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
    if not (base_url and upload_ids):
        return {}
    url = f"{base_url.rstrip('/')}/generate_presigned_urls_xhtml/{customer_id}/{database}/{table}/documents"
    payload = {"document_ids": upload_ids}
    headers = {"Content-Type": "application/json"}
    headers.update(headers_kv or {})
    r = requests.post(url, data=json.dumps(payload), headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json() if r.headers.get("content-type","").startswith("application/json") else {}
    return data.get("presigned_urls_xhtml", {}) or {}


def build_cdc_url(base: str, session: Optional[str], upload_id: str, xpath: Optional[str]) -> str:
    frag = f"#{xpath}" if xpath else ""
    if session:
        return f"{base.rstrip('/')}/f/up/v1/{session}/upload/{upload_id}/docxhtml{frag}"
    else:
        return f"{base.rstrip('/')}/f/up/v1/upload/{upload_id}/docxhtml{frag}"


def _pid_fragment(pid_key: str, ent: dict) -> str:
    # Build a compact fragment that encodes pid, xpaths and offsets.
    # Example: "#pid=9;xpaths=/div/p[11]|/div/p[12];offsets=305:24|0:245"
    try:
        pid_num = pid_key.split("-")[1]
    except Exception:
        pid_num = pid_key
    xpaths = ent.get("xpaths") or []
    offsets = ent.get("offsets") or []
    offsets_str = "|".join([f"{o[0]}:{o[1]}" for o in offsets if isinstance(o, (list, tuple)) and len(o) == 2])
    xpaths_str = "|".join([xp for xp in xpaths if isinstance(xp, str)])
    parts = [f"pid={pid_num}"]
    if xpaths_str:
        parts.append(f"xpaths={xpaths_str}")
    if offsets_str:
        parts.append(f"offsets={offsets_str}")
    return "#" + ";".join(parts) if parts else ""


def hyperlink_pids(message: str,
                   mappings: Dict[str, List[Dict[str, Any]]],
                   skyvault_map: Optional[Dict[str, str]],
                   cdc_base: Optional[str],
                   cdc_session: Optional[str],
                   pid_text_map: Optional[Dict[str, str]] = None) -> str:
    if not mappings:
        return message

    def url_for(pid_key: str) -> Optional[str]:
        entries = mappings.get(pid_key) or []
        if not entries:
            return None
        ent = entries[0]
        upload_id = ent.get("upload_identifier")
        # Prefer SkyVault presigned URL
        if skyvault_map and upload_id in skyvault_map:
            base = skyvault_map[upload_id]
            frag = _pid_fragment(pid_key, ent)
            return f"{base}{frag}" if frag else base
        # Fallback to CDC viewer if configured
        if cdc_base and upload_id:
            frag = _pid_fragment(pid_key, ent)
            first_xpath = ent.get("xpaths", [None])[0]
            legacy = f"#{first_xpath}" if first_xpath else ""
            return build_cdc_url(cdc_base, cdc_session, upload_id, None) + (frag or legacy)
        return None

    def repl(m):
        pid_num = m.group(1)
        pid_key = f"pid-{pid_num}"
        url = url_for(pid_key)
        if not url:
            return m.group(0)
        title = html_escape((pid_text_map or {}).get(pid_key, ""))
        return f'<a target="_blank" title="{title}" href="{html_escape(url)}">[pid-{pid_num}]</a>'

    return re.sub(r'\[pid-(\d+)\]', repl, message)


def _extract_json_blocks_from_sse(text: str) -> List[dict]:
    """Extract JSON objects from an SSE-like string (lines start with 'data: {..}')."""
    blocks = []
    for m in re.finditer(r'data:\s*{', text):
        i = m.start()
        start = text.find('{', i)
        depth = 0
        j = start
        while j < len(text):
            ch = text[j]
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    blocks.append(text[start:j+1])
                    break
            j += 1
    parsed = []
    for b in blocks:
        try:
            parsed.append(json.loads(b))
        except Exception:
            pass
    return parsed


def extract_pid_texts_and_mappings_from_file(path: Path) -> Tuple[Dict[str, str], Dict]:
    """
    Parse an SSE/JSONL file to extract:
      - pid_text_map: {'pid-9': 'the captured text ...', ...}
      - mappings: fallback 'content.mappings' if present in any event
    """
    pid_text_map: Dict[str, str] = {}
    mappings: Dict = {}

    if not path.exists():
        return pid_text_map, mappings

    raw = path.read_text(encoding="utf-8", errors="replace")
    events = _extract_json_blocks_from_sse(raw)

    # Gather first non-empty mappings (if any)
    for ev in events:
        c = ev.get("content") if isinstance(ev, dict) else None
        if isinstance(c, dict):
            m = c.get("mappings")
            if isinstance(m, dict) and m and not mappings:
                mappings = m

    # Pull PID-text from fields that contain the builder prompt for Argument tasks
    for ev in events:
        c = ev.get("content") if isinstance(ev, dict) else None
        if not isinstance(c, dict):
            continue
        for key in ("prompt", "ai_prompt", "builder_prompt", "content"):
            val = c.get(key)
            if not isinstance(val, str) or "[pid-" not in val:
                continue
            text = val
            # Pattern B: [pid-12: text...]
            for m in re.finditer(r'\[pid-(\d+)\s*:\s*([^\]]+)\]', text):
                pid = f"pid-{m.group(1)}"
                payload = m.group(2).strip()
                if payload:
                    pid_text_map.setdefault(pid, payload)
            # Pattern A: [pid-12] capture following text up to next pid token
            for m in re.finditer(r'\[pid-(\d+)\]\s*((?:(?!\[pid-\d+\]).)*)', text, flags=re.DOTALL):
                pid = f"pid-{m.group(1)}"
                seg = m.group(2).strip()
                if seg:
                    pid_text_map.setdefault(pid, seg)

    return pid_text_map, mappings


def render_html(qid: str,
                query: str,
                content_type: Optional[str],
                message: str,
                ref_anchors: List[Dict[str, Any]],
                ref_documents: List[Dict[str, Any]],
                mappings: Dict[str, List[Dict[str, Any]]],
                skyvault_map: Optional[Dict[str, str]],
                cdc_base: Optional[str],
                cdc_session: Optional[str],
                pdmfid: str = "1537339",
                pid_text_map: Optional[Dict[str, str]] = None) -> str:
    msg = insert_numeric_anchors(ref_anchors, message)
    msg = hyperlink_pids(msg, mappings, skyvault_map, cdc_base, cdc_session, pid_text_map or {})
    message_html = html_escape(msg).replace("\n", "<br>")

    parts = []
    parts.append("<html><body>")
    parts.append(f"{html_escape(qid)}<br>")
    parts.append("=====<br><br>")
    parts.append("User Prompt:<br>")
    parts.append("============<br>")
    parts.append(f"{html_escape(query)}<br><br>")
    if content_type:
        parts.append(f"Type: {html_escape(str(content_type))}<br><br>")
    parts.append("AI Response:<br>")
    parts.append("================<br>")
    parts.append(f"{message_html}<br><br>")

    # PID References
    if mappings:
        parts.append("PID References:<br>")
        parts.append("===============<br>")
        for pid_key, entries in mappings.items():
            parts.append(f"{html_escape(pid_key)}<br>")
            for ent in entries or []:
                upload_id = ent.get("upload_identifier", "")
                xpaths = ent.get("xpaths") or []
                # Prefer SkyVault map; else CDC fallback; else plain
                url = None
                if skyvault_map and upload_id in skyvault_map:
                    url = skyvault_map[upload_id]
                elif cdc_base and upload_id:
                    url = build_cdc_url(cdc_base, cdc_session, upload_id, None)
                if url:
                    parts.append(f'&nbsp;&nbsp;• <a target="_blank" href="{html_escape(url)}">{html_escape(upload_id)}</a><br>')
                else:
                    parts.append(f"&nbsp;&nbsp;• {html_escape(upload_id)}<br>")
                if xpaths:
                    parts.append("&nbsp;&nbsp;&nbsp;&nbsp;xpaths:<br>")
                    for xp in xpaths:
                        parts.append(f"&nbsp;&nbsp;&nbsp;&nbsp;- {html_escape(xp)}<br>")
            parts.append("<br>")

    # PID Texts
    if pid_text_map:
        parts.append("PID Texts:<br>")
        parts.append("==========<br>")
        for k in sorted(pid_text_map.keys(), key=lambda x: int(re.sub(r'[^0-9]', '', x) or '0')):
            v = pid_text_map[k]
            parts.append(f"{html_escape(k)}: {html_escape(v)}<br>")
        parts.append("<br>")

    parts.append("Citations:<br>")
    parts.append("==========<br>")

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

        parts.append("Document_name:<br>")
        parts.append(f"[{idx}] <a target=\"_blank\" href=\"{html_escape(link)}\">{html_escape(doc_name)}</a><br><br>")
        parts.append("Passage_text:<br><br>")
        parts.append(f"{html_escape(passage)}<br>")
        parts.append(f"{TEMPLATE_SEPARATOR}<br>")

    parts.append("<br>")
    parts.append(f"{TEMPLATE_SEPARATOR}<br>")
    parts.append("</body></html>")
    return "".join(parts)


def main():
    corpora = [CORPUS_TRIPLET]
    payload = build_payload(QUERY, corpora, HEADERS_IN_PAYLOAD, ANSWER_LOCATOR)

    # Predict call
    try:
        resp_json = call_predict(PREDICT_URL, payload)
    except Exception as e:
        # If Predict fails, keep going; we can still render PID-texts from SSE file
        print(f"[WARN] Predict call failed: {e}")
        resp_json = {}

    normalized = parse_predict_response(resp_json) if resp_json else {
        "type": None, "message": "", "ref_anchors": [], "ref_documents": [], "mappings": {}
    }
    mappings = normalized.get("mappings") or {}

    # Extract PID texts and fallback mappings from SSE/response log
    pid_text_map_from_file, mappings_from_file = extract_pid_texts_and_mappings_from_file(RESPONSE_LOG_PATH)
    if pid_text_map_from_file:
        print(f"[INFO] Loaded {len(pid_text_map_from_file)} PID text(s) from {RESPONSE_LOG_PATH}")
    if not mappings and mappings_from_file:
        mappings = mappings_from_file
        print(f"[INFO] Using mappings from {RESPONSE_LOG_PATH}")

    # Parse triplet for SkyVault
    try:
        customer_id, database, table = CORPUS_TRIPLET.split("/", 2)
    except ValueError:
        customer_id = database = table = ""

    # Build PID → URL map from SkyVault presigned URLs
    skyvault_map = {}
    upload_ids = collect_pid_upload_ids(mappings)
    if SKYVAULT_URL and customer_id and database and table and upload_ids:
        try:
            skyvault_map = fetch_skyvault_presigned_map(
                SKYVAULT_URL, customer_id, database, table, upload_ids, HEADERS_IN_PAYLOAD, timeout=None
            )
            print(f"[INFO] Fetched {len(skyvault_map)} presigned XHTML URL(s) from SkyVault")
        except Exception as e:
            print(f"[WARN] SkyVault presigned fetch failed: {e}. Falling back to CDC viewer if configured.")

    html = render_html(
        qid=QID,
        query=QUERY,
        content_type=normalized.get("type"),
        message=normalized.get("message", ""),
        ref_anchors=normalized.get("ref_anchors", []),
        ref_documents=normalized.get("ref_documents", []),
        mappings=mappings,
        skyvault_map=skyvault_map or None,
        cdc_base=CDC_UPLOAD_BASE or None,
        cdc_session=CDC_SESSION or None,
        pdmfid=PDMFID,
        pid_text_map=pid_text_map_from_file or None,
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"[OK] Wrote {OUTPUT_PATH.resolve()}")


if __name__ == "__main__":
    main()
