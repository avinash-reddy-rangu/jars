# uk_plan/chains/ask_doc/common/utils/xhtml_build.py  (new thin wrapper to keep call sites clean)
from uk_plan.chains.ask_doc.common.utils.xhtmls_build_xpath_mapping import build_xpath_mapping
from uk_plan.common.schema.ask_doc import XhtmlData  # if missing, define a light Pydantic model locally

def to_xhtml_data_map(upload_identifier_to_xhtml: dict[str, str]) -> list[XhtmlData]:
    """
    Takes {upload_identifier: raw_xhtml} and returns a list of XhtmlData with:
      - plain_text  (contains sentence IDs like (d0s15))
      - id_anchor_map
      - all_xpaths
      - upload_identifier
    """
    return build_xpath_mapping(upload_identifier_to_xhtml)


# AFTER (UK with locator)
from uk_plan.chains.ask_doc.common.utils.xhtml_build import to_xhtml_data_map

# documents -> you must have the original XHTML per upload
upload_identifier_to_xhtml = { d["upload_identifier"]: d["xhtml"] for d in documents }  # ensure you have this
xhtml_datas = to_xhtml_data_map(upload_identifier_to_xhtml)

document_names = [xd.upload_identifier for xd in xhtml_datas]
document_plain_texts = [xd.plain_text for xd in xhtml_datas]
self._xhtml_datas = xhtml_datas  # save for post-process


    from pydantic import BaseModel
    class XhtmlData(BaseModel):
        upload_identifier: str
        plain_text: str           # contains (dXsY)
        id_anchor_map: dict       # { "d0s15": {... anchors ...}, ... }
        all_xpaths: dict          # optional, used for windowing/filtering
        origin_text: str | None = None

# ADD inside UkMapReduceTimelineTask
def _build_xhtml_datas_from_download(self) -> list:
    """
    Build XhtmlData objects (with plain_text containing (dXsY), id_anchor_map, all_xpaths)
    from the output of UkAskDocTaskName.download_document.
    """
    doc_xhtml_map: dict[str, str] = self.get_task_output(UkAskDocTaskName.download_document) or {}
    if not doc_xhtml_map:
        return []
    # build_xpath_mapping returns a list of XhtmlData-like models:
    #   fields: upload_identifier, plain_text(with dXsY), id_anchor_map, all_xpaths, origin_text
    return build_xpath_mapping(doc_xhtml_map)

documents = self.get_task_output(AskDocTaskName.document_plain_text)
if documents is None:
    # (DBOTF retrieval path unchanged)
    chunks = list(self.get_task_output(UkDBOTFTaskName.retrieval))
    document_names, document_plain_texts = self.get_document_datas_chunk(chunks)
else:
    # Upload path
    if getattr(config, "ASK_DOC_ENABLE_TIMELINE_DOC_LOCATOR", False):
        # Try to build XHTML→(dXsY)
        self._xhtml_datas = self._build_xhtml_datas_from_download()
        if self._xhtml_datas:
            document_names = [xd.upload_identifier for xd in self._xhtml_datas]
            document_plain_texts = [xd.plain_text for xd in self._xhtml_datas]  # contains (dXsY)
        else:
            # Fall back to your existing plain-text path if XHTML isn’t available
            document_names, document_plain_texts = self.get_document_datas(documents)
    else:
        # Locator disabled: keep original behavior
        document_names, document_plain_texts = self.get_document_datas(documents)

# ADD: Answer Locator timeline post-process
if getattr(config, "ASK_DOC_ENABLE_TIMELINE_DOC_LOCATOR", False):
    # Build mappings and inject placeholders [[n]]
    tl_pp = UkTimelinePostProcessTask(
        xhtml_datas=getattr(self, "_xhtml_datas", []),    # may be [] if not built; that's OK
        enable_locator=True,
        enable_xpath_filter=getattr(config, "ASK_DOC_ENABLE_XPATH_FILTER", False),
        placeholder_fmt=getattr(config, "ANSWER_LOCATOR_PLACEHOLDER", "[[{}]]"),
        event_limit=getattr(config, "EVENT_LIMIT", 30),
        stream_handler=self._stream_handler if hasattr(self, "_stream_handler") else None,
        tracing_info=getattr(self, "_tracing_info", None),
    )
    # The post-processor accepts either:
    #  - events that contain DocViewer anchors (from process_mark), OR
    #  - events whose description includes (dXsY), resolvable via xhtml_datas
    tl_result = tl_pp.process(consolidated_chronology)
    consolidated_chronology = tl_result.get("events", consolidated_chronology)
    # mappings are streamed by the post-processor; keep llm_answer construction below unchanged


# ADD: Answer Locator timeline post-process
if getattr(config, "ASK_DOC_ENABLE_TIMELINE_DOC_LOCATOR", False):
    # Build mappings and inject placeholders [[n]]
    tl_pp = UkTimelinePostProcessTask(
        xhtml_datas=getattr(self, "_xhtml_datas", []),    # may be [] if not built; that's OK
        enable_locator=True,
        enable_xpath_filter=getattr(config, "ASK_DOC_ENABLE_XPATH_FILTER", False),
        placeholder_fmt=getattr(config, "ANSWER_LOCATOR_PLACEHOLDER", "[[{}]]"),
        event_limit=getattr(config, "EVENT_LIMIT", 30),
        stream_handler=self._stream_handler if hasattr(self, "_stream_handler") else None,
        tracing_info=getattr(self, "_tracing_info", None),
    )
    # The post-processor accepts either:
    #  - events that contain DocViewer anchors (from process_mark), OR
    #  - events whose description includes (dXsY), resolvable via xhtml_datas
    tl_result = tl_pp.process(consolidated_chronology)
    consolidated_chronology = tl_result.get("events", consolidated_chronology)
    # mappings are streamed by the post-processor; keep llm_answer construction below unchanged

