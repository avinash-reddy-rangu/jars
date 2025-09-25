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

