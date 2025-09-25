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
