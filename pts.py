from base_plan.chains.draft.common import get_prompt_token_limit_by_model
from base_plan.chains.draft.prompts.anthropic_helpers import token_count
from base_plan.chains.draft.prompts.prompt_common import (
    DraftConstants,
    compute_dbotf_data,
    compute_doc_data,
    get_dbotf_instructions,
    get_doc_instructions,
    is_invalid_prompt_list,
)

DEFAULT_MAX_PROMPT_TOKENS = DraftConstants.DEFAULT_MAX_PROMPT_TOKENS
DOC_CONTENT_QUOTA = 15000

def compute_history(history):
    if is_invalid_prompt_list(history):
        return []
    return [
        "Here is the chat history:\n<chat_history>",
        *history,
        "</chat_history>",
    ]

# Removed compute_references and any buffer use of external “references”,
# since we no longer include or rely on LNI/search_result context.

def compute_query(query):
    return [
        "\nPresume that you are in England unless told otherwise. Ensure that your answer is ",
        "appropriate for the jurisdiction. Here is the user input query:\n<query>",
        query,
        "</query>",
    ]


def compute_instructions():
    """
    DBOTF/Uploaded-documents only; use <documents> and [pid-x] citations exclusively.
    No search_results, no [id=n], no mixed formats.
    """
    return [
        "* Follow these instructions when writing persuasive arguments or counterarguments:",
        "First, carefully review the supplied <documents></documents>. Each document contains paragraphs tagged as <pid-x>…</pid-x>.",
        "Identify only those paragraphs that most directly address the legal issues and facts raised in the <query>. You may internally re-order or prioritise paragraphs.",
        "Second, take time to analyse the legal background in light of the <query> and the selected document paragraphs.",
        "In <thoughts></thoughts>, note down the key legal principles, provisions, clauses, definitions, or arguments from the documents that support your position.",
        "Third, within <issues_and_facts></issues_and_facts>, create a bullet-point list of all legal issues and facts you identified in the <query> (do not add new facts).",
        "Fourth, write the legal argument or counterargument inside <draft></draft> tags. When drafting, follow these rules:",
        "- Do not write a letter or court submission; write a well-structured argument/explanation.",
        "- Be objective and avoid tailoring or omitting information to fit a narrative.",
        "- Base claims solely on the supplied <documents> when you need evidentiary or textual support.",
        "- Any facts about the scenario must come directly from the <query>; do not import new facts from <documents> unless the query explicitly asks you to rely on them.",
        "- If document passages conflict, acknowledge and explain the conflict.",
        "- Do not use any external knowledge beyond the supplied <documents> and <query>.",
        "- Do not invent facts, scenarios, or events.",
        "- Prefer concise, persuasive, and logically structured analysis with clear signposting.",
        "- When you rely on specific document content (clauses, definitions, factual excerpts), cite the paragraph(s) using [pid-x] at the end of the supporting sentence, followed by a full stop.",
        "- Use no more than three [pid-x] citations per sentence; only cite when the sentence actually depends on that paragraph.",
        "Your legal argument MUST use British English and UK legal terminology.",
        "Prefer British terms and phrasing (e.g., 'The claimant succeeds', 'Counsel for the defendant', 'The action was unauthorised').",
        "Do not include profanity or discriminatory language.",
        "Conclude with a brief, clear reiteration of your overall conclusion.",
    ]


def compute_system_instructions():
    """
    System framing now mentions only chat history, documents, and the query.
    No search engine, no content types, no LNI layers.
    """
    return [
        "You are an experienced solicitor with great expertise and skill at writing persuasive, concise, and "
        "thoroughly supported legal arguments in British English.",
        "For each query, you may be provided with:",
        "- A chat history between the client and yourself, if it exists.",
        "- Relevant uploaded/DBOTF documents within <documents></documents>, whose paragraphs are tagged as <pid-x>…</pid-x>.",
        "- The specific user query to which your drafting should be responsive.",
        "Use only the supplied <documents> to support legal analysis when support is required, and cite using [pid-x].",
    ]


def compute_prompt(query, history=None, dbotf_data=None, doc_data=None, **kwargs):
    """
    Buffer excludes 'references' and any 'search_results'.
    Only DBOTF/doc data + UK system instructions + PID-only drafting instructions.
    """
    dbotf_data_quota_applied = "DBOTF_PLACEHOLDER"
    doc_data_quota_applied = "DOC_PLACEHOLDER"
    feature_flags = kwargs.get("feature_flags", None)

    buffer = [
        *compute_system_instructions(),
        "",
        *compute_history(history),
        "",
        dbotf_data_quota_applied,
        doc_data_quota_applied,
        *compute_instructions(),
        "",
        *get_dbotf_instructions(dbotf_data, feature_flags=feature_flags),
        "",
        *get_doc_instructions(doc_data, feature_flags=feature_flags),
        "",
        *compute_query(query),
    ]

    llm_model = kwargs.get("llm_model", "")
    if llm_model:
        quota = get_prompt_token_limit_by_model(llm_model) - token_count("".join(buffer))
    else:
        quota = DraftConstants.DEFAULT_MAX_PROMPT_TOKENS - token_count("".join(buffer))

    # Apply quotas
    doc_data_index = buffer.index(doc_data_quota_applied)
    buffer[doc_data_index: doc_data_index + 1] = compute_doc_data(
        doc_data, quota=quota, doc_quota=DOC_CONTENT_QUOTA, feature_flags=feature_flags
    )
    dbotf_data_index = buffer.index(dbotf_data_quota_applied)
    buffer[dbotf_data_index: dbotf_data_index + 1] = compute_dbotf_data(
        dbotf_data, quota=quota, doc_quota=DOC_CONTENT_QUOTA, feature_flags=feature_flags
    )
    return buffer


CITATION_INSTRUCTIONS_ANSWER_LOCATOR = [
    # What to analyse
    "Analyze the <documents> * </documents> content comprehensively. Pertinent information is any material in the documents "
    "that directly concerns the legal issue(s) raised in the <query>, including relevant facts, clauses, definitions, "
    "substantive content, required legal rules referenced in the documents, terms and conditions, or background details.",
    "Thoroughly review <documents></documents> to identify key legal facts, arguments, clauses, and evidence that directly relate to the <query>.",
    "Use relevant document content to support factual assertions and legal arguments so the response is grounded in the specific details of the case.",
    "When discussing specific facts or events, ensure relevant document content is cited (if the sentence depends on that content).",
    "If the documents contain conflicts or discrepancies, surface and explain them.",
    "Cross-reference related paragraphs within the documents to build a cohesive, well-supported analysis.",
    "Maintain consistent UK legal terminology and a professional tone throughout.",
    "Be mindful of potential biases in document content and present a balanced view where appropriate.",
    "Integrate essential context, facts, and issues from <documents> into your draft.",
    # PID-only citation policy
    "**Citation Format Instructions for paragraphs <pid-i></pid-i> in <documents> * </documents>.**",
    "Uploaded/DBOTF document paragraphs are tagged as <pid-x>…</pid-x>.",
    "Cite paragraphs from <documents> only if they directly support the sentence.",
    "Format document paragraph citations strictly as: [pid-4], [pid-17], etc.",
    "Place citations only at the end of the sentence they support, followed by a full stop.",
    "Avoid citing more than three document paragraphs per sentence.",
    "Never invent or guess PIDs.",
    "Do not overuse the same PID repeatedly; cite only when necessary for support.",
    # Examples (PID-only)
    '<examples_of_improper_citation note="These are improper because they do not use [pid-x] at the end.">',
    '"As shown in _, _ cannot be considered to _."',
    '"As regards _, the proper protocol for _ would have been to _. [some document name]."',
    "</examples_of_improper_citation>",
    '<examples_of_proper_document_paragraph_citation note="These examples ARE formatted as [pid-x], appear after the period, and each citation is followed by a period.">',
    '"The Department of _ defines _ as _. [pid-22], [pid-1]."',
    '"At the time of _, _ had not yet _, meaning she could not be said to _. [pid-12], [pid-2]."',
    '"Standards for _ only apply when _, conditions that do not hold in this case. [pid-2], [pid-13], [pid-1]."',
    "</examples_of_proper_document_paragraph_citation>",
]
