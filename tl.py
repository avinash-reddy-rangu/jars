from base_plan.common.schema.msg_type import MsgType
from uk_plan.chains.ask_doc.tasks.dbotf.utils import gen_firm_documents
from uk_plan.chains.ask_doc.tasks.map_reduce_timeline import UkMapReduceTimelineTask
from uk_plan.chains.ask_doc.prompt.map_reduce_timeline import TIMELINE_MAP_TEMPLATE_COMPLETE_DOC_LOCATOR
from uk_plan.chains.ask_doc.tasks.retrieval.vault_retrieval_task import VaultRetrievalTask
from uk_plan.chains.ask_doc.tasks.answer_post_process.timeline_post_process_task_fix_abnormal_date import (
    TimelinePostProcessFixDateTask as UkTimelinePostProcessFixDateTask,
)

from uk_plan.common.const.ask_doc import TimeAnswerFormat, UkDBOTFIntentType, UkDBOTFTaskName, UkErrorMsg
from uk_plan.common.schema.ask_doc import UkInputSchema
from uk_plan.common.schema.toggle import FeatureToggle
from uk_plan.config.ask_doc import config
from rag.schema import Progress


class DBOTFTimelineTask(UkMapReduceTimelineTask):
    name = UkDBOTFTaskName.map_reduce_timeline
    NO_RELEVANT_MESSAGE = "No relevant information found"

    def __init__(
        self,
        input_model: UkInputSchema,
        tenant: str,
        tracing_info: dict,
        model: str = config.GPT_MODEL,
        max_tokens_per_chunk: int = config.MAX_TOKENS_PRE_CHUNK,
        chunk_overlap: int = config.CHUNK_OVERLAP,
        stream: bool = True,
        abe_toggle: FeatureToggle | None = None,
    ):
        super().__init__(
            input_model, tenant, tracing_info, model, max_tokens_per_chunk, chunk_overlap, stream, abe_toggle
        )

    def condition(self) -> bool:
        intents = self.get_task_output(UkDBOTFTaskName.intent)
        if not intents or intents[0] != UkDBOTFIntentType.Timeline:
            return False

        self.logger.info(f"Ask doc run {self.name} solution, intent:{intents}")
        return True

    def get_document_datas(self, chunks: list[dict]) -> tuple[list[str], list[str]]:
        document_names, document_plain_texts = [], []
        for item in chunks:
            document_names.append(item.get("title", ""))
            document_plain_texts.append(item.get("passage_text", ""))
        return document_names, document_plain_texts

    def run(self):
        self.__send_hint()
        chunks = list(self.get_task_output(UkDBOTFTaskName.retrieval))
        document_names, document_plain_texts = self.get_document_datas(chunks)
        
        if self._abe_toggle.ASK_DOC_ENABLE_TIMELINE_DOC_LOCATOR:
            try:
                _vret = VaultRetrievalTask(stream=self._stream, abe_toggle=self._abe_toggle, tracing_info=self._input_model.tracing_info).run(self._input_model, force_download_xhtml=True)
                try:
                    self.set_task_output(UkDBOTFTaskName.retrieval, _vret)
                except Exception:
                    pass
                if getattr(_vret, "doc_list", None):
                    document_plain_texts = [getattr(_d, "plain_text", "") for _d in _vret.doc_list] or document_plain_texts
            except Exception as _e:
                self.logger.warning(f"Locator vault XHTML path failed, falling back. Err={_e}")
        document_chunks, document_scopes = self._get_chunk_infos(
            document_plain_texts, self._max_tokens_per_chunk, self._chunk_overlap
        )
        # step 1. break query
        rewrite_query = self.get_task_output(UkDBOTFTaskName.contextual_query_rewrite)
        final_query = rewrite_query if rewrite_query else self._input_model.query
        self.query1 = final_query

        # step 2. map stage
        map_answers = self.map_answers(final_query, document_names, document_chunks, document_scopes)
        return self.generate_answer(final_query, document_names, map_answers, chunks)

    def __send_hint(self):
        self.update_progress(
            Progress(
                type=MsgType.MESSAGE_HINT,
                name="",
                content=self.__get_progress_content(),
            )
        )

    def __get_progress_content(self):
        return {
            "hint_name": "relevant_results",
            "hint": "relevant results",
            "hint_header": "Retrieving",
        }

    def generate_answer(
        self,
        _: str,
        doc_names: list[str],
        map_answers: list[list[str]],
        chunks: list[dict],
    ):
        """_summary_

        Args:
            _ (str): _description_
            doc_names (list[str]): _description_
            map_answers (list[list[str]]): _description_

        Returns:
            str: _description_
        """
        answer_format = TimeAnswerFormat.consolidate
        self.logger.info(f"Ask doc {self.name} output format: {answer_format}")

        consolidated_chronology = self.get_consolidated_chronology(doc_names, map_answers)
        consolidated_chronology = self._postprocess_chronology(consolidated_chronology)

        if not consolidated_chronology:
            return self.event_response(UkErrorMsg.EVENT_NOT_FOUND, firm_documents=[])

        event_has_intercept = False
        if len(consolidated_chronology) > config.EVENT_LIMIT:
            consolidated_chronology = consolidated_chronology[: config.EVENT_LIMIT]
            event_has_intercept = True

        llm_answer = self.get_consolidated_answer(consolidated_chronology, event_has_intercept=event_has_intercept)
        llm_answer = self.post_process(llm_answer)
        firm_documents = gen_firm_documents(chunks, llm_answer)
        self._send_firm_documents(firm_documents)

        
        if self._abe_toggle.ASK_DOC_ENABLE_TIMELINE_DOC_LOCATOR:
            _post = UkTimelinePostProcessFixDateTask(
                data_source_type="vault",
                stream=self._stream,
                abe_toggle=self._abe_toggle,
                tracing_info=self._input_model.tracing_info,
                max_workers=16,
            )
            if not self._stream or len(llm_answer) > config.MAX_STREAM_ANSWER_LEN:
                return _post.event_response(llm_answer, firm_documents)
            else:
                return _post.streaming_response(llm_answer, firm_documents)
if self._abe_toggle.ProtegePreview:
            return self.get_timeline_widget_response(consolidated_chronology, event_has_intercept, firm_documents)

        if not self._stream or len(llm_answer) > config.MAX_STREAM_ANSWER_LEN:
            return self.event_response(llm_answer, firm_documents)
        return self.streaming_response(llm_answer, firm_documents)
