
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import orjson
from base_plan.chains.ask_doc.common.constants import AskDocTaskName
from base_plan.chains.ask_doc.tasks.map_reduce import MapReduceTask
from base_plan.common.schema.ask_doc import EventFinish, Royalty
from uk_plan.chains.ask_doc.common.docviewer_task import DocviewerTask
from uk_plan.chains.ask_doc.common.stream_handle import UkLLMAnswerHandler, UkStreamHandler
from uk_plan.chains.ask_doc.common.timeline_handle import TimelineHandler
from uk_plan.chains.ask_doc.prompt.map_reduce_timeline import (
    TIMELINE_MAP_TEMPLATE_DOC_VIEWER,
    TIMELINE_MAP_TEMPLATE_COMPLETE_DOC_LOCATOR,  # added
)
from uk_plan.chains.ask_doc.tasks.retrieval.upload_retrieval_task import UploadRetrievalTask  # added
from uk_plan.chains.ask_doc.tasks.retrieval.vault_retrieval_task import VaultRetrievalTask  # added
from uk_plan.chains.ask_doc.tasks.answer_post_process.timeline_post_process_task_fix_abnormal_date import (
    TimelinePostProcessFixDateTask as UkTimelinePostProcessFixDateTask,  # added
)
from uk_plan.chains.ask_doc.tasks.dbotf.utils import consolidated_chronology_to_json_docviewer
from uk_plan.chains.ask_doc.schema import UKInputSchema
from rag import Progress
from uk_plan.chains.ask_doc.utils import (
    gen_widget_uuid_markup,
    postprocess_chronology,
    sort_json_chronology,
    split_string,
)
from uk_plan.common.const.ask_doc import (
    TimeAnswerFormat,
    UkAskDocTaskName,
    UkDBOTFTaskName,
    UkErrorMsg,
)
from uk_plan.common.schema.ask_doc import (
    FirmDocument,
    TimeLineDocviewerEventFinish,
    TimelineMessage,
    UkDocViewerEventFinish,
)
from uk_plan.common.schema.toggle import FeatureToggle
from uk_plan.utils.text_utils import convert_us_to_british
from uk_plan.config.ask_doc import config
from difflib import SequenceMatcher

class UkMapReduceTimelineTask(MapReduceTask):
    name = UkAskDocTaskName.map_reduce_timeline
    TIMELINE_TEMPLATE = "<document index='{index}'>\n<title>{doc_title}</title>\n{doc_content}\n</document>"
    PREFIX_MESSAGE = "This is a response containing a timeline of events: \n"
    EVENT_LIMIT_MESSAGE = (
        f"\n\nThe Timeline feature has extracted the first {config.EVENT_LIMIT} events from your documents."
    )
    NO_RELEVANT_MESSAGE = "No relevant information found"

    def __init__(
        self,
        input_model: UKInputSchema,
        tenant: str,
        tracing_info: dict,
        model: str = config.GPT_MODEL,
        max_tokens_per_chunk: int = config.MAX_TOKENS_PRE_CHUNK,
        chunk_overlap: int = config.CHUNK_OVERLAP,
        stream: bool = True,
        abe_toggle: FeatureToggle | None = None,
    ):
        super().__init__(input_model, tenant, tracing_info, model, max_tokens_per_chunk, chunk_overlap)
        self.logger.info(f"Ask doc: UkMapReduceTimelineTask: constructor: {model}")
        self._stream = stream
        self._abe_toggle = abe_toggle or FeatureToggle()

    def condition(self) -> bool:
        intents = self.get_task_output(AskDocTaskName.intent)
        document_texts, document_tokens = self.document_details
        if intents and intents[0] == "S":
            self.logger.info(
                f"Ask doc run {self.name} solution, documents:{len(document_texts)}|tokens:{document_tokens}"
            )
            return True
        return False

    def generate_answer(self, _: str, doc_names: list[str], map_answers: list[list[str]]):
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
            return self.event_response(UkErrorMsg.EVENT_NOT_FOUND, [], [])

        event_has_intercept = False
        if len(consolidated_chronology) > config.EVENT_LIMIT:
            consolidated_chronology = consolidated_chronology[: config.EVENT_LIMIT]
            event_has_intercept = True

        llm_answer = self.get_consolidated_answer(consolidated_chronology, event_has_intercept=event_has_intercept)
        llm_answer = self.post_process(llm_answer)
        llm_answer = convert_us_to_british(llm_answer)
        firm_documents = DocviewerTask.gen_firm_documents(self._input_model.documents, llm_answer)

        # Answer Locator: post-process to emit mappings and replace (dXsY) with [n]
        if self._abe_toggle.ASK_DOC_ENABLE_TIMELINE_DOC_LOCATOR:
            _post = UkTimelinePostProcessFixDateTask(
                data_source_type="upload",
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
        self._send_firm_documents(firm_documents)
        return self.streaming_response(llm_answer, firm_documents)

    def get_timeline_widget_response(self, consolidated_chronology, event_has_intercept, firm_documents):
        self.logger.info(f"Ask doc {self.name} response timeline style")
        timeline_handler = TimelineHandler([])

        message = self.get_consolidated_message(consolidated_chronology, timeline_handler, event_has_intercept)
        timeline_response = TimeLineDocviewerEventFinish(
            message=orjson.dumps(message.model_dump()).decode(),
            firm_documents=firm_documents,
            royalty=Royalty(),
        )

        return timeline_response

    def get_consolidated_message(
        self, consolidated_chronology: list[dict], timeline_handler: TimelineHandler, event_has_intercept: bool = False
    ) -> TimelineMessage:
        """get consolidated message

        Args:
            consolidated_chronology (list[str]): _description_
            timeline_handler (TimelineHandler): _description_

        Returns:
            TimelineMessage: _description_
        """
        widgets = timeline_handler.get_timeline_widgets(consolidated_chronology)
        if not widgets:
            return TimelineMessage(content=UkErrorMsg.EVENT_NOT_FOUND, event_limit=config.EVENT_LIMIT)

        widget_uuid, widget_markup = gen_widget_uuid_markup(str(widgets))
        content = self.PREFIX_MESSAGE + widget_markup
        if event_has_intercept:
            content += self.EVENT_LIMIT_MESSAGE

        message = TimelineMessage(
            content=content,
            widget_data={widget_uuid: widgets},
            event_limit=config.EVENT_LIMIT,
        )
        return message

    @staticmethod
    def json_to_chronology(events: list[dict]) -> str:
        """
        Convert the list of dictionaries to a chronology
        """
        chronology = ""
        for event in events:
            chronology += f"{event['title']} \n"
            chronology += f"{event['time']}: {event['event']}\n"
        return chronology.strip()

    def get_consolidated_answer(
        self,
        consolidated_chronology: list[dict],
        add_header: bool = True,
        event_has_intercept: bool = False,
    ) -> str:
        """Get consolidated string answer

        Args:
            consolidated_chronology (list[dict]): _description_

        Returns:
            str: _description_
        """
        answer = ""
        if add_header:
            answer += self.PREFIX_MESSAGE
        answer += self.json_to_chronology(consolidated_chronology)
        if event_has_intercept:
            answer += self.EVENT_LIMIT_MESSAGE
        return answer

    def streaming_response(self, answer: str, firm_documents: list[FirmDocument]) -> EventFinish:
        self.logger.info(f"Ask doc {self.name} streaming response")
        answer_handler = UkLLMAnswerHandler()
        with UkStreamHandler(({"completion": answer},)) as stream_handler:
            for _answer in answer_handler(stream_handler):
                if not _answer:
                    continue
                for item in split_string(_answer):
                    self.send_llm_output(item)

        event_response = self.event_response(firm_documents=firm_documents, **answer_handler.construct_response)
        return event_response

    def _get_reduce_prompt(self, doc_names: list[str], map_answers: list[list[str]], prompt_tpl: str) -> str:
        one_doc_template = f"{self.NO_RELEVANT_MESSAGE}."

        templates = []
        for doc_name, one_doc_chunks in zip(doc_names, map_answers):
            one_doc_chunks = [
                one_doc_chunk for one_doc_chunk in one_doc_chunks if one_doc_template not in one_doc_chunk
            ]
            one_doc_chunks = [re.sub("\n+", "\n", chunk) for chunk in one_doc_chunks]
            one_doc_content = "\n\n".join(one_doc_chunks)
            if not one_doc_content:
                one_doc_content = one_doc_template

            template_prompt = self.TIMELINE_TEMPLATE.format(
                index=doc_names.index(doc_name), doc_title=doc_name, doc_content=one_doc_content
            )
            templates.append(template_prompt)

        files_temp = "\n\n".join(templates)
        prompt = prompt_tpl.format(files_temp=files_temp, query=self.query2)
        prompt = re.sub("\n{3,99}", "\n\n", prompt)
        return prompt

    def get_consolidated_chronology(self, doc_names: list[str], map_answers: list[list[str]]) -> Optional[list[dict]]:
        """Get consolidated chronology answer

        Args:
            doc_names (list[str]): List of document names
            map_answers (list[list[str]]): List of map answers

        Returns:
            Optional[list[dict]]: Consolidated chronology answer
        """
        merged_map_answers = self.merge_timeline_rule(map_answers)

        map_answers_regex = []
        no_relevant_count = 0
        for index, (_, doc_answer) in enumerate(zip(doc_names, merged_map_answers)):
            # regex
            if self.NO_RELEVANT_MESSAGE not in doc_answer:
                pattern = r"\[\^(\d+)\]"
                replacement = f"[({index},\\1)]"
                # Substitute the citation pattern from [^snippet_index] to [doc_index, snippet_index]
                doc_answer_regex = re.sub(pattern, replacement, doc_answer)
                map_answers_regex.append(doc_answer_regex)
            else:
                no_relevant_count += 1

        if no_relevant_count == len(map_answers):
            consolidated_json_answer = None
        else:
            final_chronology = "\n".join(map_answers_regex)
            json_answer = consolidated_chronology_to_json_docviewer(final_chronology)
            consolidated_json_answer = sort_json_chronology(json_answer, self.query1)

            # Trying to filter out duplicate events
            filtered: list[dict] = []
            for ev in consolidated_json_answer:
                is_dup = any(
                    ev["time"] == kept["time"]  # exact same day
                    and SequenceMatcher(None,
                                        ev["event"].lower(),
                                        kept["event"].lower()
                                        ).ratio() >= 0.75  # ≥75 % text match
                    for kept in filtered
                )
                if not is_dup:
                    filtered.append(ev)

            consolidated_json_answer = filtered

            for event in consolidated_json_answer:
                match = re.search(r"\[\((\d+),(\d+)\)\]", event['event'])
                if match:
                    event['source'] = match.group(0)[1:-1]

        self.logger.debug(f"Ask doc {self.name} consolidate answer: {consolidated_json_answer}")
        return consolidated_json_answer

    def _postprocess_chronology(self, chronology: Optional[list[dict]]):
        if chronology is None:
            return None
        for event in chronology:
            event["source"] = event.get("title", "")

        if self._abe_toggle.ASK_DOC_DOCVIEWER and self.name == UkAskDocTaskName.map_reduce_timeline:
            chronology = orjson.loads(
                DocviewerTask.process_mark(
                    chunks=self.document_chunks,
                    documnt_xhtml_texts=self.get_task_output(UkAskDocTaskName.download_document) or {},
                    llm_answer=orjson.dumps(chronology).decode(encoding="utf-8"),
                )
            )
        elif self._abe_toggle.ASK_DOC_DBOTF_DOCVIEWER and self.name == UkDBOTFTaskName.map_reduce_timeline:
            chronology = orjson.loads(
                DocviewerTask.process_mark(
                    chunks=self.document_chunks,
                    documnt_xhtml_texts={},
                    llm_answer=orjson.dumps(chronology).decode(encoding="utf-8"),
                )
            )
        else:
            for event in chronology:
                event["event"] = re.sub(r"\[[\(\)\d, ]+\]", "", event.get("event", ""))
        return chronology

    def merge_timeline_rule(self, map_answers: list[list[str]]) -> list[str]:
        merged_answers = []
        for one_doc_chunks in map_answers:
            try:
                one_doc_chunks = [
                    one_doc_chunk for one_doc_chunk in one_doc_chunks if self.NO_RELEVANT_MESSAGE not in one_doc_chunk
                ]
                one_doc_chunks = [
                    one_doc_chunk.split("<timeline>")[1].split("</timeline>")[0].strip()
                    for one_doc_chunk in one_doc_chunks
                ]
                one_doc_content = "\n".join(one_doc_chunks)
                one_doc_content = postprocess_chronology(one_doc_content)
                if not one_doc_content:
                    one_doc_content = f"{self.NO_RELEVANT_MESSAGE}."
                merged_answers.append(one_doc_content)
            except Exception:
                merged_answers.append(f"{self.NO_RELEVANT_MESSAGE}.")

        return merged_answers

    def _get_map_prompts(self, query: str, document_chunks: list[list[str]], document_names: list[str]) -> list[str]:
        prompts = []
        DOC_TEMP = """<doc name="{doc_name}">\n{doc}\n</doc>"""
        for doc_name, chunks in zip(document_names, document_chunks):
            for chunk_index, chunk in enumerate(chunks):
                chunk_temp = DOC_TEMP.format(doc_name=doc_name, doc=chunk)
                tpl = TIMELINE_MAP_TEMPLATE_COMPLETE_DOC_LOCATOR if self._abe_toggle.ASK_DOC_ENABLE_TIMELINE_DOC_LOCATOR else TIMELINE_MAP_TEMPLATE_DOC_VIEWER
                prompt = tpl.format(
                    doc_names=document_names, doc=chunk_temp, index=chunk_index, query=query
                )
                prompt = prompt.replace("{", "{{").replace("}", "}}")
                prompts.append(prompt)
        return prompts

    def map_answers(
        self, query: str, document_names: list[str], document_chunks: list[list[str]], document_scopes: list[list[int]]
    ) -> list[list[str]]:
        self.document_chunks = document_chunks
        prompts = self._get_map_prompts(query, document_chunks, document_names)

        index = 0
        map_answers = [[] for _ in document_names]
        answers = self.llm_predict_batch(prompts)
        for i, llm_response in enumerate(answers):
            try:
                summary = llm_response.split("<summary>")[1].split("</summary>")[0].strip()
                timeline = llm_response.split("<timeline>")[1].split("</timeline>")[0].strip()
                response = f"<summary>\n{summary}\n</summary>\n<timeline>\n{timeline}\n</timeline>"
            except Exception:
                response = llm_response
            answer = f"<chronology>\n{response}\n</chronology>"
            if i > document_scopes[index][1]:
                index += 1
            map_answers[index].append(answer)
        return map_answers

    def break_query(self, query: str) -> tuple[str, str]:
        """break user query

        :param query: The original query
        :return: _description_
        """
        self.query1, self.query2 = query, query
        return self.query1, self.query2

    def llm_predict_batch(self, prompts: list[str], max_workers: int = 12) -> list[str]:
        """llm proxy batch request

        :param prompts: prompts
        :param max_workers: max request limit, defaults to 12
        :return: _description_
        """
        results = []
        max_workers = max(max_workers, len(prompts))
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ask-dock-ask") as executor:
            tasks = [executor.submit(self.llm_predict, prompt, {}, self._model) for prompt in prompts]
            for future in tasks:
                llm_answer = future.result()
                if not llm_answer:
                    continue
                results.append(llm_answer)
        return results

    def event_response(self, llm_answer: str, firm_documents: list[FirmDocument], *args, **kwargs) -> EventFinish:
        """_summary_

        :param llm_answer: _description_
        :param ref_anchors: _description_
        :param ref_documents: _description_
        :return: _description_
        """
        royalty = Royalty()
        if self._abe_toggle.ASK_DOC_DOCVIEWER and self.name == UkAskDocTaskName.map_reduce_timeline:
            event_finish = UkDocViewerEventFinish(
                message=llm_answer,
                royalty=royalty,
                firm_documents=firm_documents,
            )
        elif self._abe_toggle.ASK_DOC_DBOTF_DOCVIEWER and self.name == UkDBOTFTaskName.map_reduce_timeline:
            event_finish = UkDocViewerEventFinish(
                message=llm_answer,
                royalty=royalty,
                firm_documents=firm_documents,
            )
        else:
            event_finish = EventFinish(message=llm_answer, royalty=royalty)
        self.logger.debug(f"Ask doc {self.name} final response: {event_finish}")
        return event_finish

    def _send_firm_documents(self, firm_documents):
        if not firm_documents:
            return
        royalty = Royalty()
        self.send_ref_docs({"firm_documents": firm_documents, "ref_documents": [], "royalty": royalty})

    def get_document_datas_chunk(self, chunks: list[dict]) -> tuple[list[str], list[str]]:
        document_names, document_plain_texts = [], []
        for item in chunks:
            document_names.append(item.get("title", ""))
            document_plain_texts.append(item.get("passage_text", ""))
        return document_names, document_plain_texts

    def run(self):
        self.logger.info(f"Ask doc: {self.name}")
        hint = "relevant results"
        hint_header = "Retrieving"
        self.update_progress(
            Progress(
                type="hint",
                name="",
                content={
                    "hint_name": "relevant_results",
                    "hint": hint,
                    "hint_header": hint_header,
                },
            )
        )
        documents = self.get_task_output(AskDocTaskName.document_plain_text)
        if documents is None:
            chunks = list(self.get_task_output(UkDBOTFTaskName.retrieval))
            document_names, document_plain_texts = self.get_document_datas_chunk(chunks)
        else:
            document_names, document_plain_texts = self.get_document_datas(documents)
        
        # Answer Locator: upgrade to XHTML + sentence labels for upload flow
        if self._abe_toggle.ASK_DOC_ENABLE_TIMELINE_DOC_LOCATOR:
            try:
                _ret = UploadRetrievalTask(stream=self._stream, abe_toggle=self._abe_toggle, tracing_info=self._input_model.tracing_info).run(self._input_model)
                try:
                    self.set_task_output(UkAskDocTaskName.retrieval, _ret)
                except Exception:
                    pass
                if getattr(_ret, "doc_list", None):
                    document_plain_texts = [getattr(_d, "plain_text", "") for _d in _ret.doc_list] or document_plain_texts
            except Exception as _e:
                self.logger.warning(f"Locator upload XHTML path failed, falling back. Err={_e}")
        document_chunks, document_scopes = self._get_chunk_infos(
            document_plain_texts, self._max_tokens_per_chunk, self._chunk_overlap
        )
        # Step 1: Break query
        rewrite_query = self.get_task_output(AskDocTaskName.contextual_query_rewrite)
        final_query = rewrite_query if rewrite_query else self._input_model.query
        query1, query2 = self.break_query(final_query)

        # Step 2: Map stage
        map_answers = self.map_answers(query1, document_names, document_chunks, document_scopes)

        # Step 3: Generate answer
        return self.generate_answer(query2, document_names, map_answers)
