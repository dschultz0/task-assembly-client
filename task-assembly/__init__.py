from apiclient import (
    APIClient,
    HeaderAuthentication,
    JsonResponseHandler,
    JsonRequestFormatter,
)
import io
import uuid
import warnings
from html import escape
from .handlers import create_consolidation_lambda
from urllib.parse import urlencode
import json


def _arg_decorator(function):
    def inner(*args, **kwargs):
        inner.actual_kwargs = kwargs
        return function(*args, **kwargs)

    return inner


TASK_DEFINITION_ARG_MAP = {
    "definition_id": "DefinitionId",
    "task_type_id": "TaskType",
    "template": "Template",
    "title": "Title",
    "description": "Description",
    "reward_cents": "RewardCents",
    "lifetime": "Lifetime",
    "assignment_duration": "AssignmentDuration",
    "default_assignments": "DefaultAssignments",
    "max_assignments": "MaxAssignments",
    "auto_approval_delay": "AutoApprovalDelay",
    "keywords": "Keywords",
    "qualification_requirements": "QualificationRequirements",
    "load_handler": "LoadHandler",
    "render_handler": "RenderHandler",
    "submission_handlers": "SubmissionHandlers",
    "consolidation_handlers": "ConsolidationHandlers",
    "scoring_handler": "ScoringHandler",
    "callback_handlers": "CallbackHandlers",
    "handler_code": "HandlerCode",
    "gold_answers": "GoldAnswers",
    "test_policy": "TestPolicy",
    "result_layout": "ResultLayout"
}
REV_TASK_DEFINITION_ARG_MAP = {v: k for k, v in TASK_DEFINITION_ARG_MAP.items()}


class AssemblyClient(APIClient):
    # ENDPOINT = 'https://pr60r7m9gi.execute-api.us-west-2.amazonaws.com/Prod'
    ENDPOINT = "https://api.taskassembly.com"

    def __init__(self, api_key):
        super().__init__(
            authentication_method=HeaderAuthentication(
                token=api_key, parameter="x-api-key", scheme=None,
            ),
            response_handler=JsonResponseHandler,
            request_formatter=JsonRequestFormatter,
        )

    def get_request_timeout(self) -> float:
        """Extends the default timeout to 30 seconds for longer running actions"""
        return 30.0

    @staticmethod
    def _map_parameters(parameters, actual_kwargs, key_map):
        result = {}
        for k, i in key_map.items():
            if k in parameters and (parameters[k] is not None or k in actual_kwargs):
                result[i] = parameters[k]
        return result

    def get_user(self):
        url = self.ENDPOINT + "/user"
        return self.get(url)

    def validate(self):
        url = self.ENDPOINT + "/validate"
        return self.get(url)

    @_arg_decorator
    def create_task_definition(
            self,
            name,
            task_type_id,
            template=None,
            title=None,
            description=None,
            reward_cents=None,
            lifetime=None,
            assignment_duration=None,
            default_assignments=None,
            max_assignments=None,
            auto_approval_delay=None,
            keywords=None,
            qualification_requirements=None,
            render_handler=None,
            submission_handlers=None,
            consolidation_handlers=None,
            callback_handlers=None,
            scoring_handler=None,
            gold_answers=None,
            test_policy=None,
    ):
        url = self.ENDPOINT + "/taskDefinition/create"
        params = self._map_parameters(
            locals(),
            self.create_task_definition.actual_kwargs,
            {
                "name": "Name",
                "task_type_id": "TaskType",
                "template": "Template",
                "title": "Title",
                "description": "Description",
                "reward_cents": "RewardCents",
                "lifetime": "Lifetime",
                "assignment_duration": "AssignmentDuration",
                "default_assignments": "DefaultAssignments",
                "max_assignments": "MaxAssignments",
                "auto_approval_delay": "AutoApprovalDelay",
                "keywords": "Keywords",
                "qualification_requirements": "QualificationRequirements",
                "render_handler": "RenderHandler",
                "submission_handlers": "SubmissionHandlers",
                "consolidation_handlers": "ConsolidationHandlers",
                "scoring_handler": "ScoringHandler",
                "callback_handlers": "CallbackHandlers",
                "gold_answers": "GoldAnswers",
                "test_policy": "TestPolicy",
            },
        )
        if isinstance(params.get("Template"), io.IOBase):
            params["Template"] = params["Template"].read()
        return self.post(url, data=params)

    @_arg_decorator
    def update_task_definition(
            self,
            definition_id,
            task_type_id=None,
            template=None,
            title=None,
            description=None,
            reward_cents=None,
            lifetime=None,
            assignment_duration=None,
            default_assignments=None,
            max_assignments=None,
            auto_approval_delay=None,
            keywords=None,
            qualification_requirements=None,
            load_handler=None,
            render_handler=None,
            submission_handlers=None,
            consolidation_handlers=None,
            callback_handlers=None,
            scoring_handler=None,
            gold_answers=None,
            test_policy=None,
            result_layout=None,
            handler_code=None
    ):
        url = self.ENDPOINT + "/taskDefinition/update"
        params = self._map_parameters(
            locals(),
            self.update_task_definition.actual_kwargs,
            TASK_DEFINITION_ARG_MAP,
        )
        if isinstance(params.get("Template"), io.IOBase):
            params["Template"] = params["Template"].read()
        return self.post(url, data=params)

    def add_task_as_gold(self, task_id):
        url = self.ENDPOINT + "/taskDefinition/addGold"
        return self.post(url, data={"TaskId": task_id})

    @_arg_decorator
    def get_task_definition(self, definition_id):
        url = self.ENDPOINT + "/taskDefinition"
        params = self._map_parameters(
            locals(),
            self.get_task_definition.actual_kwargs,
            {"definition_id": "DefinitionId"},
        )
        return self.get(url, params)

    @_arg_decorator
    def create_task(
            self,
            definition_id,
            data,
            sandbox=False,
            default_assignments=None,
            max_assignments=None,
    ):
        url = self.ENDPOINT + "/task/create"
        params = self._map_parameters(
            locals(),
            self.create_task.actual_kwargs,
            {
                "definition_id": "DefinitionId",
                "data": "Data",
                "sandbox": "Sandbox",
                "default_assignments": "DefaultAssignments",
                "max_assignments": "MaxAssignments",
            },
        )
        return self.post(url, data=params)["TaskId"]

    @_arg_decorator
    def render_task(self, definition_id, data):
        url = self.ENDPOINT + "/task/render"
        params = self._map_parameters(
            locals(),
            self.render_task.actual_kwargs,
            {"definition_id": "DefinitionId", "data": "Data"},
        )
        return self.post(url, data=params)["html"]

    @staticmethod
    def create_test_render_url(
            definition_id,
            input_data,
            assignment_id="ASSIGNMENTID",
            worker_id="WORKERID",
            hit_id="HITID",
            submit_to="https://www.mturk.com",
    ):
        base_url = "https://task.taskassembly.com/test?"
        return base_url + urlencode(
            {
                "definitionId": definition_id,
                "assignmentId": assignment_id,
                "workerId": worker_id,
                "hitId": hit_id,
                "turkSubmitTo": submit_to,
                "input": json.dumps(input_data),
            }
        )

    @_arg_decorator
    def submit_batch(
            self,
            definition_id,
            name,
            input_uri,
            output_uri,
            sandbox=False,
            default_assignments=None,
            max_assignments=None,
    ):
        url = self.ENDPOINT + "/batch/submit"
        params = self._map_parameters(
            locals(),
            self.submit_batch.actual_kwargs,
            {
                "definition_id": "DefinitionId",
                "name": "Name",
                "input_uri": "InputUri",
                "output_uri": "OutputUri",
                "sandbox": "Sandbox",
                "default_assignments": "DefaultAssignments",
                "max_assignments": "MaxAssignments",
            },
        )
        return self.post(url, data=params)["BatchId"]

    @_arg_decorator
    def expire_batch(self, batch_id):
        url = self.ENDPOINT + "/batch/expire"
        params = self._map_parameters(
            locals(), self.expire_batch.actual_kwargs, {"batch_id": "BatchId"}
        )
        return self.post(url, data=params)

    @_arg_decorator
    def redrive_task(self, task_id, extend=False):
        url = self.ENDPOINT + "/task/redrive"
        params = self._map_parameters(
            locals(),
            self.redrive_task.actual_kwargs,
            {"task_id": "TaskId", "extend": "Extend"},
        )
        return self.post(url, data=params)

    @_arg_decorator
    def redrive_batch(self, batch_id, extend=False):
        url = self.ENDPOINT + "/batch/redrive"
        params = self._map_parameters(
            locals(),
            self.redrive_batch.actual_kwargs,
            {"batch_id": "BatchId", "extend": "Extend"},
        )
        return self.post(url, data=params)

    @_arg_decorator
    def resolve_batch(self, batch_id, extend=False):
        url = self.ENDPOINT + "/batch/resolve"
        params = self._map_parameters(
            locals(), self.resolve_batch.actual_kwargs, {"batch_id": "BatchId"}
        )
        return self.post(url, data=params)

    def get_task(self, task_id, include_assignments=False):
        return self.get(
            self.ENDPOINT + "/task/" + task_id,
            {"includeAssignments": include_assignments},
        )

    def get_task_responses(self, task_id, include_excluded=False):
        task = self.get_task(task_id, False)
        return [
            {key: value for key, value in response.items() if key != "Excluded"}
            for response in task.get("Responses", [])
            if not response.get("Excluded")
        ]

    @_arg_decorator
    def create_task_type(self, name):
        url = self.ENDPOINT + "/taskType/create"
        params = self._map_parameters(
            locals(), self.create_task_type.actual_kwargs, {"name": "Name"}
        )
        return self.post(url, data=params)["TypeId"]

    @_arg_decorator
    def create_dataset(self, name, s3_uri):
        url = self.ENDPOINT + "/dataset/create"
        params = self._map_parameters(
            locals(),
            self.create_dataset.actual_kwargs,
            {"name": "Name", "s3_uri": "S3Uri"},
        )
        return self.post(url, data=params)["DatasetId"]

    def get_batch(self, batch_id):
        return self.get(self.ENDPOINT + "/batch/" + batch_id)

    def get_worker_stats(self, worker_id):
        return self.get(self.ENDPOINT + "/worker/stats/" + worker_id)

    def list_workers(self, definition_id):
        return self.get(f"{self.ENDPOINT}/taskDefinition/{definition_id}/workers")

    @_arg_decorator
    def exclude_worker(self, worker_id, reverse=False):
        url = self.ENDPOINT + "/worker/exclude"
        params = self._map_parameters(
            locals(),
            self.exclude_worker.actual_kwargs,
            {"worker_id": "WorkerId", "reverse": "Reverse"},
        )
        self.post(url, data=params)

    @_arg_decorator
    def reset_worker_scores(
            self, worker_id, definition_id=None, task_type_id=None, batch_id=None
    ):
        url = self.ENDPOINT + "/worker/resetscores"
        params = self._map_parameters(
            locals(),
            self.reset_worker_scores.actual_kwargs,
            {
                "worker_id": "WorkerId",
                "definition_id": "DefinitionId",
                "task_type_id": "TaskTypeId",
                "batch_id": "BatchId",
            },
        )
        self.post(url, data=params)

    @_arg_decorator
    def redrive_scoring(self, definition_id):
        url = self.ENDPOINT + "/taskDefinition/redriveScoring"
        params = self._map_parameters(
            locals(),
            self.redrive_scoring.actual_kwargs,
            {"definition_id": "DefinitionId"},
        )
        self.post(url, data=params)

    def display_task_preview(
            self, definition_id, data, width=None, height=600, link_only=False
    ):
        display_iframe(
            html=self.render_task(definition_id, data), width=width, height=height
        )

    def create_consolidation_lambda(
            self,
            handler,
            name,
            role,
            imports=None,
            functions=None,
            files=None,
            layers=None,
            timeout=None,
    ):
        return create_consolidation_lambda(
            handler, name, role, imports, functions, files, layers, timeout
        )


def display_iframe(url=None, html=None, width=None, height=600, frame_border=5):
    frame_id = "f" + str(uuid.uuid4())[:8]
    w = 900 if width is None else width
    try:
        from IPython.display import display, HTML

        if html:
            _html = '<iframe id="{}" width="{}" height="{}" srcdoc="{}" frameborder="{}" allowfullscreen></iframe>'.format(
                frame_id, w, height, escape(html), frame_border
            )
        else:
            _html = '<iframe id="{}" width="{}" height="{}" src="{}" frameborder="{}" allowfullscreen></iframe>'.format(
                frame_id, w, height, url, frame_border
            )

        if width is None:
            _html += """<script language="JavaScript">
                document.getElementById("{0}").width = document.getElementById("{0}").parentElement.clientWidth - 25
                window.addEventListener("resize", function(){{
                    document.getElementById("{0}").width = document.getElementById("{0}").parentElement.clientWidth - 25
                }})</script>""".format(
                frame_id
            )

        # this will throw an irrelevant warning about considering the iframe element
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            display(HTML(_html))

    except ImportError as e:
        raise Exception(
            "Display operations are not supported outside of the IPython environment"
        )


def display_html(html):
    try:
        from IPython.display import display, HTML

        display(HTML(html))
    except ImportError as e:
        raise Exception(
            "Display operations are not supported outside of the IPython environment"
        )


def display_link(url, prefix):
    display_html(
        '<p><strong>{0}:</strong> <a href="{1}" target="_blank">{1}</a></p>'.format(
            prefix, url
        )
    )
