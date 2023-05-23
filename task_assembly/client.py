import io
import json
from apiclient import (
    APIClient,
    HeaderAuthentication,
    JsonResponseHandler,
    JsonRequestFormatter,
)
from urllib.parse import urlencode

from .handlers import create_consolidation_lambda
from .utils import display_iframe, TASK_DEFINITION_ARG_MAP


# TODO: Fix this simplified approach for caching the client
_client: "AssemblyClient" = None


def _arg_decorator(function):
    def inner(*args, **kwargs):
        inner.actual_kwargs = kwargs
        return function(*args, **kwargs)

    return inner


class Task(dict):

    def __getitem__(self, item):
        global _client
        if item not in self:
            if item == "Data":
                self[item] = _client.get_task_input(self.task_id).get("Data")
            elif item == "Responses":
                self[item] = _client.get_task_responses(self.task_id).get("Responses")
            elif item == "Result":
                self[item] = _client.get_task_result(self.task_id).get("Result")
        return super().__getitem__(item)

    @property
    def task_id(self):
        return self.get("TaskId")

    @property
    def extend_requested(self):
        return self.get("ExtendRequested")

    @property
    def state(self):
        return self.get("State")

    @property
    def errors(self):
        return self.get("Errors")

    @property
    def spend(self):
        return self.get("Spend")

    @property
    def spend_tuple(self):

        s = self.spend
        if s:
            return (
                s["TaskRewardCents"],
                s["TaskFeeCents"],
                s["TestRewardCents"],
                s["TestFeeCents"],
            )
        else:
            return (0, 0, 0, 0)

    @property
    def qualification_requirements(self):
        return self.get("QualificationRequirements")

    @property
    def definition(self):
        return self.get("Definition")

    @property
    def input(self):
        return self["Data"]

    @property
    def responses(self):
        return self["Responses"]

    @property
    def result(self):
        return self["Result"]


class AssemblyClient(APIClient):
    # ENDPOINT = 'https://pr60r7m9gi.execute-api.us-west-2.amazonaws.com/Prod'
    ENDPOINT = "https://api.taskassembly.com"

    def __init__(self, api_key):
        global _client
        super().__init__(
            authentication_method=HeaderAuthentication(
                token=api_key, parameter="x-api-key", scheme=None,
            ),
            response_handler=JsonResponseHandler,
            request_formatter=JsonRequestFormatter,
        )
        _client = self

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
            computed_result_handler=None,
            gold_answers=None,
            test_policy=None,
    ):
        url = self.ENDPOINT + "/taskDefinition/create"
        params = self._map_parameters(
            locals(),
            self.create_task_definition.actual_kwargs,
            TASK_DEFINITION_ARG_MAP
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
            computed_result_handler=None,
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
            sfn_token=None,
            qualification_requirements=None,
            use_computed_result=None,
            tag=None
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
                "sfn_token": "SFNToken",
                "qualification_requirements": "QualificationRequirements",
                "use_computed_result": "UseComputedResult",
                "tag": "Tag"
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
    def expire_task(self, task_id):
        url = self.ENDPOINT + "/task/expire"
        params = self._map_parameters(
            locals(), self.expire_task.actual_kwargs, {"task_id": "TaskId"}
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

    def get_task(self, task_id, include_detail=False, include_assignments=False) -> Task:
        if include_detail:
            return Task(self.get(
                self.ENDPOINT + "/task/detail/" + task_id,
                {"includeAssignments": include_assignments},
            ))
        else:
            return Task(self.get(self.ENDPOINT + "/task/" + task_id))

    def get_task_input(self, task_id):
        return self.get(self.ENDPOINT + "/task/input/" + task_id)

    def get_task_result(self, task_id):
        return self.get(self.ENDPOINT + "/task/result/" + task_id)

    def get_task_responses(self, task_id, include_excluded=False):
        result = self.get(self.ENDPOINT + "/task/responses/" + task_id)
        result["Responses"] = [
            {key: value for key, value in response.items() if key != "Excluded"}
            for response in result.get("Responses", [])
            if not response.get("Excluded")
        ]
        return result

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

    def list_batches(self, definition_id=None, max_results=None, start_key=None):
        params = {}
        if definition_id:
            params["TaskDefinitionId"] = definition_id
        if max_results:
            params["MaxResults"] = max_results
        if start_key:
            params["StartKey"] = start_key
        return self.get(f"{self.ENDPOINT}/batch/list", params)

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