import uuid
import requests
import yaml
import time, os

from apiclient.authentication_methods import (
    HeaderAuthentication,
)
from apiclient import (
    APIClient,
    JsonResponseHandler,
    JsonRequestFormatter,
)
from .utils import (
    BLUEPRINT_DEFINITION_ARG_MAP,
    BLUEPRINT_ASSET_DEFINITION_ARG_MAP,
    TASK_DEFINITION_ARG_MAP,
    BATCH_DEFINITION_ARG_MAP,
    load_yaml,
)

# TODO: Fix this simplified approach for caching the client
_client: "AssemblyClient" = None

# TODO - CLIENT_ID has to come from a url - so we can change it
CLIENT_ID = "qtX9ORUYq3CVEFVTTlHuSqB8miXu5Nmj"
OAUTH_DOMAIN = "dev-task-assembly-1008.us.auth0.com"
REFRESH_TOKEN_LEWAY = 10


def _arg_decorator(function):
    def inner(*args, **kwargs):
        inner.actual_kwargs = kwargs
        return function(*args, **kwargs)

    return inner


"""
class Auth0Authentication(HeaderAuthentication):
    def __init__(
        self
    ):

        token_y = load_yaml("token.yaml")
        access_token =
        super().init(token=, parameter="Authorization", scheme="Bearer", extra=None)
"""


class AssemblyClient(APIClient):
    # points to lambda
    # a builder method to have the url would be appropriate
    ENDPOINT = "https://6tlw4klgmrtqkcumg5iwihe4sq0oztme.lambda-url.us-west-2.on.aws"

    def __init__(self, api_key):
        global _client
        super().__init__(
            response_handler=JsonResponseHandler,
            request_formatter=JsonRequestFormatter,
        )
        _client = self

    # TODO - needs refactoring - lots of returns
    def get_token(self):
        response = {}

        print("\nGetting/Refreshing token...\n")
        headers = {"content-type": "application/x-www-form-urlencoded"}

        #   Check if same token can be used or we can refresh
        #   Reference from here - https://github.com/mlcommons/medperf/blob/main/cli/medperf/comms/auth/auth0.py#L198
        if os.path.isfile("token.yaml"):
            token_yaml = load_yaml("token.yaml")
            absolute_expiration = (
                token_yaml["token_issued_at"] + token_yaml["expires_in"]
            )
            refresh_possible_expiration = absolute_expiration - REFRESH_TOKEN_LEWAY

            current_time = time.time()

            if current_time < refresh_possible_expiration:
                print("Token unexpired - reusing")
                response = {"token": token_yaml["access_token"]}

                return response

            if current_time > absolute_expiration:
                print("Token expired - please login again")
                os.remove("token.yaml")

                response = {"error": "token expired"}

                return response

            print("using refresh token")
            svc_response = requests.post(
                f"https://{OAUTH_DOMAIN}/oauth/token",
                headers=headers,
                data={
                    "client_id": ("%s" % CLIENT_ID),
                    "grant_type": "refresh_token",
                    "refresh_token": token_yaml["refresh_token"],
                },
            )
        else:
            login_yaml = load_yaml("login.yaml")

            print("requesting new token")
            svc_response = requests.post(
                f"https://{OAUTH_DOMAIN}/oauth/token",
                headers=headers,
                data={
                    "client_id": ("%s" % CLIENT_ID),
                    "device_code": ("%s" % login_yaml["device_code"]),
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                },
            )

        json_response = svc_response.json()
        json_response["token_issued_at"] = time.time()

        # TODO - Add better error messages from dave
        if "error" in json_response:
            response["error"] = json_response["error"]

            if json_response["error"] == "authorization_pending":
                print(
                    f"Error during get_token - Auth Pending - {json_response['error_description']}"
                )
            elif json_response["error"] == "slow_down":
                print(
                    f"Error during get_token - Too many requests - {json_response['error_description']}"
                )
            elif json_response["error"] == "expired_token":
                print(
                    f"Error during get_token - Expired Token - {json_response['error_description']}"
                )
            elif json_response["error"] == "access_denied":
                print(
                    f"Error during get_token - Access Denied - {json_response['error_description']}"
                )
            elif json_response["error"] == "invalid_grant":
                print(
                    f"Invalid or expired device code - use cli with login to generate device code\n"
                )
            else:
                print(f"Error during get_token - {json_response}")
        else:
            with open("token.yaml", "w") as fp:
                yaml.dump(json_response, fp)
            response = {"token": json_response["access_token"]}

        return response

    def do_login(self):
        headers = {"content-type": "application/x-www-form-urlencoded"}
        response = requests.post(
            f"https://{OAUTH_DOMAIN}/oauth/device/code",
            headers=headers,
            data={
                "client_id": ("%s" % CLIENT_ID),
                "audience": "https://task-assembly-backend",
                "scope": "offline_access",
            },
        )
        json_response = response.json()
        if "error" in json_response:
            print(f"Error during login - {json_response['error_description']}")
        else:
            print(f"\nYour device code - {json_response['device_code']}")
            print(
                f"\n\nLogin through your webbrowser with this link: {json_response['verification_uri_complete']}\n"
            )
            with open("login.yaml", "w") as fp:
                yaml.dump({"device_code": json_response["device_code"]}, fp)

    @staticmethod
    def _map_parameters(parameters, actual_kwargs, key_map):
        result = {}
        for k, i in key_map.items():
            #   handles a nested dictionary
            if isinstance(i, dict):
                result[k] = {}
                for kk, ii in i.items():
                    if kk in parameters and (
                        parameters[kk] is not None or kk in actual_kwargs
                    ):
                        result[k][ii] = parameters[kk]
            else:
                #   handles a flat value
                if k in parameters and (
                    parameters[k] is not None or k in actual_kwargs
                ):
                    result[i] = parameters[k]
        return result

    @_arg_decorator
    def create_batch(self, blueprint_id, account_id):
        url = self.ENDPOINT + "/batch"
        params = self._map_parameters(
            locals(), self.create_batch.actual_kwargs, BATCH_DEFINITION_ARG_MAP
        )
        return self.post(url, data=params)

    @_arg_decorator
    def get_batches(self):
        url = self.ENDPOINT + "/batch"
        params = self._map_parameters(locals(), self.get_batches.actual_kwargs, {})
        return self.get(url, params)

    @_arg_decorator
    def create_task(self, blueprint_id, team_id):
        url = self.ENDPOINT + "/task"
        params = self._map_parameters(
            locals(), self.create_task.actual_kwargs, TASK_DEFINITION_ARG_MAP
        )
        return self.post(url, data=params)

    @_arg_decorator
    def get_tasks(self):
        url = self.ENDPOINT + "/task"
        params = self._map_parameters(locals(), self.get_tasks.actual_kwargs, {})
        return self.get(url, params)

    @_arg_decorator
    def create_blueprint(
        self,
        name,
        state=None,
        title=None,
        description=None,
        keywords=None,
        assignment_duration_seconds=None,
        lifetime_seconds=None,
        default_assignments=None,
        max_assignments=None,
        default_team_id=None,
        template_uri=None,
        instructions_uri=None,
        result_template_uri=None,
        response_template_uri=None,
    ):
        url = self.ENDPOINT + "/blueprint"
        params = self._map_parameters(
            locals(), self.create_blueprint.actual_kwargs, BLUEPRINT_DEFINITION_ARG_MAP
        )
        params["accountId"] = str(uuid.uuid4())
        return self.post(url, data=params)

    @_arg_decorator
    def get_blueprint(self, id):
        url = self.ENDPOINT + f"/blueprint/{id}"
        params = self._map_parameters(locals(), self.get_blueprint.actual_kwargs, {})
        headers = {"accept": "application/json"}

        return self.get(url, params, headers)

    @_arg_decorator
    def get_blueprints(self):
        url = f"{self.ENDPOINT}/blueprint"
        response = self.get_token()
        headers = {
            "accept": "application/json",
            "Authorization": f'Bearer {response["token"]}',
        }
        params = self._map_parameters(locals(), self.get_blueprints.actual_kwargs, {})
        return self.get(endpoint=url, params=params, headers=headers)

    @_arg_decorator
    def update_blueprint(
        self,
        name,
        state=None,
        title=None,
        description=None,
        keywords=None,
        assignment_duration_seconds=None,
        lifetime_seconds=None,
        default_assignments=None,
        max_assignments=None,
        default_team_id=None,
        template_uri=None,
        instructions_uri=None,
        result_template_uri=None,
        response_template_uri=None,
        account_id=None,
        blueprint_id=None,
    ):
        url = self.ENDPOINT + f"/blueprint/{blueprint_id}"
        params = self._map_parameters(
            locals(),
            self.update_blueprint.actual_kwargs,
            BLUEPRINT_DEFINITION_ARG_MAP,
        )
        print(params)
        return self.put(url, data=params)

    @_arg_decorator
    def create_blueprint_asset(self, blueprint_id, name, kb=0):
        url = self.ENDPOINT + "/blueprint_asset"
        params = self._map_parameters(
            locals(),
            self.create_blueprint_asset.actual_kwargs,
            BLUEPRINT_ASSET_DEFINITION_ARG_MAP,
        )
        return self.post(url, data=params)


"""
import io
import json
from apiclient import (
    APIClient,
    HeaderAuthentication,
    JsonResponseHandler,
    JsonRequestFormatter,
)
from urllib.parse import urlencode
import typing
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
        #Extends the default timeout to 30 seconds for longer running actions
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
            tags: typing.Dict[str, str]=None
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
                "tags": "Tags"
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
            qualification_requirements=None
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
                "qualification_requirements": "QualificationRequirements"
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
    def redrive_tasks(self, definition_id=None, tag_name=None, tag_value=None,
                      start_datetime=None, end_datetime=None, extend=False):
        url = self.ENDPOINT + "/definition/redrive"
        params = self._map_parameters(
            locals(),
            self.redrive_tasks.actual_kwargs,
            {
                "definition_id": "DefinitionId",
                "tag_name": "TagName",
                "tag_value": "TagValue",
                "start_datetime": "StartDatetime",
                "end_datetime": "EndDatetime",
                "extend": "Extend"
            },
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
        start_key = "start"
        while start_key:
            params = {}
            if start_key != "start":
                params["StartKey"] = start_key
            response = self.get(f"{self.ENDPOINT}/taskDefinition/{definition_id}/workers", params)
            start_key = response.get("NextKey")
            for w in response.get("Workers", []):
                yield w

    def list_tasks(self, task_definition_id=None, tag=None, tag_value=None, batch_id=None):
        ops = [task_definition_id, tag, batch_id]
        params = {}
        if task_definition_id:
            params["TaskDefinitionId"] = task_definition_id
        if tag and tag_value:
            params["Tag"] = tag
            params["TagValue"] = tag_value
        if batch_id:
            params["BatchId"] = batch_id
        start_key = "start"
        while start_key:
            if start_key != "start":
                params["StartKey"] = start_key
            response = self.get(f"{self.ENDPOINT}/tasks", params)
            start_key = response.get("NextKey")
            for t in response.get("Tasks", []):
                yield t

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
    def list_assignments(
            self,
            definition_id,
            worker_id=None,
            test_index: int = None,
            max_results: int = None,
            start_key=None,
            include_detail: bool = None,
            tests_only: bool = None
    ):
        params = self._map_parameters(
            locals(),
            self.list_assignments.actual_kwargs,
            {
                "definition_id": "TaskDefinitionId",
                "worker_id": "WorkerId",
                "test_index": "TestIndex",
                "max_results": "MaxResults",
                "start_key": "StartKey",
                "include_detail": "IncludeTaskDetail",
                "tests_only": "TestsOnly",
            },
        )
        return self.get(f"{self.ENDPOINT}/assignments", params)

    def iter_assignments(
            self,
            definition_id,
            worker_id=None,
            test_index: int = None,
            max_results: int = None,
            include_detail: bool = None,
            tests_only: bool = None,
    ):
        start_key = None
        complete = False
        while not complete:
            response = self.list_assignments(
                definition_id, worker_id, test_index, max_results, start_key, include_detail, tests_only
            )
            start_key = response.get("NextKey")
            if not start_key:
                complete = True
            for assignment in response.get("Assignments"):
                yield assignment

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
    def reset_worker_score(
            self, worker_id, definition_id, count=None
    ):
        url = self.ENDPOINT + "/worker/definition/resetscore"
        params = self._map_parameters(
            locals(),
            self.reset_worker_score.actual_kwargs,
            {
                "worker_id": "WorkerId",
                "definition_id": "DefinitionId",
                "count": "Count",
            },
        )
        self.post(url, data=params)

    @_arg_decorator
    def redrive_scoring(self, definition_id, redrive_submissions=False):
        url = self.ENDPOINT + "/taskDefinition/redriveScoring"
        params = self._map_parameters(
            locals(),
            self.redrive_scoring.actual_kwargs,
            {"definition_id": "DefinitionId", "redrive_submissions": "RedriveSubmissions"},
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
"""
