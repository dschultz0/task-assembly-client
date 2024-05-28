from __future__ import annotations
from pprint import pprint
import mimetypes
from codecs import encode
from apiclient import (
    APIClient,
    JsonResponseHandler,
    JsonRequestFormatter,
)
from apiclient.request_formatters import NoOpRequestFormatter
from .utils import BLUEPRINT_DEFINITION_ARG_MAP, BATCH_DEFINITION_ARG_MAP
from dataclasses import dataclass, asdict

# TODO: Fix this simplified approach for caching the client
_client: "AssemblyClient" = None


def _arg_decorator(function):
    def inner(*args, **kwargs):
        inner.actual_kwargs = kwargs
        return function(*args, **kwargs)

    return inner


@dataclass
class CrowdConfig:
    service: str = None
    title: str = None
    description: str = None
    reward_cents: int = None
    assignment_duration_seconds: int = None
    lifetime_seconds: int = None
    default_assignments: int = None
    max_assignments: int = None
    auto_approval_delay: int = None
    keywords: str = None

    @staticmethod
    def configure_crowd(
        service=None,
        title=None,
        description=None,
        reward_cents=None,
        assignment_duration_seconds=None,
        lifetime_seconds=None,
        default_assignments=None,
        max_assignments=None,
        auto_approval_delay=None,
        keywords=None,
    ) -> CrowdConfig:
        crowd_config = CrowdConfig(
            service=service,
            title=title,
            description=description,
            reward_cents=reward_cents,
            assignment_duration_seconds=assignment_duration_seconds,
            lifetime_seconds=lifetime_seconds,
            default_assignments=default_assignments,
            max_assignments=max_assignments,
            auto_approval_delay=auto_approval_delay,
            keywords=keywords,
        )
        return crowd_config


class AssemblyClient(APIClient):
    # TODO get this url directly from lambda
    ENDPOINT = "https://qdlb4szcaoqni3l5ubyghxclym0obhoe.lambda-url.us-east-1.on.aws/"

    def __init__(self, api_key):
        global _client
        super().__init__(
            response_handler=JsonResponseHandler,
            request_formatter=JsonRequestFormatter,
        )
        _client = self

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
    def create_batch(
        self,
        definition=None,
        render_handler_arn=None,
        blueprint_id=None,
        file_name=None,
    ):
        url = self.ENDPOINT + "/batch"
        params = self._map_parameters(
            locals(), self.create_batch.actual_kwargs, BATCH_DEFINITION_ARG_MAP
        )

        post_response = self.post(url, data=params)
        return post_response

    @_arg_decorator
    def create_blueprint(
        self,
        name,
        task_template=None,
        crowd_config: CrowdConfig = None,
        render_handler_arn=None,
    ):
        url = self.ENDPOINT + "/blueprint"
        params = self._map_parameters(
            locals(), self.create_blueprint.actual_kwargs, BLUEPRINT_DEFINITION_ARG_MAP
        )
        params["crowd_config"] = asdict(crowd_config)
        post_response = self.post(url, data=params)
        return post_response

    @_arg_decorator
    def update_blueprint(
        self,
        name=None,
        task_template=None,
        crowd_config: CrowdConfig = None,
        render_handler_arn=None,
    ):
        url = self.ENDPOINT + "/blueprint"
        params = self._map_parameters(
            locals(),
            self.put_blueprint.actual_kwargs,
            BLUEPRINT_DEFINITION_ARG_MAP,
        )
        params["crowd_config"] = asdict(crowd_config)
        return self.put(url, data=params)

    def add_task_as_gold(self, task_id):
        url = self.ENDPOINT + "/taskDefinition/addGold"
        return self.post(url, data={"TaskId": task_id})

    @_arg_decorator
    def get_blueprints(self):
        url = self.ENDPOINT + "/blueprint"
        params = self._map_parameters(locals(), self.get_blueprints.actual_kwargs, {})
        return self.get(url, params)
