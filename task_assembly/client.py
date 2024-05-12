import mimetypes
from codecs import encode
from apiclient import (
    APIClient,
    JsonResponseHandler,
    JsonRequestFormatter,
)
from apiclient.request_formatters import NoOpRequestFormatter
from .utils import BLUEPRINT_DEFINITION_ARG_MAP, BATCH_DEFINITION_ARG_MAP

# TODO: Fix this simplified approach for caching the client
_client: "AssemblyClient" = None


def _arg_decorator(function):
    def inner(*args, **kwargs):
        inner.actual_kwargs = kwargs
        return function(*args, **kwargs)

    return inner


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

        dataList = []
        boundary = "wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T"

        dataList.append(encode("--" + boundary))
        dataList.append(encode("Content-Disposition: form-data; name=key;"))
        dataList.append(encode("Content-Type: {}".format("text/plain")))
        dataList.append(encode(""))
        dataList.append(encode(post_response["url"]["fields"]["key"]))

        dataList.append(encode("--" + boundary))
        dataList.append(encode("Content-Disposition: form-data; name=AWSAccessKeyId;"))
        dataList.append(encode("Content-Type: {}".format("text/plain")))
        dataList.append(encode(""))
        dataList.append(encode(post_response["url"]["fields"]["AWSAccessKeyId"]))

        dataList.append(encode("--" + boundary))
        dataList.append(
            encode("Content-Disposition: form-data; name=x-amz-security-token;")
        )
        dataList.append(encode("Content-Type: {}".format("text/plain")))
        dataList.append(encode(""))
        dataList.append(encode(post_response["url"]["fields"]["x-amz-security-token"]))

        dataList.append(encode("--" + boundary))
        dataList.append(encode("Content-Disposition: form-data; name=policy;"))
        dataList.append(encode("Content-Type: {}".format("text/plain")))
        dataList.append(encode(""))
        dataList.append(encode(post_response["url"]["fields"]["policy"]))

        dataList.append(encode("--" + boundary))
        dataList.append(encode("Content-Disposition: form-data; name=signature;"))
        dataList.append(encode("Content-Type: {}".format("text/plain")))
        dataList.append(encode(""))
        dataList.append(encode(post_response["url"]["fields"]["signature"]))

        dataList.append(encode("--" + boundary))
        dataList.append(
            encode(
                "Content-Disposition: form-data; name=file; filename={0}".format(
                    file_name
                )
            )
        )
        fileType = mimetypes.guess_type(file_name)[0] or "application/octet-stream"
        dataList.append(encode("Content-Type: {}".format(fileType)))
        dataList.append(encode(""))

        with open(file_name, "rb") as f:
            dataList.append(f.read())
        dataList.append(encode("--" + boundary + "--"))
        dataList.append(encode(""))
        body = b"\r\n".join(dataList)

        headers = {"Content-type": "multipart/form-data; boundary={}".format(boundary)}

        #   Upload file
        file_e = post_response["url"]["url"]
        file_f = body
        self.set_request_formatter(NoOpRequestFormatter)
        print(self.post(file_e, data=file_f, headers=headers))

        return post_response

    @_arg_decorator
    def create_blueprint(
        self,
        name,
        task_template=None,
        crowdconfig_service=None,
        crowdconfig_title=None,
        crowdconfig_description=None,
        crowdconfig_reward_cents=None,
        crowdconfig_assignment_duration_seconds=None,
        crowdconfig_lifetime_seconds=None,
        crowdconfig_default_assignments=None,
        crowdconfig_max_assignments=None,
        crowdconfig_auto_approval_delay=None,
        crowdconfig_keywords=None,
        render_handler_arn=None,
    ):
        url = self.ENDPOINT + "/blueprint"
        params = self._map_parameters(
            locals(), self.create_blueprint.actual_kwargs, BLUEPRINT_DEFINITION_ARG_MAP
        )

        post_response = self.post(url, data=params)
        return post_response

    @_arg_decorator
    def update_blueprint(
        self,
        name=None,
        task_template=None,
        crowdconfig_service=None,
        crowdconfig_title=None,
        crowdconfig_description=None,
        crowdconfig_reward_cents=None,
        crowdconfig_assignment_duration_seconds=None,
        crowdconfig_lifetime_seconds=None,
        crowdconfig_default_assignments=None,
        crowdconfig_max_assignments=None,
        crowdconfig_auto_approval_delay=None,
        crowdconfig_keywords=None,
        render_handler_arn=None,
    ):
        url = self.ENDPOINT + "/blueprint"
        params = self._map_parameters(
            locals(),
            self.put_blueprint.actual_kwargs,
            BLUEPRINT_DEFINITION_ARG_MAP,
        )
        return self.put(url, data=params)

    def add_task_as_gold(self, task_id):
        url = self.ENDPOINT + "/taskDefinition/addGold"
        return self.post(url, data={"TaskId": task_id})

    @_arg_decorator
    def get_blueprints(self):
        url = self.ENDPOINT + "/blueprint"
        params = self._map_parameters(locals(), self.get_blueprints.actual_kwargs, {})
        return self.get(url, params)
