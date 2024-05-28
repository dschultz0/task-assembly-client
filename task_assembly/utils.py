import uuid
import warnings
import mimetypes
from codecs import encode
from collections.abc import MutableMapping
from html import escape

BATCH_DEFINITION_ARG_MAP = {
    "definition": "definition",
    "blueprint_id": "blueprintId",
    "render_handler_arn": "renderHandlerArn",
}

BLUEPRINT_DEFINITION_ARG_MAP = {
    "name": "name",
    "task_template": "task_template",
    "render_handler_arn": "render_handler_arn",
}

TASK_DEFINITION_ARG_MAP = {
    "definition_id": "DefinitionId",
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
    "load_handler": "LoadHandler",
    "render_handler": "RenderHandler",
    "submission_handlers": "SubmissionHandlers",
    "consolidation_handlers": "ConsolidationHandlers",
    "scoring_handler": "ScoringHandler",
    "callback_handlers": "CallbackHandlers",
    "computed_result_handler": "ComputedResultHandler",
    "handler_code": "HandlerCode",
    "gold_answers": "GoldAnswers",
    "test_policy": "TestPolicy",
    "result_layout": "ResultLayout",
}
REV_TASK_DEFINITION_ARG_MAP = {v: k for k, v in TASK_DEFINITION_ARG_MAP.items()}


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

    except ImportError:
        raise Exception(
            "Display operations are not supported outside of the IPython environment"
        )


def display_html(html):
    try:
        from IPython.display import display, HTML

        display(HTML(html))
    except ImportError:
        raise Exception(
            "Display operations are not supported outside of the IPython environment"
        )


def display_link(url, prefix):
    display_html(
        '<p><strong>{0}:</strong> <a href="{1}" target="_blank">{1}</a></p>'.format(
            prefix, url
        )
    )


def flatten_dict(
    d: MutableMapping, parent_key: str = "", sep: str = "."
) -> MutableMapping:
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


REV_BLUEPRINT_DEFINITION_ARG_MAP = {
    v: k for k, v in flatten_dict(BLUEPRINT_DEFINITION_ARG_MAP).items()
}


def upload_file(post_response, file_name):
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
            "Content-Disposition: form-data; name=file; filename={0}".format(file_name)
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
    return {"url": file_e, "body": file_f, "headers": headers}
    # self.set_request_formatter(NoOpRequestFormatter)
    # print(self.post(file_e, data=file_f, headers=headers))
