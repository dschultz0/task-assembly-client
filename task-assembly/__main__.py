import json
import os.path
import posixpath

import larry as lry
import argparse
import csv

from botocore.exceptions import ClientError

from . import AssemblyClient, REV_TASK_DEFINITION_ARG_MAP


parser = argparse.ArgumentParser("Task Assembly CLI")
subparsers = parser.add_subparsers(dest="command", required=True)

ctt_parser = subparsers.add_parser("create_task_type")
ctt_parser.add_argument("name")

ctd_parser = subparsers.add_parser("create_task_definition")
ctd_parser.add_argument("name")
ctd_parser.add_argument("task_type_id")

utd_parser = subparsers.add_parser("update_task_definition")
utd_parser.add_argument("--definition_file", default="definition.json")

gtd_parser = subparsers.add_parser("get_task_definition")
gtd_parser.add_argument("id")

ct_parser = subparsers.add_parser("create_task")
ct_parser.add_argument("values", type=str, nargs="*")
ct_parser.add_argument("--assignments", type=int)
ct_parser.add_argument("--sandbox", action="store_true")
ct_parser.add_argument("--definition_file", default="definition.json")

gt_parser = subparsers.add_parser("get_task")
gt_parser.add_argument("task_id")
gt_parser.add_argument("--include_assignments", action="store_true")

rt_parser = subparsers.add_parser("redrive_task")
rt_parser.add_argument("task_id")
rt_parser.add_argument("--extend", action="store_true")

sb_parser = subparsers.add_parser("submit_batch")
sb_parser.add_argument("--definition_file", default="definition.json")
sb_parser.add_argument("--sandbox", action="store_true")
sb_parser.add_argument("--assignments", type=int)
sb_parser.add_argument("name")
sb_parser.add_argument("input_file")
sb_parser.add_argument("s3_uri_prefix")

stb_parser = subparsers.add_parser("stop_batch")
stb_parser.add_argument("batch_id")

gbs_parser = subparsers.add_parser("get_batch_status")
gbs_parser.add_argument("batch_id")

gbr_parser = subparsers.add_parser("get_batch_results")
gbr_parser.add_argument("batch_id")
gbr_parser.add_argument("output_file")

c_parser = subparsers.add_parser("configure")
c_parser.add_argument("--key")
c_parser.add_argument("--validate", action="store_true")

lw_parser = subparsers.add_parser("list_workers")
lw_parser.add_argument("--definition_file", default="definition.json")
lw_parser.add_argument("output_file")

# lb_parser = subparsers.add_parser("list_batches")
# lb_parser.add_argument("--definition_file", default="definition.json")
# lb_parser.add_argument("--output_file")

args = parser.parse_args()

if args.command == "configure" and args.key:
    with open("api-key.txt", "w") as fp:
        fp.write(args.key)
    if not args.validate:
        exit(0)

if not os.path.exists("api-key.txt"):
    print("No api-key.txt file found. Please run the configure command first.")
    exit(1)

with open("api-key.txt") as fp:
    api_key = fp.read().strip()
    client = AssemblyClient(api_key)


def read_definition(file_name):
    with open(file_name) as ffp:
        definition_ = json.load(ffp)
    return definition_


if args.command == "configure" and args.validate:
    response = client.validate()
    print(f"Organization: {response['Organization']['Name']}")
    print(f"AWS Account: {response['AWSAccountId']}")
    print(f"MTurk connection status: {'SUCCESS' if response.get('MTurk') else 'FAILED'}")
    print(f"MTurk Sandbox connection status: {'SUCCESS' if response.get('MTurkSandbox') else 'FAILED'}")
elif args.command == "create_task_type":
    task_type_id = client.create_task_type(args.name)
    print(f"Created task type ID: {task_type_id}")
elif args.command == "create_task_definition":
    definition = client.create_task_definition(args.name, args.task_type_id)
    definition["TaskType"] = args.task_type_id
    with open("definition.json", "w") as fp:
        json.dump(definition, fp, indent=4)
    print(f"Created task definition {definition['DefinitionId']} in definition.json")
elif args.command == "update_task_definition":
    definition = read_definition(args.definition_file)
    if "TemplateFile" in definition:
        with open(definition.pop("TemplateFile")) as fp:
            definition["Template"] = fp.read()
    if "GoldAnswersFile" in definition:
        with open(definition.pop("GoldAnswersFile")) as fp:
            definition["GoldAnswers"] = json.load(fp)
    if "HandlerFile" in definition:
        with open(definition.pop("HandlerFile")) as fp:
            definition["HandlerCode"] = fp.read()
    params = {REV_TASK_DEFINITION_ARG_MAP[k]: v for k, v in definition.items()}
    client.update_task_definition(**params)
    print(f"Updated task definition {definition['DefinitionId']}")
elif args.command == "get_task_definition":
    definition = client.get_task_definition(args.id)
    if "GoldAnswers" in definition:
        definition.pop("GoldAnswers")
    print(json.dumps(definition, indent=4))
elif args.command == "create_task":
    definition = read_definition(args.definition_file)
    params = {
        "definition_id": definition["DefinitionId"]
    }
    if args.assignments:
        params["default_assignments"] = args.assignments
    if args.sandbox:
        params["sandbox"] = True
    if isinstance(args.values, list):
        vals = [v.split("=") for v in args.values]
        params["data"] = {v[0]: v[1] for v in vals}
    else:
        vals = args.values.split("=")
        params["data"] = {vals[0]: vals[1]}
    task_id = client.create_task(**params)
    print(f"Task created: {task_id}")
elif args.command == "get_task":
    response = client.get_task(args.task_id)
    print(json.dumps(response, indent=4))
elif args.command == "redrive_task":
    response = client.redrive_task(args.task_id, extend=args.extend)
    print(json.dumps(response, indent=4))
elif args.command == "submit_batch":
    definition = read_definition(args.definition_file)
    name = args.name.replace(" ", "_")
    with open(args.input_file) as fp:
        lines = list(csv.DictReader(fp))
    input_uri = posixpath.join(args.s3_uri_prefix, f"{name}.jsonl")
    output_uri = posixpath.join(args.s3_uri_prefix, f"{name}_output.jsonl")
    lry.s3.write_as(lines, [dict], input_uri)
    params = {
        "definition_id": definition["DefinitionId"],
        "name": args.name,
        "input_uri": input_uri,
        "output_uri": output_uri,
    }
    if args.sandbox:
        params["sandbox"] = True
    if args.assignments:
        params["default_assignments"] = args.assignments
    batch_id = client.submit_batch(**params)
    print(f"A batch with id {batch_id} has been created")
    print(f"Results will be written to {output_uri}")
elif args.command == "get_batch_status":
    response = client.get_batch(args.batch_id)
    print(f"Batch {response['Id']}: {response['Name']}")
    print(f" - State: {response['State']}")
    print(f" - Input: {response['InputUri']}")
    print(f" - Items: {response.get('ItemCount', 0)}")
    print(f" - Created: {response.get('CreatedCount', 0)}")
    print(f" - Completed: {response.get('CompletedCount', 0)}")
    print(f" - Output: {response['OutputUri']}")
    print(f" - Response Counts:")
    print(f"     Task: {response.get('AssignmentCount', 0)}")
    print(f"     Test: {response.get('TestResponseCount', 0)}")
    print(f"     Total: {response.get('AssignmentCount', 0) + response.get('TestResponseCount', 0)}")
elif args.command == "get_batch_results":
    response = client.get_batch(args.batch_id)
    try:
        results = lry.s3.read_as([dict], response['OutputUri'])
        if len(results) == 0:
            print("No results yet")
        else:
            ext = os.path.splitext(args.output_file)[1][1:]
            if ext == "jsonl":
                lry.s3.download(args.output_file, response['OutputUri'])
            elif ext == "json":
                with open(args.output_file, "w") as fp:
                    json.dump(results, fp)
            else:
                # TODO: Detect and handle inputs and outputs with the same key
                fieldnames = dict.fromkeys(results[0]["Data"].keys())
                for result in results:
                    if "Result" in result:
                        fieldnames.update(dict.fromkeys(result["Result"].keys()))
                with open(args.output_file, "w", newline='') as fp:
                    writer = csv.DictWriter(fp, fieldnames=list(fieldnames.keys()))
                    writer.writeheader()
                    for result in results:
                        obj: dict = result["Data"].copy()
                        obj.update(result.get("Result", {}))
                        writer.writerow(obj)

    except ClientError as e:
        if e.code == "404":
            print("The output file not yet available")
        else:
            raise e
elif args.command == "list_workers":
    definition = read_definition(args.definition_file)
    response = client.list_workers(definition["DefinitionId"])
    with open(args.output_file, "w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["WorkerId", "Submitted", "ScoredCount", "Points"])
        writer.writeheader()
        for worker in response.get("Workers", []):
            writer.writerow(worker)
# elif args.command == "list_batches":
#    definition = read_definition(args.definition_file)
#    response = client.list_batches()
