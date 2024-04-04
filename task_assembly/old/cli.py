import itertools
import json
import os.path
import posixpath
import sys
from pathlib import Path
from pkg_resources import resource_filename
import shutil
from datetime import datetime

import larry as lry
import argparse
import toml
import yaml
import csv

from botocore.exceptions import ClientError
from tabulate import tabulate

from .client import AssemblyClient
from .utils import REV_TASK_DEFINITION_ARG_MAP


class CLI:

    def __init__(self, client):
        self.client = client
        self.delimiter_map = {
            "tsv": "\t",
            "csv": ",",
            "txt": "\t",
        }

    def example(self):
        files = ["batch.csv", "gold.json", "handlers.py", "template.html"]
        for file in files:
            shutil.copy(resource_filename(__name__, f"example/{file}"), os.getcwd())
        print(f"The files {files} have been added to the current directory")

    def migrate_yaml(self, definition_file="definition.json"):
        base_name = os.path.splitext(definition_file)[0]
        yaml_name = f"{base_name}.yaml"
        with open(definition_file) as fp:
            definition = json.load(fp)
        with open(yaml_name, "w") as fp:
            yaml.dump(definition, fp)
        print(f"The file {definition_file} has been migrated to {yaml_name}, you may delete the original json file")

    def create_task_type(self, name):
        task_type_id = self.client.create_task_type(name)
        print(f"Created task type ID: {task_type_id}")

    def create_task_definition(self, name, task_type_id):
        definition = self.client.create_task_definition(name, task_type_id)
        definition["TaskType"] = task_type_id
        with open("definition.yaml", "w") as fp:
            yaml.dump(definition, fp)
        print(f"Created task definition {definition['DefinitionId']} in definition.yaml")

    def update_task_definition(self, definition_file):
        definition = self.read_definition(definition_file)
        if "TemplateFile" in definition:
            # TODO: Update to read UTF-8 and then warn on non-ASCII characters (or replace)
            with open(definition.pop("TemplateFile")) as fp:
                definition["Template"] = fp.read()
        if "GoldAnswersFile" in definition:
            with open(definition.pop("GoldAnswersFile")) as fp:
                definition["GoldAnswers"] = json.load(fp)
        if "HandlerFile" in definition:
            with open(definition.pop("HandlerFile")) as fp:
                definition["HandlerCode"] = fp.read()
        for key in ["Name", "Updated", "Created", "UpdatedBy"]:
            if key in definition:
                definition.pop(key)
        params = {REV_TASK_DEFINITION_ARG_MAP[k]: v for k, v in definition.items()}
        self.client.update_task_definition(**params)
        print(f"Updated task definition {definition['DefinitionId']}")

    def get_task_definition(self, id, definition_file=None):
        definition = self.client.get_task_definition(id)
        if "GoldAnswers" in definition:
            definition.pop("GoldAnswers")
        if definition_file:
            with open(definition_file, "w") as fp:
                yaml.dump(definition, fp)
        else:
            print(yaml.dump(definition))

    def get_task_definition_gold(self, id):
        definition = self.client.get_task_definition(id)
        if "GoldAnswers" in definition:
            with open("gold.json", "w") as fp:
                json.dump(definition["GoldAnswers"], fp, indent=4)

    def create_task(self, definition_file, assignments, sandbox, values, max_assignments, quals, tags,
                    use_computed_result=False):
        definition = self.read_definition(definition_file)
        params = {
            "definition_id": definition["DefinitionId"],
            "use_computed_result": use_computed_result
        }
        if assignments:
            params["default_assignments"] = assignments
        if max_assignments:
            params["max_assignments"] = max_assignments
        if sandbox:
            params["sandbox"] = True
        if isinstance(values, list):
            vals = [v.split("=") for v in values]
            params["data"] = {v[0]: v[1] for v in vals}
        else:
            vals = values.split("=")
            params["data"] = {vals[0]: vals[1]}
        if quals:
            params["qualification_requirements"] = json.loads(quals)
        if tags:
            params["tags"] = dict(tags)
        task_id = self.client.create_task(**params)
        print(f"Task created: {task_id}")

    def get_task(self, task_id, include_assignments=False):
        response = self.client.get_task(task_id, True, include_assignments)
        print(json.dumps(response, indent=4))

    def stop_task(self, task_id):
        self.client.expire_task(task_id)

    def redrive_task(self, task_id, extend):
        response = self.client.redrive_task(task_id, extend=extend)
        print(json.dumps(response, indent=4))

    def submit_batch(self, definition_file, name, input_file, s3_uri_prefix, sandbox=False, assignments=None):
        definition = self.read_definition(definition_file)
        name = name.replace(" ", "_")
        extension = os.path.splitext(input_file)[1][1:].lower()
        delimiter = self.delimiter_map.get(extension)
        if not delimiter:
            raise Exception("Input file must have an extension of csv, tsv, or txt")
        with open(input_file, encoding="utf-8-sig") as fp:
            lines = list(csv.DictReader(fp, delimiter=delimiter))
        input_uri = posixpath.join(s3_uri_prefix, f"{name}.jsonl")
        output_uri = posixpath.join(s3_uri_prefix, f"{name}_output.jsonl")
        lry.s3.write_as(lines, [dict], input_uri)
        params = {
            "definition_id": definition["DefinitionId"],
            "name": name,
            "input_uri": input_uri,
            "output_uri": output_uri,
        }
        if sandbox:
            params["sandbox"] = True
        if assignments:
            params["default_assignments"] = assignments
        batch_id = self.client.submit_batch(**params)
        print(f"A batch with id {batch_id} has been created")
        print(f"Results will be written to {output_uri}")

    def get_batch_status(self, batch_id):
        response = self.client.get_batch(batch_id)
        print(f"Batch {response['Id']}: {response['Name']}")
        print(f" - State: {response['State']}")
        if response["State"] == "Error" and "ErrorDetail" in response:
            detail = response["ErrorDetail"]
            if isinstance(detail, dict):
                if "errorMessage" in detail:
                    print(f" - Error: {detail['errorMessage']}")
                else:
                    print(f" - Error: ")
                    print(json.dumps(detail))
            else:
                print(f" - Error: {detail}")
        print(f" - Input: {response['InputUri']}")
        print(f" - Items: {response.get('ItemCount', 0)}")
        print(f" - Created: {response.get('CreatedCount', 0)}")
        print(f" - Completed: {response.get('CompletedCount', 0)}")
        if response.get('StateCounts'):
            counts = response.get('StateCounts')
            for key, value in counts.items():
                if value:
                    print(f"     {key}: {value}")
        print(f" - Output: {response['OutputUri']}")
        print(f" - Response Counts:")
        print(f"     Task: {response.get('AssignmentCount', 0)}")
        print(f"     Test: {response.get('TestResponseCount', 0)}")
        print(f"     Total: {response.get('AssignmentCount', 0) + response.get('TestResponseCount', 0)}")
        spend = response.get('Spend')
        if spend and (spend.get("TaskRewardCents") or spend.get("TestRewardCents")):
            z = "{:.2f}"
            print(f" - Spend:")
            if spend.get("TaskRewardCents"):
                print(f"     Task Reward: ${z.format(spend.get('TaskRewardCents', 0) / 100)}")
                print(f"     Task Fees: ${z.format(spend.get('TaskFeeCents', 0) / 100)}")
            if spend.get("TestRewardCents"):
                print(f"     Test Reward: ${z.format(spend.get('TestRewardCents', 0) / 100)}")
                print(f"     Test Fees: ${z.format(spend.get('TestFeeCents', 0) / 100)}")

    def get_batch_results(self, batch_id, output_file):
        response = self.client.get_batch(batch_id)
        try:
            print(f"Retrieving results from: {response['OutputUri']}")
            results = lry.s3.read_as([dict], response['OutputUri'])
            if len(results) == 0:
                print("No results yet")
            else:
                ext = os.path.splitext(output_file)[1][1:].lower()
                delimiter = self.delimiter_map.get(ext)
                if ext == "jsonl":
                    lry.s3.download(output_file, response['OutputUri'])
                elif ext == "json":
                    with open(output_file, "w") as fp:
                        json.dump(results, fp, indent=4)
                elif delimiter:
                    # TODO: Detect and handle inputs and outputs with the same key
                    fieldnames = dict.fromkeys(results[0]["Data"].keys())
                    for result in results:
                        if "Result" in result:
                            fieldnames.update(dict.fromkeys(result["Result"].keys()))
                    with open(output_file, "w", newline='') as fp:
                        writer = csv.DictWriter(fp, fieldnames=list(fieldnames.keys()), delimiter=delimiter)
                        writer.writeheader()
                        for result in results:
                            obj: dict = result["Data"].copy()
                            obj.update(result.get("Result", {}))
                            writer.writerow(obj)
                else:
                    raise Exception("Input file must have an extension of json, jsonl, csv, tsv, or txt")

        except ClientError as e:
            if e.code == "404":
                print("The output file not yet available")
            else:
                raise e

    def build_gold_from_batch(self, batch_id, output_file):
        response = self.client.get_batch(batch_id)
        try:
            print(f"Retrieving results from: {response['OutputUri']}")
            results = lry.s3.read_as([dict], response['OutputUri'])
            if len(results) == 0:
                print("No results yet")
            else:
                gold = [{k: v["value"] if k == 'Result' else v for k, v in r.items() if k in ["Data", "Result"]} for r in results]
                with open(output_file, "w") as fp:
                    json.dump(gold, fp, indent=4)

        except ClientError as e:
            if e.code == "404":
                print("The output file not yet available")
            else:
                raise e

    def list_workers(self, definition_file, output_file):
        definition = self.read_definition(definition_file)
        workers = self.client.list_workers(definition["DefinitionId"])
        with open(output_file, "w", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=["WorkerId", "Submitted", "ScoredCount", "Points", "Score"])
            writer.writeheader()
            for worker in workers:
                if worker.get("ScoredCount", 0) > 0 and worker.get("Points") is not None:
                    worker["Score"] = round(worker["Points"]/worker["ScoredCount"], 1)
                writer.writerow(worker)

    def list_tasks(self,
                   output_file,
                   definition_file=None,
                   tag: list[str] = None,
                   batch_id=None,
                   task_definition_id=None,
                   max_results=None):
        def_id = None
        if os.path.exists(definition_file):
            def_id = task_definition_id if task_definition_id else self.read_definition(definition_file)["DefinitionId"]
        if tag:
            tasks = self.client.list_tasks(tag=tag[0], tag_value=tag[1], task_definition_id=def_id)
        elif batch_id:
            tasks = self.client.list_tasks(batch_id=batch_id)
        elif def_id:
            tasks = self.client.list_tasks(task_definition_id=def_id)
        else:
            print("missing task_definition_id, batch_id or tag")
            sys.exit(-1)
        if max_results:
            tasks = itertools.islice(tasks, max_results)
        with open(output_file, "w", newline="", encoding="utf-8") as fp:
            writer = csv.DictWriter(fp, fieldnames=['TaskId', 'ResponseCount',
                        'State', 'Stats', 'Definition', 'Errors', 'Batch', 'ExtendRequested','IncompleteDetail',
                        'QualificationRequirements', 'TaskFeeCents', 'TestRewardCents', 'TaskRewardCents',
                        'TestFeeCents', 'UseComputedResult', 'TestResponseCount', 'Tag',])
            # Data	HITs	Assignments	Result	Sandbox	Responses
            writer.writeheader()
            for task in tasks:
                task.pop("Data", None)
                task.pop("HITs", None)
                task.pop("Assignments", None)
                task.pop("Result", None)
                task.pop("Responses", None)
                task.pop("Stats", None)
                spend = task.pop("Spend", None)
                if spend:
                    for k, v in spend.items():
                        task[k] = v
                writer.writerow(task)

    def close_testing(self, definition_file, min_score=None):
        definition = self.read_definition(definition_file)
        workers = self.client.list_workers(definition["DefinitionId"])
        min_score = min_score if min_score else definition["TestPolicy"]["MinScore"]
        min_tests = definition["TestPolicy"]["MinTests"]
        workers = [w for w in workers
                   if w.get("ScoredCount", 0) >= min_tests
                   and w.get("Points") is not None
                   and w["Points"]/w["ScoredCount"] >= min_score]
        print(f"Creating qualification for {len(workers)} workers")
        qualification_id = lry.mturk.create_qualification_type(
            f"Good {definition['DefinitionId']}",
            f"Good {definition['DefinitionId']}"
        )
        print(f"Assigning workers to qualification {qualification_id}")
        failures = []
        for w in workers:
            try:
                lry.mturk.assign_qualification(qualification_id, w["WorkerId"], 1)
            except ClientError:
                failures.append(w["WorkerId"])
        if failures:
            print(f"Failed to add the following {len(failures)} workers to the qualification:")
            for w in failures:
                print(w)
        print(f"Adding qualification to definition")
        reqs = definition.get("QualificationRequirements", [])
        if reqs is None:
            reqs = []
        reqs.append({
            "ActionsGuarded": "DiscoverPreviewAndAccept",
            "Comparator": "Exists",
            "QualificationTypeId": qualification_id
        })
        definition["QualificationRequirements"] = reqs
        with open(definition_file, "w") as fp:
            yaml.dump(definition, fp)
        self.update_task_definition(definition_file)

    def list_batches(self, definition_file, output_file=None, all_definitions=False):
        params = {}
        if not all_definitions:
            definition = self.read_definition(definition_file)
            params["definition_id"] = definition["DefinitionId"]
        batches = []
        has_available = True
        while has_available:
            response = self.client.list_batches(**params)
            batches.extend(response.get("Batches", []))
            if response.get("NextKey"):
                params["StartKey"] = response["NextKey"]
            else:
                has_available = False
        batches.sort(key=lambda x: x["Created"], reverse=True)
        for batch in batches:
            batch["Created"] = datetime.fromisoformat(batch["Created"]).strftime("%m/%d/%Y %H:%M")
            if "OrganizationId" in batch:
                del batch["OrganizationId"]
            if "Updated" in batch:
                del batch["Updated"]
        if output_file:
            self._write_output_file(
                batches,
                output_file,
                ["Id", "Name", "State", "Created", "CreatedCount", "CompletedCount"]
            )
        else:
            fields = {
                "Id": "Id",
                "Name": "Name",
                "State": "State",
                "Created": "Created",
                "CreatedCount": "Count",
                "CompletedCount": "Completed",
                "StateCounts.Success": "Successful",
            }
            table = []
            for b in batches:
                row = [b.get(k.split(".")[0], {}).get(k.split(".")[1]) if "." in k else b.get(k) for k in fields.keys()]
                table.append(row)
            print(tabulate(table, headers=list(fields.values())))

    def redrive_scoring(self, definition_file):
        definition = self.read_definition(definition_file)
        self.client.redrive_scoring(definition["DefinitionId"])

    def stop_batch(self, batch_id):
        self.client.expire_batch(batch_id)

    def redrive_batch(self, batch_id, extend):
        self.client.redrive_batch(batch_id, extend)

    def resolve_batch(self, batch_id, extend):
        self.client.resolve_batch(batch_id, extend)

    @staticmethod
    def read_definition(file_name):
        with open(file_name, "r") as ffp:
            definition_ = yaml.safe_load(ffp)
        return definition_

    def _write_output_file(self, results, output_file, field_order):
        ext = os.path.splitext(output_file)[1][1:].lower()
        delimiter = self.delimiter_map.get(ext)
        if ext == "jsonl":
            with open(output_file, "w") as fp:
                fp.writelines(results)
        elif ext == "json":
            with open(output_file, "w") as fp:
                json.dump(results, fp)
        elif delimiter:
            new_results = self._flatten_results_for_report(results, field_order)
            fieldnames = self._get_fieldnames(new_results)
            with open(output_file, "w", newline='') as fp:
                writer = csv.DictWriter(fp, fieldnames=fieldnames, delimiter=delimiter)
                writer.writeheader()
                for result in new_results:
                    writer.writerow(result)
        else:
            raise Exception("Input file must have an extension of json, jsonl, csv, tsv, or txt")

    @staticmethod
    def _flatten_results_for_report(results, field_order=None):
        if field_order is None:
            field_order = []
        new_results = []
        for r in results:
            row = r.copy()
            new_row = {}
            for f in field_order:
                new_row[f] = row.pop(f) if f in row else None
            for k, v in row.items():
                if isinstance(v, dict):
                    for kk, vv in v.items():
                        new_row[f"{k}.{kk}"] = vv
                else:
                    new_row[k] = v
            new_results.append(new_row)
        return new_results

    @staticmethod
    def _get_fieldnames(results):
        fieldnames = dict.fromkeys(results[0].keys())
        for result in results:
            fieldnames.update(dict.fromkeys(result.keys()))
        return list(fieldnames.keys())


def load_config(ta_config, profile) -> str:
    if not ta_config.exists():
        if os.path.exists("api-key.txt"):
            with open("api-key.txt") as fp:
                return fp.read().strip()
        print("No configuration file found. Please run the 'configure' command first.")
        exit(1)

    with open(ta_config) as fp:
        config = toml.load(fp)
        profile_config = config.get(profile)
    if not profile_config:
        print(f"No configuration found for {profile} profile")
        exit(1)
    profile_credentials = profile_config["credentials"]
    if profile_credentials.get("aws_profile"):
        lry.set_session(profile_name=profile_credentials.get("aws_profile"))
    api_key = None
    if "api_key" in profile_credentials:
        api_key = profile_credentials.get("api_key")
    elif "api_key_secret" in profile_credentials:
        sm = lry.session().client('secretsmanager')
        response = sm.get_secret_value(SecretId=profile_credentials["api_key_secret"])
        secret_value = response["SecretString"]
        try:
            secrets = json.loads(secret_value)
            api_key = secrets["api_key"]
        except:
            api_key = secret_value
    return api_key


def main():
    parser = argparse.ArgumentParser("Task Assembly CLI")
    parser.add_argument("--profile")
    subparsers = parser.add_subparsers(dest="command", required=True)

    c_parser = subparsers.add_parser("configure")
    c_parser.add_argument("--key")
    c_parser.add_argument("--key_secret")
    c_parser.add_argument("--aws_profile")
    c_parser.add_argument("--validate", action="store_true")

    ex_parser = subparsers.add_parser("example")
    ex_parser.set_defaults(func=CLI.example)

    my_parser = subparsers.add_parser("migrate_yaml")
    my_parser.add_argument("--definition_file", default="definition.json")
    my_parser.set_defaults(func=CLI.migrate_yaml)

    ctt_parser = subparsers.add_parser("create_task_type")
    ctt_parser.add_argument("name")
    ctt_parser.set_defaults(func=CLI.create_task_type)

    ctd_parser = subparsers.add_parser("create_task_definition")
    ctd_parser.add_argument("name")
    ctd_parser.add_argument("task_type_id")
    ctd_parser.set_defaults(func=CLI.create_task_definition)

    utd_parser = subparsers.add_parser("update_task_definition")
    utd_parser.add_argument("--definition_file", default="definition.yaml")
    utd_parser.set_defaults(func=CLI.update_task_definition)

    gtd_parser = subparsers.add_parser("get_task_definition")
    gtd_parser.add_argument("id")
    gtd_parser.add_argument("--definition_file")
    gtd_parser.set_defaults(func=CLI.get_task_definition)

    gtdg_parser = subparsers.add_parser("get_task_definition_gold")
    gtdg_parser.add_argument("id")
    gtdg_parser.set_defaults(func=CLI.get_task_definition_gold)

    ct_parser = subparsers.add_parser("create_task")
    ct_parser.add_argument("values", type=str, nargs="*")
    ct_parser.add_argument("--assignments", type=int)
    ct_parser.add_argument("--sandbox", action="store_true")
    ct_parser.add_argument("--definition_file", default="definition.yaml")
    ct_parser.add_argument("--max_assignments", type=int)
    ct_parser.add_argument("--quals", type=str)
    ct_parser.add_argument("--use_computed_result", action="store_true")
    ct_parser.add_argument("--tag", dest="tags", nargs=2, action="append")
    ct_parser.set_defaults(func=CLI.create_task)

    gt_parser = subparsers.add_parser("get_task")
    gt_parser.add_argument("task_id")
    gt_parser.add_argument("--include_assignments", action="store_true")
    gt_parser.set_defaults(func=CLI.get_task)

    st_parser = subparsers.add_parser("stop_task")
    st_parser.add_argument("task_id")
    st_parser.set_defaults(func=CLI.stop_task)

    rt_parser = subparsers.add_parser("redrive_task")
    rt_parser.add_argument("task_id")
    rt_parser.add_argument("--extend", action="store_true")
    rt_parser.set_defaults(func=CLI.redrive_task)

    sb_parser = subparsers.add_parser("submit_batch")
    sb_parser.add_argument("--definition_file", default="definition.yaml")
    sb_parser.add_argument("--sandbox", action="store_true")
    sb_parser.add_argument("--assignments", type=int)
    sb_parser.add_argument("name")
    sb_parser.add_argument("input_file")
    sb_parser.add_argument("s3_uri_prefix")
    sb_parser.set_defaults(func=CLI.submit_batch)

    stb_parser = subparsers.add_parser("stop_batch")
    stb_parser.add_argument("batch_id")
    stb_parser.set_defaults(func=CLI.stop_batch)

    gbs_parser = subparsers.add_parser("get_batch_status")
    gbs_parser.add_argument("batch_id")
    gbs_parser.set_defaults(func=CLI.get_batch_status)

    gbr_parser = subparsers.add_parser("get_batch_results")
    gbr_parser.add_argument("batch_id")
    gbr_parser.add_argument("output_file")
    gbr_parser.set_defaults(func=CLI.get_batch_results)

    gbr_parser = subparsers.add_parser("build_gold_from_batch")
    gbr_parser.add_argument("batch_id")
    gbr_parser.add_argument("--output_file", default="gold.json")
    gbr_parser.set_defaults(func=CLI.build_gold_from_batch)

    lw_parser = subparsers.add_parser("list_workers")
    lw_parser.add_argument("--definition_file", default="definition.yaml")
    lw_parser.add_argument("output_file")
    lw_parser.set_defaults(func=CLI.list_workers)

    lt_parser = subparsers.add_parser("list_tasks")
    lt_parser.add_argument("--definition_file", default="definition.yaml")
    lt_parser.add_argument("output_file")
    lt_parser.add_argument("--tag", nargs=2)
    lt_parser.add_argument("--batch_id")
    lt_parser.add_argument("--task_definition_id")
    lt_parser.add_argument("--max_results", type=int)
    lt_parser.set_defaults(func=CLI.list_tasks)

    clt_parser = subparsers.add_parser("close_testing")
    clt_parser.add_argument("--definition_file", default="definition.yaml")
    clt_parser.add_argument("--min_score", type=int)
    clt_parser.set_defaults(func=CLI.close_testing)

    lb_parser = subparsers.add_parser("list_batches")
    lb_parser.add_argument("--definition_file", default="definition.yaml")
    lb_parser.add_argument("--all_definitions", action="store_true")
    lb_parser.add_argument("--output_file")
    lb_parser.set_defaults(func=CLI.list_batches)

    rds_parser = subparsers.add_parser("redrive_scoring")
    rds_parser.add_argument("--definition_file", default="definition.yaml")
    rds_parser.set_defaults(func=CLI.redrive_scoring)

    rb_parser = subparsers.add_parser("redrive_batch")
    rb_parser.add_argument("batch_id")
    rb_parser.add_argument("--extend", action="store_true")
    rb_parser.set_defaults(func=CLI.redrive_batch)

    rsb_parser = subparsers.add_parser("resolve_batch")
    rsb_parser.add_argument("batch_id")
    rsb_parser.add_argument("--extend", action="store_true")
    rsb_parser.set_defaults(func=CLI.resolve_batch)

    args = parser.parse_args()

    ta_dir = Path.home().joinpath(".taskassembly")
    ta_config = ta_dir.joinpath("config.toml")
    profile = args.profile if args.profile else "default"

    if args.command == "configure" and (args.key or args.key_secret or args.aws_profile):
        ta_dir.mkdir(exist_ok=True)
        config = {"version": "0.1"}
        if ta_config.exists():
            with open(ta_config) as fp:
                config = toml.load(fp)
        if profile not in config:
            config[profile] = {}
        pf = config[profile]
        if "credentials" not in pf:
            pf["credentials"] = {}
        creds = pf["credentials"]
        if args.key:
            creds["api_key"] = args.key
        if args.key_secret:
            creds["api_key_secret"] = args.key_secret
        if args.aws_profile:
            creds["aws_profile"] = args.aws_profile
        with open(ta_config, "w") as fp:
            toml.dump(config, fp)
        if not args.validate:
            exit(0)

    api_key = load_config(ta_config, profile)

    if api_key is None:
        print("Missing api key value")
        exit(1)

    client = AssemblyClient(api_key)
    cli = CLI(client)

    if args.command == "configure" and args.validate:
        response = client.validate()
        print(f"Organization: {response['Organization']['Name']}")
        print(f"AWS Account: {response['AWSAccountId']}")
        print(f"MTurk connection status: {'SUCCESS' if response.get('MTurk') else 'FAILED'}")
        print(f"MTurk Sandbox connection status: {'SUCCESS' if response.get('MTurkSandbox') else 'FAILED'}")
    elif args.func:
        arg_dict = dict(args._get_kwargs())
        arg_dict.pop("func")
        arg_dict.pop("command")
        arg_dict.pop("profile")
        args.func(cli, **arg_dict)
    else:
        raise Exception("Misformated command")
