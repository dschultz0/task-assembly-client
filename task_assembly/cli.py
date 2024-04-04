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

    def __init__(self, client:AssemblyClient):
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

    def create_blueprint(self, name,
            task_template=None,
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
            render_handler_arn=None):
        
        params = {}
        params["name"] = name

        if render_handler_arn:
            params["render_handler_arn"] = render_handler_arn
        if task_template:
            params["task_template"] = task_template
        if service:
            params["crowdconfig_service"] = service
        if title:
            params["crowdconfig_title"] = title
        if description:
            params["crowdconfig_description"] = description
        if reward_cents:
            params["crowdconfig_reward_cents"] = reward_cents                        
        if assignment_duration_seconds:
            params["crowdconfig_assignment_duration_seconds"] = assignment_duration_seconds
        if lifetime_seconds:
            params["crowdconfig_lifetime_seconds"] = lifetime_seconds
        if default_assignments:
            params["crowdconfig_default_assignments"] = default_assignments
        if max_assignments:
            params["crowdconfig_max_assignments"] = max_assignments
        if auto_approval_delay:
            params["crowdconfig__auto_approval_delay"] = auto_approval_delay
        if keywords:
            params["crowdconfig_keywords"] = keywords

        print(json.dumps(self.client.create_blueprint(**params), indent=4))

    def get_blueprints(self):
        print(json.dumps(self.client.get_blueprints(), indent=4))

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

    gtd_parser = subparsers.add_parser("get_blueprints")
    gtd_parser.set_defaults(func=CLI.get_blueprints)

    ct_parser = subparsers.add_parser("create_blueprint")
    ct_parser.add_argument("--name", type=str)
    ct_parser.add_argument("--render_handler_arn", type=str)
    ct_parser.add_argument("--service", type=str)
    ct_parser.add_argument("--title", type=str)
    ct_parser.add_argument("--description", type=str)
    ct_parser.add_argument("--reward_cents", type=int)
    ct_parser.add_argument("--assignment_duration_seconds", type=int)
    ct_parser.add_argument("--lifetime_seconds", type=int)
    ct_parser.add_argument("--default_assignments", type=int)
    ct_parser.add_argument("--max_assignments", type=int)
    ct_parser.add_argument("--auto_approval_delay", type=int)
    ct_parser.add_argument("--keywords", type=str)
    ct_parser.set_defaults(func=CLI.create_blueprint)

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

    if args.func:
        arg_dict = dict(args._get_kwargs())
        arg_dict.pop("func")
        arg_dict.pop("command")
        arg_dict.pop("profile")
        args.func(cli, **arg_dict)
    else:
        raise Exception("Misformated command")
