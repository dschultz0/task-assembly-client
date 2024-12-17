from prance import ResolvingParser
import json

parser = ResolvingParser("schema.json")
schemas = parser.specification["components"]["schemas"]

type_replace = {
    "string": "str",
    "integer": "int",
    "boolean": "bool",
    "null": "None"
}

schema = json.loads('{"properties": {"blueprintId": {"anyOf": [{"type": "string"}, {"type": "null"}], "title": "Blueprintid"}, "assetId": {"anyOf": [{"type": "string"}, {"type": "null"}], "title": "Assetid"}, "name": {"anyOf": [{"type": "string"}, {"type": "null"}], "title": "Name"}, "state": {"anyOf": [{"type": "string"}, {"type": "null"}], "title": "State"}, "uri": {"anyOf": [{"type": "string"}, {"type": "null"}], "title": "Uri"}, "kb": {"anyOf": [{"type": "integer"}, {"type": "null"}], "title": "Kb", "default": 0}}, "type": "object", "title": "BlueprintAssetResponse"}')

def parse_object(schema):
    if "type" in schema and schema["type"] == "object":
        print("object")
        return schema["title"] if "title" in schema else "object"
    return False

def parse_array(schema):
    if "type" in schema and schema["type"] == "array":
        print("array")
        items = schema["items"]
        type_str = parse_object(items) or parse_anyof(items) or parse_type(items)
        return "List[" + type_str + "]"
    return False

def parse_anyof(schema):
    if "anyOf" in schema:
        print("anyOf")
        any_of_array = schema["anyOf"]
        type_str = []
        for item in any_of_array:
            if len(item) == 0:
                continue
            print("parsing {}".format(item))
            type_str.append(parse_object(item) or parse_array(item) or parse_type(item))
        return " | ".join(type_str)
    return False

def parse_type(schema):
    if "type" in schema:
        if schema["type"] == "string":
            print("str")
            return "str"
        elif schema["type"] == "integer":
            print("int")
            return "int"
        elif schema["type"] == "null":
            print("None")
            return "None"
        elif schema["type"] == "boolean":
            print("bool")
            return "bool"
    return False

for fqn, schema in schemas.items():
    if fqn == "BlueprintRequest" or True:
        print("fqn = {0}".format(fqn))
        properties = schema["properties"]
        print(properties)

        for property_name in properties.keys():
            print("parsing property - {}".format(property_name))
            property_bag = properties[property_name]
            print(parse_anyof(property_bag) or parse_type(property_bag))

"""
def parse_type(argument, script):
    if not "anyOf" in script:
        the_type = ""
        if script["type"] == "object":
            the_type = script["title"]
        else:
            the_type = type_replace[script["type"]]
        return "{0}:{1}".format(argument, the_type)
    else:
"""

"""
for fqn, schema in schemas.items():
    if fqn == "BlueprintRequest" or True:
        properties = schema["properties"]
        print("\n")
        print("___")
        print(fqn)
        print(properties)
        print("___")
        print("\n")
        prop_result = ""
        for property in properties.keys():
            types_append = ""

            print("___")
            print(properties[property])
            print("___")

            if "anyOf" in properties[property]:
                for a_type_dict in properties[property]["anyOf"]:
                    replace_str = ""
                    if a_type_dict["type"] == "object":
                        replace_str = a_type_dict["title"]
                    elif a_type_dict["type"] == "array":
                        if a_type_dict["items"]["type"] == "object":
                            replace_str = a_type_dict["items"]["title"]
                        else:
                            replace_str = "List[" + a_type_dict["items"]["type"] + "]"
                    else:
                        replace_str = type_replace[a_type_dict["type"]]

                    if types_append == "":
                        types_append = replace_str
                    else:
                        types_append = types_append + "|" + replace_str
            else:
                types_append = type_replace[properties[property]["type"]]

            if len(prop_result) > 0:
                prop_result = "{0}, {1}:{2}".format(prop_result, property, types_append)
            else:
                prop_result = "{1}:{2}".format(prop_result, property, types_append)

        print(prop_result)
"""