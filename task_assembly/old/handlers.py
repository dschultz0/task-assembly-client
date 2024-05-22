import sys
import traceback
import larry as lry


def create_consolidation_lambda(
    handler,
    name,
    role,
    imports=None,
    functions=None,
    files=None,
    layers=None,
    timeout=None,
):
    functions = functions if functions else []
    functions.append(consolidation)
    imports = imports if imports else ["sys", "traceback"]
    if "sys" not in imports:
        imports.append("sys")
    if "traceback" not in imports:
        imports.append("traceback")
    package, handler = lry.lmbda.package_function(
        handler,
        imports=imports,
        functions=functions,
        decorators=["@consolidation"],
        files=files,
    )
    return lry.lmbda.create_or_update(
        name=name,
        package=package,
        role=role,
        handler=handler,
        layers=layers,
        timeout=timeout,
    )


def consolidation(func):
    def consolidation_wrapper(event, context=None):
        try:
            return func(event.get("Responses"), event.get("Sandbox", False))
        except Exception as e:
            print(e)
            exc_info = sys.exc_info()
            trace = "".join(traceback.format_exception(*exc_info))
            print(trace)
            return {"error": e.args[0], "trace": trace}

    return consolidation_wrapper
