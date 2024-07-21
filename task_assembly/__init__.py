import importlib.metadata
from .client import AssemblyClient

try:
    __version__ = importlib.metadata.version("task-assembly")
except importlib.metadata.PackageNotFoundError:
    __version__ = "local"
