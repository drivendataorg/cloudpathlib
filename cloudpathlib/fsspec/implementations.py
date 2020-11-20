import importlib
import os

from fsspec.registry import known_implementations

from ..cloudpath import CloudImplementation, CloudPathMeta, CloudPath
from .fsspecclient import FsspecClient
from .fsspecpath import FsspecPath


implementation_registry = {}


def register_fsspec_implementation(protocol: str):
    implementation = CloudImplementation(name=f"fsspec-{protocol}")

    # known_implementations is a dictionary:
    # {
    #     protocol: {
    #         "class": filesystem_class_import_path,
    #     },
    # }
    import_path = known_implementations[protocol]["class"]
    module_name, class_name = import_path.rsplit(".", 1)

    try:
        filesystem_class = getattr(importlib.import_module(module_name), class_name)
    except ImportError:
        filesystem_class = type(class_name, (), {})
        implementation.dependencies_loaded = False

    class_name_prefix = filesystem_class.__name__.rpartition("FileSystem")[0]

    # Create concrete client class
    class _ConcreteClient(FsspecClient):
        _filesystem_class = filesystem_class

    _ConcreteClient.__name__ = f"{class_name_prefix}Client"
    _ConcreteClient._cloud_meta = implementation
    implementation._client_class = _ConcreteClient

    # Create concrete path class
    class _ConcretePath(FsspecPath):
        cloud_prefix = f"{protocol}://"

    _ConcretePath.__name__ = f"{class_name_prefix}Path"
    _ConcretePath._cloud_meta = implementation
    implementation._path_class = _ConcretePath

    # register implementation
    implementation_registry[protocol] = implementation


for protocol in known_implementations:
    register_fsspec_implementation(protocol)


def dispatch_fsspec_path(cloud_path, *args, **kwargs):
    protocol = str(cloud_path).partition("://")[0]
    if protocol in implementation_registry:
        path_class = implementation_registry[protocol].path_class()
        # Instantiate path_class instance
        new_obj = path_class.__new__(path_class, cloud_path, *args, **kwargs)
        if isinstance(new_obj, path_class):
            path_class.__init__(new_obj, cloud_path, *args, **kwargs)
        return new_obj
    raise Exception(
        f"Path {cloud_path} does not begin with a known prefix "
        f"{list(implementation_registry.keys())}."
    )


try:
    FSSPEC_MODE = int(os.getenv("FSSPEC_MODE", 0))
except ValueError:
    FSSPEC_MODE = 0

if FSSPEC_MODE:
    # Replace CloudPath dispatching with dispatch to fsspec class
    CloudPathMeta._dispatch_gates = [(CloudPath, dispatch_fsspec_path)]
else:
    # Add FsspecPath as an additional dispatch gate
    CloudPathMeta._dispatch_gates.append((FsspecPath, dispatch_fsspec_path))
