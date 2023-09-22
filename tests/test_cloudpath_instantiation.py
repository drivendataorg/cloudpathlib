import inspect
import os
from pathlib import PurePath
import re

import pytest

from cloudpathlib import AzureBlobPath, CloudPath, GSPath, S3Path
from cloudpathlib.exceptions import InvalidPrefixError, MissingDependenciesError


@pytest.mark.parametrize(
    "path_class, cloud_path",
    [
        (AzureBlobPath, "az://b/k"),
        (AzureBlobPath, "AZ://b/k"),
        (AzureBlobPath, "Az://b/k"),
        (AzureBlobPath, "aZ://b/k"),
        (S3Path, "s3://b/k"),
        (S3Path, "S3://b/k"),
        (GSPath, "gs://b/k"),
        (GSPath, "GS://b/k"),
        (GSPath, "Gs://b/k"),
        (GSPath, "gS://b/k"),
    ],
)
def test_dispatch(path_class, cloud_path, monkeypatch):
    """Test that CloudPath(...) appropriately dispatches to the correct cloud's implementation
    class.
    """
    if path_class == AzureBlobPath:
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "AccountName=fake;AccountKey=fake2;")

    assert isinstance(CloudPath(cloud_path), path_class)


def test_dispatch_error():
    with pytest.raises(InvalidPrefixError):
        CloudPath("pp://b/k")


@pytest.mark.parametrize("path", ["b/k", "b/k", "b/k.file", "b/k", "b"])
def test_instantiation(rig, path):
    # check two cases of prefix
    for prefix in [rig.cloud_prefix.lower(), rig.cloud_prefix.upper()]:
        expected = prefix + path
        p = rig.path_class(expected)
        assert repr(p) == f"{rig.path_class.__name__}('{expected}')"
        assert str(p) == expected

        assert p._no_prefix == expected.split("://", 1)[-1]

        assert p._url.scheme == expected.split("://", 1)[0].lower()
        assert p._url.netloc == expected.split("://", 1)[-1].split("/")[0]

        assert str(p._path) == expected.split(":/", 1)[-1]


def test_instantiation_errors(rig):
    with pytest.raises(TypeError):
        rig.path_class()

    with pytest.raises(InvalidPrefixError):
        rig.path_class("NOT_S3_PATH")


def test_idempotency(rig):
    rig.client_class._default_client = None

    client = rig.client_class()
    p = client.CloudPath(f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt")

    p2 = CloudPath(p)
    assert p == p2
    assert p.client == p2.client


def test_dependencies_not_loaded(rig, monkeypatch):
    monkeypatch.setattr(rig.path_class._cloud_meta, "dependencies_loaded", False)
    with pytest.raises(MissingDependenciesError):
        CloudPath(f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt")
    with pytest.raises(MissingDependenciesError):
        rig.create_cloud_path("dir_0/file0_0.txt")


def test_is_pathlike(rig):
    p = rig.create_cloud_path("dir_0")
    assert isinstance(p, os.PathLike)


def test_public_interface_is_superset(rig):
    """Test that a CloudPath has all of the Path methods and properties. For methods
    we also ensure that the only difference in the signature is that a CloudPath has
    optional additional kwargs (which are likely added in subsequent Python versions).
    """
    lp = PurePath(".")
    cp = rig.create_cloud_path("dir_0/file0_0.txt")

    # Use regex to find the methods not implemented that are listed in the CloudPath code
    not_implemented_section = re.search(
        r"# =+ NOT IMPLEMENTED =+\n(.+?)\n\n", inspect.getsource(CloudPath), re.DOTALL
    )

    if not_implemented_section:
        methods_not_implemented_str = not_implemented_section.group(1)
        methods_not_implemented = re.findall(r"# (\w+)", methods_not_implemented_str)

    for name, lp_member in inspect.getmembers(lp):
        if name.startswith("_") or name in methods_not_implemented:
            continue

        # checks all public methods and properties
        cp_member = getattr(cp, name, None)
        assert cp_member is not None, f"CloudPath missing {name}"

        # for methods, checks the function signature
        if callable(lp_member):
            cp_signature = inspect.signature(cp_member)
            lp_signature = inspect.signature(lp_member)

            # all parameters for Path method should be part of CloudPath signature
            for parameter in lp_signature.parameters:
                # some parameters like _deprecated in Path.is_relative_to are not really part of the signature
                if parameter.startswith("_") or (
                    name == "joinpath" and parameter in ["args", "pathsegments"]
                ):  # handle arg name change in 3.12
                    continue

                assert (
                    parameter in cp_signature.parameters
                ), f"CloudPath.{name} missing parameter {parameter}"

            # extra parameters for CloudPath method should be optional with defaults
            for parameter, param_details in cp_signature.parameters.items():
                if name == "joinpath" and parameter in [
                    "args",
                    "pathsegments",
                ]:  # handle arg name change in 3.12
                    continue

                if parameter not in lp_signature.parameters:
                    assert (
                        param_details.default is not inspect.Parameter.empty
                    ), f"CloudPath.{name} added parameter {parameter} without a default"
