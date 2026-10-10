import subprocess
import sys
import textwrap

import pytest


@pytest.mark.parametrize("provider", ["s3", "gs", "azure"])
@pytest.mark.parametrize(
    "constructor", ["client", "client_with_options", "default_client", "path", "dispatch"]
)
def test_missing_sdk_reports_install_hint(provider, constructor):
    # Use a fresh interpreter so missing imports cannot be masked by loaded SDK modules.
    script = textwrap.dedent("""
        import builtins
        import sys

        provider, constructor = sys.argv[1:]
        blocked_roots = {
            "s3": {"boto3", "botocore"},
            "gs": {"google"},
            "azure": {"azure"},
        }[provider]
        original_import = builtins.__import__

        def import_without_sdk(name, globals=None, locals=None, fromlist=(), level=0):
            if level == 0 and name.split(".")[0] in blocked_roots:
                raise ModuleNotFoundError(f"No module named {name!r}")
            return original_import(name, globals, locals, fromlist, level)

        builtins.__import__ = import_without_sdk
        import cloudpathlib
        from cloudpathlib.exceptions import MissingDependenciesError

        client_name, path_name, cloud_path, options = {
            "s3": ("S3Client", "S3Path", "s3://bucket/file", {"no_sign_request": True}),
            "gs": (
                "GSClient", "GSPath", "gs://bucket/file",
                {"application_credentials": "missing-credentials.json"},
            ),
            "azure": (
                "AzureBlobClient", "AzureBlobPath", "az://container/file",
                {"connection_string": "AccountName=fake;AccountKey=fake;"},
            ),
        }[provider]
        client_class = getattr(cloudpathlib, client_name)
        try:
            if constructor == "client":
                client_class()
            elif constructor == "client_with_options":
                client_class(**options)
            elif constructor == "default_client":
                client_class.get_default_client()
            elif constructor == "path":
                getattr(cloudpathlib, path_name)(cloud_path)
            else:
                cloudpathlib.CloudPath(cloud_path)
        except MissingDependenciesError as error:
            assert client_name in str(error)
            assert f"pip install cloudpathlib[{provider}]" in str(error)
        else:
            raise AssertionError("Expected MissingDependenciesError")
        """)
    result = subprocess.run(
        [sys.executable, "-c", script, provider, constructor],
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
