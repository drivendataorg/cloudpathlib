import pytest

from tests.utils import run_isolated


@pytest.mark.parametrize("provider", ["s3", "gs", "azure"])
@pytest.mark.parametrize(
    "constructor", ["client", "client_with_options", "default_client", "path", "dispatch"]
)
def test_missing_sdk_reports_install_hint(provider, constructor):
    blocked_modules = {
        "s3": ["boto3", "botocore"],
        "gs": ["google"],
        "azure": ["azure"],
    }[provider]
    client_name = {"s3": "S3Client", "gs": "GSClient", "azure": "AzureBlobClient"}[provider]
    script = """
        import cloudpathlib
        from cloudpathlib.exceptions import MissingDependenciesError

        provider, constructor = sys.argv[1:]
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
            print(type(error).__name__)
            print(error)
        """
    out = run_isolated(blocked_modules, script, args=(provider, constructor))

    assert out.splitlines()[0:1] == ["MissingDependenciesError"]
    assert client_name in out
    assert f"pip install cloudpathlib[{provider}]" in out
