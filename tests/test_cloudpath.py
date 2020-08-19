import pytest

from cloudpathlib import AzureBlobPath, CloudPath, InvalidPrefix, S3Path


@pytest.mark.parametrize(
    "path_class, cloud_path",
    [
        (AzureBlobPath, "az://b/k"),
        (AzureBlobPath, "AZ://b/k"),
        (S3Path, "s3://b/k"),
        (S3Path, "S3://b/k"),
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
    with pytest.raises(InvalidPrefix):
        CloudPath("pp://b/k")
