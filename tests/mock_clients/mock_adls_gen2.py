from azure.storage.filedatalake import FileProperties

from .mock_azureblob import mocked_client_class_factory


def mocked_adls_factory(test_dir, blob_service_client):
    """Just wrap and use `MockBlobClient` where needed to mock ADLS Gen2"""

    class MockedDataLakeServiceClient:
        def __init__(self, blob_service_client):
            self.blob_service_client = blob_service_client

        @classmethod
        def from_connection_string(cls, *args, **kwargs):
            return cls(mocked_client_class_factory(test_dir, adls_gen2=True)())

        def get_file_system_client(self, file_system):
            return MockedFileSystemClient(self.blob_service_client)

    return MockedDataLakeServiceClient


class MockedFileSystemClient:
    def __init__(self, blob_service_client):
        self.blob_service_client = blob_service_client

    def get_file_client(self, key):
        return MockedFileClient(key, self.blob_service_client)


class MockedFileClient:
    def __init__(self, key, blob_service_client) -> None:
        self.key = key
        self.blob_service_client = blob_service_client

    def get_file_properties(self):
        path = self.blob_service_client.tmp_path / self.key

        if path.exists() and path.is_dir():
            return FileProperties(
                **{
                    "name": self.path.name,
                    "size": 0,
                    "etag": "etag",
                    "last_modified": self.path.stat().st_mtime,
                    "metadata": {"hdi_isfolder": True},
                }
            )

        # fallback to blob properties for files
        else:
            return self.blob_service_client.get_blob_client("", self.key).get_blob_properties()
