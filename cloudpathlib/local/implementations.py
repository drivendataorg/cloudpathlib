from ..cloudpath import CloudImplementation
from .localclient import LocalClient
from .localpath import LocalPath


_local_azure_blob_implementation = CloudImplementation()


class LocalAzureBlobClient(LocalClient):
    """Replacement for AzureBlobClient that uses the local file system. Intended as a monkeypatch
    substitute when writing tests.
    """

    _cloud_meta = _local_azure_blob_implementation


LocalAzureBlobClient.AzureBlobPath = LocalAzureBlobClient.CloudPath  # type: ignore


class LocalAzureBlobPath(LocalPath):
    """Replacement for AzureBlobPath that uses the local file system. Intended as a monkeypatch
    substitute when writing tests.
    """

    cloud_prefix: str = "az://"
    _cloud_meta = _local_azure_blob_implementation

    @property
    def drive(self) -> str:
        return self.container

    def mkdir(self, parents=False, exist_ok=False):
        # not possible to make empty directory on blob storage
        pass

    @property
    def container(self) -> str:
        return self._no_prefix.split("/", 1)[0]

    @property
    def blob(self) -> str:
        key = self._no_prefix_no_drive

        # key should never have starting slash for
        if key.startswith("/"):
            key = key[1:]

        return key

    @property
    def etag(self):
        return self.client._md5(self)

    @property
    def md5(self) -> str:
        return self.client._md5(self)


LocalAzureBlobPath.__name__ = "AzureBlobPath"

_local_azure_blob_implementation._client_class = LocalAzureBlobClient
_local_azure_blob_implementation._path_class = LocalAzureBlobPath


_local_s3_implementation = CloudImplementation()


class LocalS3Client(LocalClient):
    """Replacement for S3Client that uses the local file system. Intended as a monkeypatch
    substitute when writing tests.
    """

    _cloud_meta = _local_s3_implementation


LocalS3Client.S3Path = LocalS3Client.CloudPath  # type: ignore


class LocalS3Path(LocalPath):
    """Replacement for S3Path that uses the local file system. Intended as a monkeypatch substitute
    when writing tests.
    """

    cloud_prefix: str = "s3://"
    _cloud_meta = _local_s3_implementation

    @property
    def drive(self) -> str:
        return self.bucket

    def mkdir(self, parents=False, exist_ok=False):
        # not possible to make empty directory on s3
        pass

    @property
    def bucket(self) -> str:
        return self._no_prefix.split("/", 1)[0]

    @property
    def key(self) -> str:
        key = self._no_prefix_no_drive

        # key should never have starting slash for
        # use with boto, etc.
        if key.startswith("/"):
            key = key[1:]

        return key

    @property
    def etag(self):
        return self.client._md5(self)


LocalS3Path.__name__ = "S3Path"

_local_s3_implementation._client_class = LocalS3Client
_local_s3_implementation._path_class = LocalS3Path
