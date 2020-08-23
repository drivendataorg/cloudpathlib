from datetime import datetime
import os
from pathlib import PurePosixPath
from typing import Optional, Union

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

from ..base import Backend
from ...cloudpath import register_backend_class


@register_backend_class("azure")
class AzureBlobBackend(Backend):
    """Backend for Azure Blob Storage."""

    def __init__(
        self,
        account_url: Optional[str] = None,
        credential: Optional[any] = None,
        connection_string: Optional[str] = None,
        blob_service_client: Optional[BlobServiceClient] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
    ):
        """
        Class constructor. Sets up a [`BlobServiceClient`][azure.storage.blob.BlobServiceClient].
        Supports the following authentication methods of `BlobServiceClient`.

        - Environment variable `""AZURE_STORAGE_CONNECTION_STRING"` containing connecting string
        with account credentials. See [Azure Storage SDK documentation](
        https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal).
        - Account URL via `account_url`, authenticated either with an embedded SAS token, or with
        credentials passed to `credentials`.
        - Connection string via `connection_string`, authenticated either with an embedded SAS
        token or with credentials passed to `credentials`.
        - Instantiated and already authenticated
        [`BlobServiceClient`][azure.storage.blob.BlobServiceClient].

        If multiple methods are used, priority order is reverse of list above (later in list takes
        priority).

        Parameters
        ----------
        account_url : Optional[str], optional
            The URL to the blob storage account, optionally authenticated with a SAS token. See
            [`BlobServiceClient`][azure.storage.blob.BlobServiceClient]. By default None.
        credential : Optional[any], optional
            Credentials with which to authenticate. Can be used with `account_url` or
            `connection_string`, but is unnecessary if the other already has an SAS token. See
            [`BlobServiceClient`][azure.storage.blob.BlobServiceClient] or
            [`BlobServiceClient.from_connection_string`][
            azure.storage.blob.BlobServiceClient.from_connection_string]. By default None.
        connection_string : Optional[str], optional
            A connection string to an Azure Storage account. See [Azure Storage SDK documentation](
            https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal).
            By default None.
        blob_service_client : Optional[BlobServiceClient], optional
            Instantiated [`BlobServiceClient`][azure.storage.blob.BlobServiceClient].
            By default None.
        local_cache_dir : Optional[Union[str, os.PathLike]]
            Path to directory to use as cache for downloaded files. If None, will use a temporary
            directory. By default None.
        """

        if blob_service_client is not None:
            self.service_client = blob_service_client
        elif connection_string is not None:
            self.service_client = BlobServiceClient.from_connection_string(
                conn_str=connection_string, credential=credential
            )
        elif account_url is not None:
            self.service_client = BlobServiceClient(account_url=account_url, credential=credential)
        else:
            self.service_client = BlobServiceClient.from_connection_string(
                os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            )

        super().__init__(local_cache_dir=local_cache_dir)

    def get_metadata(self, cloud_path):
        blob = self.service_client.get_blob_client(
            container=cloud_path.container, blob=cloud_path.blob
        )
        properties = blob.get_blob_properties()

        return properties

    def download_file(self, cloud_path, local_path):
        blob = self.service_client.get_blob_client(
            container=cloud_path.container, blob=cloud_path.blob
        )

        download_stream = blob.download_blob()
        local_path.write_bytes(download_stream.readall())

        return local_path

    def is_file_or_dir(self, cloud_path):
        # short-circuit the root-level container
        if not cloud_path.blob:
            return "dir"

        try:
            self.get_metadata(cloud_path)
            return "file"
        except ResourceNotFoundError:
            prefix = cloud_path.blob
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            # not a file, see if it is a directory
            container_client = self.service_client.get_container_client(cloud_path.container)

            try:
                next(container_client.list_blobs(name_starts_with=prefix))
                return "dir"
            except StopIteration:
                return None

    def exists(self, cloud_path):
        return self.is_file_or_dir(cloud_path) in ["file", "dir"]

    def list_dir(self, cloud_path, recursive=False):
        container_client = self.service_client.get_container_client(cloud_path.container)

        prefix = cloud_path.blob
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        yielded_dirs = set()

        # NOTE: Not recursive may be slower than necessary since it just filters
        #   the recursive implementation
        for o in container_client.list_blobs(name_starts_with=prefix):
            # get directory from this path
            for parent in PurePosixPath(o.name[len(prefix) :]).parents:
                parent = str(parent)

                # if we haven't surfaced thei directory already
                if parent not in yielded_dirs and parent != ".":

                    # skip if not recursive and this is beyond our depth
                    if not recursive and "/" in parent[len(prefix) :]:
                        continue

                    yield self.CloudPath(f"az://{cloud_path.container}/{prefix}{parent}")
                    yielded_dirs.add(parent)

            # skip file if not recursive and this is beyond our depth
            if not recursive and "/" in o.name[len(prefix) :]:
                continue

            yield self.CloudPath(f"az://{cloud_path.container}/{o.name}")

    def move_file(self, src, dst):
        # just a touch, so "REPLACE" metadata
        if src == dst:
            blob_client = self.service_client.get_blob_client(
                container=src.container, blob=src.blob
            )

            blob_client.set_blob_metadata(
                metadata=dict(last_modified=str(datetime.utcnow().timestamp()))
            )

        else:
            target = self.service_client.get_blob_client(container=dst.container, blob=dst.blob)

            source = self.service_client.get_blob_client(container=src.container, blob=src.blob)

            target.start_copy_from_url(source.url)

            self.remove(src)

        return dst

    def remove(self, cloud_path):
        if self.is_file_or_dir(cloud_path) == "dir":
            blobs = [b.blob for b in self.list_dir(cloud_path, recursive=True)]
            container_client = self.service_client.get_container_client(cloud_path.container)
            container_client.delete_blobs(*blobs)
        elif self.is_file_or_dir(cloud_path) == "file":
            blob = self.service_client.get_blob_client(
                container=cloud_path.container, blob=cloud_path.blob
            )

            blob.delete_blob()

        return cloud_path

    def upload_file(self, local_path, cloud_path):
        blob = self.service_client.get_blob_client(
            container=cloud_path.container, blob=cloud_path.blob
        )

        blob.upload_blob(local_path.read_bytes(), overwrite=True)

        return cloud_path


AzureBlobBackend.AzureBlobPath = AzureBlobBackend.CloudPath
