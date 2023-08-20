from datetime import datetime
import mimetypes
import os
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union


from ..client import Client, register_client_class
from ..cloudpath import implementation_registry
from ..enums import FileCacheMode
from ..exceptions import MissingCredentialsError
from .azblobpath import AzureBlobPath


try:
    from azure.core.exceptions import ResourceNotFoundError
    from azure.storage.blob import BlobServiceClient, BlobProperties, ContentSettings
except ModuleNotFoundError:
    implementation_registry["azure"].dependencies_loaded = False


@register_client_class("azure")
class AzureBlobClient(Client):
    """Client class for Azure Blob Storage which handles authentication with Azure for
    [`AzureBlobPath`](../azblobpath/) instances. See documentation for the
    [`__init__` method][cloudpathlib.azure.azblobclient.AzureBlobClient.__init__] for detailed
    authentication options.
    """

    def __init__(
        self,
        account_url: Optional[str] = None,
        credential: Optional[Any] = None,
        connection_string: Optional[str] = None,
        blob_service_client: Optional["BlobServiceClient"] = None,
        file_cache_mode: Optional[Union[str, FileCacheMode]] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
        content_type_method: Optional[Callable] = mimetypes.guess_type,
    ):
        """Class constructor. Sets up a [`BlobServiceClient`](
        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python).
        Supports the following authentication methods of `BlobServiceClient`.

        - Environment variable `""AZURE_STORAGE_CONNECTION_STRING"` containing connecting string
        with account credentials. See [Azure Storage SDK documentation](
        https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal).
        - Account URL via `account_url`, authenticated either with an embedded SAS token, or with
        credentials passed to `credentials`.
        - Connection string via `connection_string`, authenticated either with an embedded SAS
        token or with credentials passed to `credentials`.
        - Instantiated and already authenticated [`BlobServiceClient`](
        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python).

        If multiple methods are used, priority order is reverse of list above (later in list takes
        priority). If no methods are used, a [`MissingCredentialsError`][cloudpathlib.exceptions.MissingCredentialsError]
        exception will be raised raised.

        Args:
            account_url (Optional[str]): The URL to the blob storage account, optionally
                authenticated with a SAS token. See documentation for [`BlobServiceClient`](
                https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python).
            credential (Optional[Any]): Credentials with which to authenticate. Can be used with
                `account_url` or `connection_string`, but is unnecessary if the other already has
                an SAS token. See documentation for [`BlobServiceClient`](
                https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python)
                or [`BlobServiceClient.from_connection_string`](
                https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python#from-connection-string-conn-str--credential-none----kwargs-).
            connection_string (Optional[str]): A connection string to an Azure Storage account. See
                [Azure Storage SDK documentation](
                https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal).
            blob_service_client (Optional[BlobServiceClient]): Instantiated [`BlobServiceClient`](
                https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python).
            file_cache_mode (Optional[Union[str, FileCacheMode]]): How often to clear the file cache; see
                [the caching docs](https://cloudpathlib.drivendata.org/stable/caching/) for more information
                about the options in cloudpathlib.eums.FileCacheMode.
            local_cache_dir (Optional[Union[str, os.PathLike]]): Path to directory to use as cache
                for downloaded files. If None, will use a temporary directory. Default can be set with
                the `CLOUDPATHLIB_LOCAL_CACHE_DIR` environment variable.
            content_type_method (Optional[Callable]): Function to call to guess media type (mimetype) when
                writing a file to the cloud. Defaults to `mimetypes.guess_type`. Must return a tuple (content type, content encoding).
        """
        super().__init__(
            local_cache_dir=local_cache_dir,
            content_type_method=content_type_method,
            file_cache_mode=file_cache_mode,
        )

        if connection_string is None:
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", None)

        if blob_service_client is not None:
            self.service_client = blob_service_client
        elif connection_string is not None:
            self.service_client = BlobServiceClient.from_connection_string(
                conn_str=connection_string, credential=credential
            )
        elif account_url is not None:
            self.service_client = BlobServiceClient(account_url=account_url, credential=credential)
        else:
            raise MissingCredentialsError(
                "AzureBlobClient does not support anonymous instantiation. "
                "Credentials are required; see docs for options."
            )

    def _get_metadata(self, cloud_path: AzureBlobPath) -> Union["BlobProperties", Dict[str, Any]]:
        blob = self.service_client.get_blob_client(
            container=cloud_path.container, blob=cloud_path.blob
        )
        properties = blob.get_blob_properties()

        properties["content_type"] = properties.content_settings.content_type

        return properties

    def _download_file(
        self, cloud_path: AzureBlobPath, local_path: Union[str, os.PathLike]
    ) -> Path:
        blob = self.service_client.get_blob_client(
            container=cloud_path.container, blob=cloud_path.blob
        )

        download_stream = blob.download_blob()

        local_path = Path(local_path)

        local_path.parent.mkdir(exist_ok=True, parents=True)

        local_path.write_bytes(download_stream.content_as_bytes())

        return local_path

    def _is_file_or_dir(self, cloud_path: AzureBlobPath) -> Optional[str]:
        # short-circuit the root-level container
        if not cloud_path.blob:
            return "dir"

        try:
            self._get_metadata(cloud_path)
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

    def _exists(self, cloud_path: AzureBlobPath) -> bool:
        # short circuit when only the container
        if not cloud_path.blob:
            return self.service_client.get_container_client(cloud_path.container).exists()

        return self._is_file_or_dir(cloud_path) in ["file", "dir"]

    def _list_dir(
        self, cloud_path: AzureBlobPath, recursive: bool = False
    ) -> Iterable[Tuple[AzureBlobPath, bool]]:
        # shortcut if listing all available containers
        if not cloud_path.container:
            if recursive:
                raise NotImplementedError(
                    "Cannot recursively list all containers and contents; you can get all the containers then recursively list each separately."
                )

            yield from (
                (self.CloudPath(f"az://{c.name}"), True)
                for c in self.service_client.list_containers()
            )
            return

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
                # if we haven't surfaced this directory already
                if parent not in yielded_dirs and str(parent) != ".":
                    # skip if not recursive and this is beyond our depth
                    if not recursive and "/" in str(parent):
                        continue

                    yield (
                        self.CloudPath(f"az://{cloud_path.container}/{prefix}{parent}"),
                        True,  # is a directory
                    )
                    yielded_dirs.add(parent)

            # skip file if not recursive and this is beyond our depth
            if not recursive and "/" in o.name[len(prefix) :]:
                continue

            yield (self.CloudPath(f"az://{cloud_path.container}/{o.name}"), False)  # is a file

    def _move_file(
        self, src: AzureBlobPath, dst: AzureBlobPath, remove_src: bool = True
    ) -> AzureBlobPath:
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

            if remove_src:
                self._remove(src)

        return dst

    def _remove(self, cloud_path: AzureBlobPath, missing_ok: bool = True) -> None:
        file_or_dir = self._is_file_or_dir(cloud_path)
        if file_or_dir == "dir":
            blobs = [
                b.blob for b, is_dir in self._list_dir(cloud_path, recursive=True) if not is_dir
            ]
            container_client = self.service_client.get_container_client(cloud_path.container)
            container_client.delete_blobs(*blobs)
        elif file_or_dir == "file":
            blob = self.service_client.get_blob_client(
                container=cloud_path.container, blob=cloud_path.blob
            )

            blob.delete_blob()
        else:
            # Does not exist
            if not missing_ok:
                raise FileNotFoundError(f"File does not exist: {cloud_path}")

    def _upload_file(
        self, local_path: Union[str, os.PathLike], cloud_path: AzureBlobPath
    ) -> AzureBlobPath:
        blob = self.service_client.get_blob_client(
            container=cloud_path.container, blob=cloud_path.blob
        )

        extra_args = {}
        if self.content_type_method is not None:
            content_type, content_encoding = self.content_type_method(str(local_path))

            if content_type is not None:
                extra_args["content_type"] = content_type
            if content_encoding is not None:
                extra_args["content_encoding"] = content_encoding

        content_settings = ContentSettings(**extra_args)

        blob.upload_blob(Path(local_path).read_bytes(), overwrite=True, content_settings=content_settings)  # type: ignore

        return cloud_path


AzureBlobClient.AzureBlobPath = AzureBlobClient.CloudPath  # type: ignore
