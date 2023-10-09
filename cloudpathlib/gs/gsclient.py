from datetime import datetime
import mimetypes
import os
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Dict, Iterable, Optional, TYPE_CHECKING, Tuple, Union

from ..client import Client, register_client_class
from ..cloudpath import implementation_registry
from ..enums import FileCacheMode
from .gspath import GSPath

try:
    if TYPE_CHECKING:
        from google.auth.credentials import Credentials

    from google.api_core.exceptions import NotFound
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud.storage import Client as StorageClient


except ModuleNotFoundError:
    implementation_registry["gs"].dependencies_loaded = False


@register_client_class("gs")
class GSClient(Client):
    """Client class for Google Cloud Storage which handles authentication with GCP for
    [`GSPath`](../gspath/) instances. See documentation for the
    [`__init__` method][cloudpathlib.gs.gsclient.GSClient.__init__] for detailed authentication
    options.
    """

    def __init__(
        self,
        application_credentials: Optional[Union[str, os.PathLike]] = None,
        credentials: Optional["Credentials"] = None,
        project: Optional[str] = None,
        storage_client: Optional["StorageClient"] = None,
        file_cache_mode: Optional[Union[str, FileCacheMode]] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
        content_type_method: Optional[Callable] = mimetypes.guess_type,
    ):
        """Class constructor. Sets up a [`Storage
        Client`](https://googleapis.dev/python/storage/latest/client.html).
        Supports the following authentication methods of `Storage Client`.

        - Environment variable `"GOOGLE_APPLICATION_CREDENTIALS"` containing a
          path to a JSON credentials file for a Google service account. See
          [Authenticating as a Service
          Account](https://cloud.google.com/docs/authentication/production).
        - File path to a JSON credentials file for a Google service account.
        - OAuth2 Credentials object and a project name.
        - Instantiated and already authenticated `Storage Client`.

        If multiple methods are used, priority order is reverse of list above
        (later in list takes priority). If no authentication methods are used,
        then the client will be instantiated as anonymous, which will only have
        access to public buckets.

        Args:
            application_credentials (Optional[Union[str, os.PathLike]]): Path to Google service
                account credentials file.
            credentials (Optional[Credentials]): The OAuth2 Credentials to use for this client.
                See documentation for [`StorageClient`](
                https://googleapis.dev/python/storage/latest/client.html).
            project (Optional[str]): The project which the client acts on behalf of. See
                documentation for [`StorageClient`](
                https://googleapis.dev/python/storage/latest/client.html).
            storage_client (Optional[StorageClient]): Instantiated [`StorageClient`](
                https://googleapis.dev/python/storage/latest/client.html).
            file_cache_mode (Optional[Union[str, FileCacheMode]]): How often to clear the file cache; see
                [the caching docs](https://cloudpathlib.drivendata.org/stable/caching/) for more information
                about the options in cloudpathlib.eums.FileCacheMode.
            local_cache_dir (Optional[Union[str, os.PathLike]]): Path to directory to use as cache
                for downloaded files. If None, will use a temporary directory. Default can be set with
                the `CLOUDPATHLIB_LOCAL_CACHE_DIR` environment variable.
            content_type_method (Optional[Callable]): Function to call to guess media type (mimetype) when
                writing a file to the cloud. Defaults to `mimetypes.guess_type`. Must return a tuple (content type, content encoding).
        """
        if application_credentials is None:
            application_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        if storage_client is not None:
            self.client = storage_client
        elif credentials is not None:
            self.client = StorageClient(credentials=credentials, project=project)
        elif application_credentials is not None:
            self.client = StorageClient.from_service_account_json(application_credentials)
        else:
            try:
                self.client = StorageClient()
            except DefaultCredentialsError:
                self.client = StorageClient.create_anonymous_client()

        super().__init__(
            local_cache_dir=local_cache_dir,
            content_type_method=content_type_method,
            file_cache_mode=file_cache_mode,
        )

    def _get_metadata(self, cloud_path: GSPath) -> Optional[Dict[str, Any]]:
        bucket = self.client.bucket(cloud_path.bucket)
        blob = bucket.get_blob(cloud_path.blob)

        if blob is None:
            return None
        else:
            return {
                "etag": blob.etag,
                "size": blob.size,
                "updated": blob.updated,
                "content_type": blob.content_type,
            }

    def _download_file(self, cloud_path: GSPath, local_path: Union[str, os.PathLike]) -> Path:
        bucket = self.client.bucket(cloud_path.bucket)
        blob = bucket.get_blob(cloud_path.blob)

        local_path = Path(local_path)

        blob.download_to_filename(local_path)
        return local_path

    def _is_file_or_dir(self, cloud_path: GSPath) -> Optional[str]:
        # short-circuit the root-level bucket
        if not cloud_path.blob:
            return "dir"

        bucket = self.client.bucket(cloud_path.bucket)
        blob = bucket.get_blob(cloud_path.blob)

        if blob is not None:
            return "file"
        else:
            prefix = cloud_path.blob
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            # not a file, see if it is a directory
            f = bucket.list_blobs(max_results=1, prefix=prefix)

            # at least one key with the prefix of the directory
            if bool(list(f)):
                return "dir"
            else:
                return None

    def _exists(self, cloud_path: GSPath) -> bool:
        # short-circuit the root-level bucket
        if not cloud_path.blob:
            try:
                next(self.client.bucket(cloud_path.bucket).list_blobs())
                return True
            except NotFound:
                return False

        return self._is_file_or_dir(cloud_path) in ["file", "dir"]

    def _list_dir(self, cloud_path: GSPath, recursive=False) -> Iterable[Tuple[GSPath, bool]]:
        # shortcut if listing all available buckets
        if not cloud_path.bucket:
            if recursive:
                raise NotImplementedError(
                    "Cannot recursively list all buckets and contents; you can get all the buckets then recursively list each separately."
                )

            yield from (
                (self.CloudPath(f"gs://{str(b)}"), True) for b in self.client.list_buckets()
            )
            return

        bucket = self.client.bucket(cloud_path.bucket)

        prefix = cloud_path.blob
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        if recursive:
            yielded_dirs = set()
            for o in bucket.list_blobs(prefix=prefix):
                # get directory from this path
                for parent in PurePosixPath(o.name[len(prefix) :]).parents:
                    # if we haven't surfaced this directory already
                    if parent not in yielded_dirs and str(parent) != ".":
                        yield (
                            self.CloudPath(f"gs://{cloud_path.bucket}/{prefix}{parent}"),
                            True,  # is a directory
                        )
                        yielded_dirs.add(parent)
                yield (self.CloudPath(f"gs://{cloud_path.bucket}/{o.name}"), False)  # is a file
        else:
            iterator = bucket.list_blobs(delimiter="/", prefix=prefix)

            # files must be iterated first for `.prefixes` to be populated:
            #   see: https://github.com/googleapis/python-storage/issues/863
            for file in iterator:
                yield (
                    self.CloudPath(f"gs://{cloud_path.bucket}/{file.name}"),
                    False,  # is a file
                )

            for directory in iterator.prefixes:
                yield (
                    self.CloudPath(f"gs://{cloud_path.bucket}/{directory}"),
                    True,  # is a directory
                )

    def _move_file(self, src: GSPath, dst: GSPath, remove_src: bool = True) -> GSPath:
        # just a touch, so "REPLACE" metadata
        if src == dst:
            bucket = self.client.bucket(src.bucket)
            blob = bucket.get_blob(src.blob)

            # See https://github.com/googleapis/google-cloud-python/issues/1185#issuecomment-431537214
            if blob.metadata is None:
                blob.metadata = {"updated": datetime.utcnow()}
            else:
                blob.metadata["updated"] = datetime.utcnow()
            blob.patch()

        else:
            src_bucket = self.client.bucket(src.bucket)
            dst_bucket = self.client.bucket(dst.bucket)

            src_blob = src_bucket.get_blob(src.blob)
            src_bucket.copy_blob(src_blob, dst_bucket, dst.blob)

            if remove_src:
                src_blob.delete()

        return dst

    def _remove(self, cloud_path: GSPath, missing_ok: bool = True) -> None:
        file_or_dir = self._is_file_or_dir(cloud_path)
        if file_or_dir == "dir":
            blobs = [
                b.blob for b, is_dir in self._list_dir(cloud_path, recursive=True) if not is_dir
            ]
            bucket = self.client.bucket(cloud_path.bucket)
            for blob in blobs:
                bucket.get_blob(blob).delete()
        elif file_or_dir == "file":
            bucket = self.client.bucket(cloud_path.bucket)
            bucket.get_blob(cloud_path.blob).delete()
        else:
            # Does not exist
            if not missing_ok:
                raise FileNotFoundError(f"File does not exist: {cloud_path}")

    def _upload_file(self, local_path: Union[str, os.PathLike], cloud_path: GSPath) -> GSPath:
        bucket = self.client.bucket(cloud_path.bucket)
        blob = bucket.blob(cloud_path.blob)

        extra_args = {}
        if self.content_type_method is not None:
            content_type, _ = self.content_type_method(str(local_path))
            extra_args["content_type"] = content_type

        blob.upload_from_filename(str(local_path), **extra_args)
        return cloud_path


GSClient.GSPath = GSClient.CloudPath  # type: ignore
