from datetime import datetime
import os
from pathlib import PurePosixPath
from typing import Any, Dict, Iterable, Optional, Union

from ..client import Client, register_client_class
from ..cloudpath import implementation_registry
from .gspath import GSPath

try:
    from google.auth.credentials import Credentials
    from google.cloud.storage import Client as StorageClient

except ModuleNotFoundError:
    implementation_registry["gs"].dependencies_loaded = False


@register_client_class("gs")
class GSClient(Client):
    """Client for Google Cloud Storage."""

    def __init__(
        self,
        application_credentials: Optional[Union[str, os.PathLike]] = None,
        credentials: Optional[Credentials] = None,
        project: Optional[str] = None,
        storage_client: Optional[StorageClient] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
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
        (later in list takes priority).

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
            local_cache_dir (Optional[Union[str, os.PathLike]]): Path to directory to use as cache
                for downloaded files. If None, will use a temporary directory.
        """

        if storage_client is not None:
            self.client = storage_client
        elif credentials is not None:
            self.client = StorageClient(credentials=credentials, project=project)
        elif application_credentials is not None:
            self.client = StorageClient.from_service_account_json(application_credentials)
        else:
            self.client = StorageClient.from_service_account_json(
                os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            )

        super().__init__(local_cache_dir=local_cache_dir)

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
            }

    def _download_file(
        self, cloud_path: GSPath, local_path: Union[str, os.PathLike]
    ) -> Union[str, os.PathLike]:
        bucket = self.client.bucket(cloud_path.bucket)
        blob = bucket.get_blob(cloud_path.blob)

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
        return self._is_file_or_dir(cloud_path) in ["file", "dir"]

    def _list_dir(self, cloud_path: GSPath, recursive=False) -> Iterable[GSPath]:
        bucket = self.client.bucket(cloud_path.bucket)

        prefix = cloud_path.blob
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        yielded_dirs = set()

        # NOTE: Not recursive may be slower than necessary since it just filters
        #   the recursive implementation
        for o in bucket.list_blobs(prefix=prefix):
            # get directory from this path
            for parent in PurePosixPath(o.name[len(prefix) :]).parents:

                # if we haven't surfaced thei directory already
                if parent not in yielded_dirs and str(parent) != ".":

                    # skip if not recursive and this is beyond our depth
                    if not recursive and "/" in str(parent):
                        continue

                    yield self.CloudPath(f"gs://{cloud_path.bucket}/{prefix}{parent}")
                    yielded_dirs.add(parent)

            # skip file if not recursive and this is beyond our depth
            if not recursive and "/" in o.name[len(prefix) :]:
                continue

            yield self.CloudPath(f"gs://{cloud_path.bucket}/{o.name}")

    def _move_file(self, src: GSPath, dst: GSPath) -> GSPath:
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
            src_blob.delete()

        return dst

    def _remove(self, cloud_path: GSPath) -> None:
        if self._is_file_or_dir(cloud_path) == "dir":
            blobs = [b.blob for b in self._list_dir(cloud_path, recursive=True)]
            bucket = self.client.bucket(cloud_path.bucket)
            for blob in blobs:
                bucket.get_blob(blob).delete()
        elif self._is_file_or_dir(cloud_path) == "file":
            bucket = self.client.bucket(cloud_path.bucket)
            bucket.get_blob(cloud_path.blob).delete()

    def _upload_file(self, local_path: Union[str, os.PathLike], cloud_path: GSPath) -> GSPath:
        bucket = self.client.bucket(cloud_path.bucket)
        blob = bucket.blob(cloud_path.blob)

        blob.upload_from_filename(str(local_path))
        return cloud_path


GSClient.GSPath = GSClient.CloudPath  # type: ignore
