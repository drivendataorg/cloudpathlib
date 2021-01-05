import os
from pathlib import PurePosixPath
from typing import Any, Dict, Iterable, Optional, Union

from ..client import Client, register_client_class
from ..cloudpath import implementation_registry
from .gspath import GSPath

try:
    from google.cloud.storage import Client as Client_

except ModuleNotFoundError:
    implementation_registry["gs"].dependencies_loaded = False


@register_client_class("gs")
class GSClient(Client):
    """Client for Google Cloud Storage."""

    def __init__(
        self,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
    ):
        """Class constructor.

        Args:
            local_cache_dir (Optional[Union[str, os.PathLike]]): Path to directory to use as cache
                for downloaded files. If None, will use a temporary directory.
        """

        self.client = Client_()
        super().__init__(local_cache_dir=local_cache_dir)

    def _get_metadata(self, cloud_path: GSPath) -> Dict[str, Any]:
        bucket = self.client.get_bucket(cloud_path.bucket)
        blob = bucket.get_blob(cloud_path.key)

        return blob.metadata or {}

    def _download_file(
        self, cloud_path: GSPath, local_path: Union[str, os.PathLike]
    ) -> Union[str, os.PathLike]:
        bucket = self.client.get_bucket(cloud_path.bucket)
        blob = bucket.get_blob(cloud_path.key)

        with open(local_path, "bw") as file_object:
            self.client.download_blob_to_file(blob, file_object)
        return local_path

    def _is_file_or_dir(self, cloud_path: GSPath) -> Optional[str]:
        # short-circuit the root-level bucket
        if not cloud_path.key:
            return "dir"

        bucket = self.client.get_bucket(cloud_path.bucket)
        blob = bucket.get_blob(cloud_path.key)

        if blob.exists():
            return "file"
        else:
            prefix = cloud_path.key
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            # not a file, see if it is a directory
            f = self.client.list_blobs(bucket, max_results=1, prefix=prefix)

            # at least one key with the prefix of the directory
            if bool(list(f)):
                return "dir"
            else:
                return None

    def _exists(self, cloud_path: GSPath) -> bool:
        return self._is_file_or_dir(cloud_path) in ["file", "dir"]

    def _list_dir(self, cloud_path: GSPath, recursive=False) -> Iterable[GSPath]:
        pass

    def _move_file(self, src: GSPath, dst: GSPath) -> GSPath:
        pass

    def _remove(self, cloud_path: GSPath) -> None:
        pass

    def _upload_file(self, local_path: Union[str, os.PathLike], cloud_path: GSPath) -> GSPath:
        bucket = self.client.bucket(cloud_path.bucket)
        blob = bucket.blob(cloud_path.key)

        blob.upload_from_filename(str(local_path))
        return cloud_path


GSClient.GSPath = GSClient.CloudPath  # type: ignore
