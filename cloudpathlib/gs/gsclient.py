from datetime import datetime, timedelta
from functools import lru_cache
import mimetypes
import os
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Dict, Iterable, Optional, TYPE_CHECKING, Tuple, Union, Sequence
import warnings

from ..client import Client, _UploadPart, register_client_class
from ..cloudpath import implementation_registry
from ..enums import FileCacheMode
from ..exceptions import CloudPathFileNotFoundError, CloudPathNotImplementedError
from .gspath import GSPath

try:
    if TYPE_CHECKING:
        from google.auth.credentials import Credentials
        from google.api_core.retry import Retry

    from google.api_core.exceptions import NotFound as GCSNotFound
    from google.auth import default as google_default_auth
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud.storage.client import Client as StorageClient

except ModuleNotFoundError:
    implementation_registry["gs"].dependencies_loaded = False
    GCSNotFound = Exception  # type: ignore[misc, assignment]  # fallback so name is always defined


try:
    import google.cloud.storage.transfer_manager as transfer_manager
except ImportError:
    transfer_manager = None  # type: ignore[assignment]

try:
    from google.cloud.storage._media.requests import XMLMPUContainer, XMLMPUPart
except ImportError:
    try:
        # older google-cloud-storage versions expose these via google-resumable-media
        from google.resumable_media.requests import (  # type: ignore[assignment,no-redef]
            XMLMPUContainer,
            XMLMPUPart,
        )
    except ImportError:
        XMLMPUContainer = None  # type: ignore[assignment,misc]
        XMLMPUPart = None  # type: ignore[assignment,misc]


@lru_cache(maxsize=1)
def _bytes_mpu_part_class() -> type:
    """XMLMPUPart subclass that uploads an in-memory payload instead of a file slice."""

    class _BytesXMLMPUPart(XMLMPUPart):
        def __init__(self, upload_url, upload_id, data, part_number, headers=None):
            super().__init__(
                upload_url,
                upload_id,
                filename="",
                start=0,
                end=len(data),
                part_number=part_number,
                headers=headers,
                checksum=None,
            )
            self._data = bytes(data)

        def _prepare_upload_request(self):
            if self.finished:
                raise ValueError("This part has already been uploaded.")
            query = f"?partNumber={self._part_number}&uploadId={self._upload_id}"
            return "PUT", self.upload_url + query, self._data, self._headers

    return _BytesXMLMPUPart


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
        streaming_max_concurrency: int = 1,
        download_chunks_concurrently_kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        retry: Optional["Retry"] = None,
    ):
        """Class constructor. Sets up a [`Storage
        Client`](https://googleapis.dev/python/storage/latest/client.html).
        Supports, in this order, the following authentication methods of `Storage Client`.

        - Instantiated and already authenticated `Storage Client`.
        - OAuth2 Credentials object and a project name.
        - File path to a JSON credentials file for a Google service account.
        - Google Cloud SDK default credentials. See [How Application Default Credentials works](https://cloud.google.com/docs/authentication/application-default-credentials)

        If no authentication methods are used,
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
            streaming_max_concurrency (int): Maximum concurrent requests per open streaming
                stream (background part uploads and read prefetch) when using
                `FileCacheMode.streaming`; defaults to 1 (sequential).
            download_chunks_concurrently_kwargs (Optional[Dict[str, Any]]): Keyword arguments to pass to
                [`download_chunks_concurrently`](https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.transfer_manager#google_cloud_storage_transfer_manager_download_chunks_concurrently)
                for sliced parallel downloads; Only available in `google-cloud-storage` version 2.7.0 or later, otherwise ignored and a warning is emitted.
            timeout (Optional[float]): Cloud Storage [timeout value](https://cloud.google.com/python/docs/reference/storage/1.39.0/retry_timeout)
            retry (Optional[google.api_core.retry.Retry]): Cloud Storage [retry configuration](https://cloud.google.com/python/docs/reference/storage/1.39.0/retry_timeout#configuring-retries)
        """
        # don't check `GOOGLE_APPLICATION_CREDENTIALS` since `google_default_auth` already does that
        # use explicit client
        if storage_client is not None:
            self.client = storage_client
        # use explicit credentials
        elif credentials is not None:
            self.client = StorageClient(credentials=credentials, project=project)
        # use explicit credential file
        elif application_credentials is not None:
            self.client = StorageClient.from_service_account_json(application_credentials)
        # use default credentials based on SDK precedence
        else:
            try:
                # use `google_default_auth` instead of `StorageClient()` since it
                # handles precedence of creds in different locations properly
                credentials, default_project = google_default_auth()
                project = project or default_project  # use explicit project if present
                self.client = StorageClient(credentials=credentials, project=project)
            except DefaultCredentialsError:
                self.client = StorageClient.create_anonymous_client()

        self.download_chunks_concurrently_kwargs = download_chunks_concurrently_kwargs
        self.blob_kwargs: dict[str, Any] = {}
        if timeout is not None:
            self.timeout: float = timeout
            self.blob_kwargs["timeout"] = self.timeout
        if retry is not None:
            self.retry: Retry = retry
            self.blob_kwargs["retry"] = self.retry

        super().__init__(
            local_cache_dir=local_cache_dir,
            content_type_method=content_type_method,
            file_cache_mode=file_cache_mode,
            streaming_max_concurrency=streaming_max_concurrency,
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
                "md5_hash": blob.md5_hash,
            }

    def _download_file(self, cloud_path: GSPath, local_path: Union[str, os.PathLike]) -> Path:
        bucket = self.client.bucket(cloud_path.bucket)
        blob = bucket.get_blob(cloud_path.blob)

        local_path = Path(local_path)
        if transfer_manager is not None and self.download_chunks_concurrently_kwargs is not None:
            transfer_manager.download_chunks_concurrently(
                blob, local_path, **self.download_chunks_concurrently_kwargs
            )
        else:
            if transfer_manager is None and self.download_chunks_concurrently_kwargs is not None:
                warnings.warn(
                    "Ignoring `download_chunks_concurrently_kwargs` for version of google-cloud-storage that does not support them (<2.7.0)."
                )

            blob.download_to_filename(local_path, **self.blob_kwargs)

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
            return self.client.bucket(cloud_path.bucket).exists()

        return self._is_file_or_dir(cloud_path) in ["file", "dir"]

    def _list_dir(self, cloud_path: GSPath, recursive=False) -> Iterable[Tuple[GSPath, bool]]:
        # shortcut if listing all available buckets
        if not cloud_path.bucket:
            if recursive:
                raise NotImplementedError(
                    "Cannot recursively list all buckets and contents; you can get all the buckets then recursively list each separately."
                )

            yield from (
                (self.CloudPath(f"{cloud_path.cloud_prefix}{str(b)}"), True)
                for b in self.client.list_buckets()
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
                            self.CloudPath(
                                f"{cloud_path.cloud_prefix}{cloud_path.bucket}/{prefix}{parent}"
                            ),
                            True,  # is a directory
                        )
                        yielded_dirs.add(parent)
                yield (
                    self.CloudPath(f"{cloud_path.cloud_prefix}{cloud_path.bucket}/{o.name}"),
                    False,
                )  # is a file
        else:
            iterator = bucket.list_blobs(delimiter="/", prefix=prefix)

            # files must be iterated first for `.prefixes` to be populated:
            #   see: https://github.com/googleapis/python-storage/issues/863
            for file in iterator:
                yield (
                    self.CloudPath(f"{cloud_path.cloud_prefix}{cloud_path.bucket}/{file.name}"),
                    False,  # is a file
                )

            for directory in iterator.prefixes:
                yield (
                    self.CloudPath(f"{cloud_path.cloud_prefix}{cloud_path.bucket}/{directory}"),
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
            src_bucket.copy_blob(src_blob, dst_bucket, dst.blob, **self.blob_kwargs)

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

        blob.upload_from_filename(str(local_path), **extra_args, **self.blob_kwargs)
        return cloud_path

    def _get_public_url(self, cloud_path: GSPath) -> str:
        bucket = self.client.get_bucket(cloud_path.bucket)
        blob = bucket.blob(cloud_path.blob)
        return blob.public_url

    def _generate_presigned_url(self, cloud_path: GSPath, expire_seconds: int = 60 * 60) -> str:
        bucket = self.client.get_bucket(cloud_path.bucket)
        blob = bucket.blob(cloud_path.blob)
        url = blob.generate_signed_url(
            version="v4", expiration=timedelta(seconds=expire_seconds), method="GET"
        )
        return url

    def _range_download(self, cloud_path: GSPath, start: int, end: int) -> bytes:
        """Download a byte range from GCS."""
        blob = self.client.bucket(cloud_path.bucket).blob(cloud_path.blob)
        try:
            return blob.download_as_bytes(start=start, end=end, **self.blob_kwargs)
        except GCSNotFound:
            raise CloudPathFileNotFoundError(f"GCS object not found: {cloud_path}")
        except Exception as e:
            # match a range-past-EOF error structurally (status 416) rather than by
            # substring, so unrelated errors are not silently treated as EOF
            status = getattr(e, "code", None)
            if status is None:
                status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 416 or "Requested Range Not Satisfiable" in str(e):
                return b""
            raise

    def _get_content_length(self, cloud_path: GSPath) -> int:
        """Get the size of a GCS object."""
        blob = self.client.bucket(cloud_path.bucket).blob(cloud_path.blob)
        try:
            blob.reload(**self.blob_kwargs)
            return blob.size
        except GCSNotFound:
            raise CloudPathFileNotFoundError(f"GCS object not found: {cloud_path}")

    def _mpu_url(self, cloud_path: GSPath) -> str:
        """XML API URL for a multipart upload of this object."""
        from urllib.parse import quote

        connection = self.client._connection
        hostname = (
            connection.get_api_base_url_for_mtls()
            if hasattr(connection, "get_api_base_url_for_mtls")
            else connection.API_BASE_URL
        )
        return f"{hostname}/{cloud_path.bucket}/{quote(cloud_path.blob)}"

    def _initiate_multipart_upload(self, cloud_path: GSPath) -> str:
        """Start a GCS XML multipart upload, threading content type and encoding."""
        if XMLMPUContainer is None:
            raise CloudPathNotImplementedError(
                "Streaming writes require google-cloud-storage with XML multipart support."
            )
        content_type = None
        headers = {}
        if self.content_type_method is not None:
            content_type, content_encoding = self.content_type_method(str(cloud_path))
            if content_encoding is not None:
                headers["Content-Encoding"] = content_encoding
        container = XMLMPUContainer(self._mpu_url(cloud_path), cloud_path.blob, headers=headers)
        container.initiate(
            transport=self.client._http, content_type=content_type or "application/octet-stream"
        )
        return container.upload_id

    def _upload_part(
        self, cloud_path: GSPath, upload_id: str, part_number: int, data: bytes
    ) -> _UploadPart:
        """Upload one part of a GCS XML multipart upload."""
        part = _bytes_mpu_part_class()(self._mpu_url(cloud_path), upload_id, data, part_number)
        part.upload(self.client._http)
        return {"part_number": part_number, "etag": part.etag}

    def _complete_multipart_upload(
        self, cloud_path: GSPath, upload_id: str, parts: Sequence[_UploadPart]
    ) -> None:
        """Finalize a GCS XML multipart upload."""
        container = XMLMPUContainer(
            self._mpu_url(cloud_path), cloud_path.blob, upload_id=upload_id
        )
        for part in parts:
            container.register_part(part["part_number"], part["etag"])
        container.finalize(self.client._http)

    def _abort_multipart_upload(self, cloud_path: GSPath, upload_id: str) -> None:
        """Cancel a GCS XML multipart upload, discarding uploaded parts."""
        container = XMLMPUContainer(
            self._mpu_url(cloud_path), cloud_path.blob, upload_id=upload_id
        )
        container.cancel(self.client._http)

    def _put_empty_object(self, cloud_path: GSPath) -> None:
        """Upload a zero-byte GCS object."""
        blob = self.client.bucket(cloud_path.bucket).blob(cloud_path.blob)
        blob.upload_from_string(b"", **self.blob_kwargs)


GSClient.GSPath = GSClient.CloudPath  # type: ignore
