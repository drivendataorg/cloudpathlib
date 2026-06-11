from datetime import datetime, timezone
import http
import os
import re
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path
from typing import Iterable, Optional, Tuple, Union, Callable
import shutil
import mimetypes
import warnings

from cloudpathlib.client import Client, register_client_class
from cloudpathlib.enums import FileCacheMode

from .httppath import HttpPath


@register_client_class("http")
class HttpClient(Client):
    def __init__(
        self,
        file_cache_mode: Optional[Union[str, FileCacheMode]] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
        content_type_method: Optional[Callable] = mimetypes.guess_type,
        auth: Optional[urllib.request.BaseHandler] = None,
        custom_list_page_parser: Optional[Callable[[str], Iterable[str]]] = None,
        custom_dir_matcher: Optional[Callable[[str], bool]] = None,
        write_file_http_method: Optional[str] = "PUT",
    ):
        """Class constructor. Creates an HTTP client that can be used to interact with HTTP servers
            using the cloudpathlib library.

        Args:
            file_cache_mode (Optional[Union[str, FileCacheMode]]): How often to clear the file cache; see
                [the caching docs](https://cloudpathlib.drivendata.org/stable/caching/) for more information
                about the options in cloudpathlib.eums.FileCacheMode.
            local_cache_dir (Optional[Union[str, os.PathLike]]): Path to directory to use as cache
                for downloaded files. If None, will use a temporary directory. Default can be set with
                the `CLOUDPATHLIB_LOCAL_CACHE_DIR` environment variable.
            content_type_method (Optional[Callable]): Function to call to guess media type (mimetype) when
                uploading files. Defaults to `mimetypes.guess_type`.
            auth (Optional[urllib.request.BaseHandler]): Authentication handler to use for the client. Defaults to None, which will use the default handler.
            custom_list_page_parser (Optional[Callable[[str], Iterable[str]]]): Function to call to parse pages that list directories. Defaults to looking for `<a>` tags with `href`.
            custom_dir_matcher (Optional[Callable[[str], bool]]): Function to call to identify a url that is a directory. Defaults to a lambda that checks if the path ends with a `/`.
            write_file_http_method (Optional[str]): HTTP method to use when writing files. Defaults to "PUT", but some servers may want "POST".
        """
        super().__init__(file_cache_mode, local_cache_dir, content_type_method)
        self.auth = auth

        if self.auth is None:
            self.opener = urllib.request.build_opener()
        else:
            self.opener = urllib.request.build_opener(self.auth)

        self.custom_list_page_parser = custom_list_page_parser

        self.dir_matcher = (
            custom_dir_matcher if custom_dir_matcher is not None else lambda x: x.endswith("/")
        )

        self.write_file_http_method = write_file_http_method

    def _get_metadata(self, cloud_path: HttpPath) -> dict:
        with self.opener.open(cloud_path.as_url()) as response:
            last_modified = response.headers.get("Last-Modified", None)

            if last_modified is not None:
                # per https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified
                last_modified = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")

                # should always be utc https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified#gmt
                last_modified = last_modified.replace(tzinfo=timezone.utc)

            return {
                "size": int(response.headers.get("Content-Length", 0)),
                "last_modified": last_modified,
                "content_type": response.headers.get("Content-Type", None),
            }

    def _is_file_or_dir(self, cloud_path: HttpPath) -> Optional[str]:
        if self.dir_matcher(cloud_path.as_url()):
            return "dir"
        else:
            return "file"

    def _download_file(self, cloud_path: HttpPath, local_path: Union[str, os.PathLike]) -> Path:
        local_path = Path(local_path)
        with self.opener.open(cloud_path.as_url()) as response:
            # Ensure parent directory exists before opening file
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with local_path.open("wb") as out_file:
                shutil.copyfileobj(response, out_file)
        return local_path

    def _exists(self, cloud_path: HttpPath) -> bool:
        request = urllib.request.Request(cloud_path.as_url(), method="HEAD")
        try:
            with self.opener.open(request) as response:
                return response.status == 200
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            if isinstance(e, urllib.error.URLError) or e.code == 404:
                return False
            raise

    def _move_file(self, src: HttpPath, dst: HttpPath, remove_src: bool = True) -> HttpPath:
        # .fspath will download the file so the local version can be uploaded
        self._upload_file(src.fspath, dst)
        if remove_src:
            try:
                self._remove(src)
            except Exception as e:
                warnings.warn(
                    f"File was successfully uploaded to {dst} but failed to remove original {src}: {e}",
                    UserWarning,
                )
                raise
        return dst

    def _remove(self, cloud_path: HttpPath, missing_ok: bool = True) -> None:
        request = urllib.request.Request(cloud_path.as_url(), method="DELETE")
        try:
            with self.opener.open(request) as response:
                if response.status != 204:
                    raise Exception(f"Failed to delete {cloud_path}.")
        except urllib.error.HTTPError as e:
            if e.code == 404 and missing_ok:
                pass
            else:
                raise FileNotFoundError(f"Failed to delete {cloud_path}.")

    def _list_dir(self, cloud_path: HttpPath, recursive: bool) -> Iterable[Tuple[HttpPath, bool]]:
        try:
            with self.opener.open(cloud_path.as_url()) as response:
                # Parse the directory listing
                for path, is_dir in self._parse_list_dir_response(
                    response.read().decode(), base_url=str(cloud_path)
                ):
                    yield path, is_dir

                    # If it's a directory and recursive is True, list the contents of the directory
                    if recursive and is_dir:
                        yield from self._list_dir(path, recursive=True)

        except Exception as e:  # noqa E722
            raise NotImplementedError(
                f"Unable to parse response as a listing of files; please provide a custom parser as `custom_list_page_parser`. Error raised: {e}"
            )

    def _upload_file(self, local_path: Union[str, os.PathLike], cloud_path: HttpPath) -> HttpPath:
        local_path = Path(local_path)
        if self.content_type_method is not None:
            content_type, _ = self.content_type_method(local_path)

        headers = {"Content-Type": content_type or "application/octet-stream"}

        with local_path.open("rb") as file_data:
            request = urllib.request.Request(
                cloud_path.as_url(),
                data=file_data.read(),
                method=self.write_file_http_method,
                headers=headers,
            )
            with self.opener.open(request) as response:
                if response.status != 201 and response.status != 200:
                    raise Exception(f"Failed to upload {local_path} to {cloud_path}.")
        return cloud_path

    def _get_public_url(self, cloud_path: HttpPath) -> str:
        return cloud_path.as_url()

    def _generate_presigned_url(self, cloud_path: HttpPath, expire_seconds: int = 60 * 60) -> str:
        raise NotImplementedError("Presigned URLs are not supported using urllib.")

    def _parse_list_dir_response(
        self, response: str, base_url: str
    ) -> Iterable[Tuple[HttpPath, bool]]:
        # Ensure base_url ends with a trailing slash so joining works
        if not base_url.endswith("/"):
            base_url += "/"

        def _simple_links(html: str) -> Iterable[str]:
            return re.findall(r'<a\s+href="([^"]+)"', html)

        parser: Callable[[str], Iterable[str]] = (
            self.custom_list_page_parser
            if self.custom_list_page_parser is not None
            else _simple_links
        )

        yield from (
            (self.CloudPath((urllib.parse.urljoin(base_url, match))), self.dir_matcher(match))
            for match in parser(response)
        )

    def request(
        self, url: HttpPath, method: str, **kwargs
    ) -> Tuple[http.client.HTTPResponse, bytes]:
        request = urllib.request.Request(url.as_url(), method=method, **kwargs)
        with self.opener.open(request) as response:
            # eager read of response content, which is not available after
            # the connection is closed when we exit the context manager.
            return response, response.read()

    # ====================== STREAMING I/O METHODS ======================

    def _range_download(self, cloud_path: "HttpPath", start: int, end: int) -> bytes:
        """Download a byte range from HTTP.

        Verifies the response is 206 Partial Content. If the server returns 200
        (ignoring the Range header), slices the full body locally so callers
        always receive exactly the requested bytes.
        """
        headers = {"Range": f"bytes={start}-{end}"}
        request = urllib.request.Request(str(cloud_path), headers=headers)
        try:
            with self.opener.open(request) as response:
                status = response.status
                data = response.read()
                if status == 206:
                    return data
                elif status == 200:
                    # Server ignored the Range header; slice locally
                    return data[start : end + 1]
                else:
                    raise OSError(f"Unexpected status {status} for range request on {cloud_path}")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise FileNotFoundError(f"HTTP resource not found: {cloud_path}")
            elif e.code == 416:  # Range not satisfiable
                return b""
            raise

    def _get_content_length(self, cloud_path: "HttpPath") -> int:
        """Get the size of an HTTP resource."""
        request = urllib.request.Request(str(cloud_path), method="HEAD")
        try:
            with self.opener.open(request) as response:
                content_length = response.headers.get("Content-Length")
                if content_length:
                    return int(content_length)
                raise ValueError(f"HTTP resource does not provide Content-Length: {cloud_path}")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise FileNotFoundError(f"HTTP resource not found: {cloud_path}")
            raise

    def _initiate_multipart_upload(self, cloud_path: "HttpPath") -> str:
        """HTTP uploads are single-shot PUT; no session needed."""
        return ""

    def _upload_part(
        self, cloud_path: "HttpPath", upload_id: str, part_number: int, data: bytes
    ) -> dict:
        """HTTP does not support true multipart; use _put_data for a single PUT."""
        raise NotImplementedError(
            "HTTP uses a single PUT for uploads; multipart is not supported. "
            "Use _put_data instead."
        )

    def _complete_multipart_upload(
        self, cloud_path: "HttpPath", upload_id: str, parts: list
    ) -> None:
        """HTTP does not support true multipart; use _put_data for a single PUT."""
        raise NotImplementedError(
            "HTTP uses a single PUT for uploads; multipart is not supported."
        )

    def _abort_multipart_upload(self, cloud_path: "HttpPath", upload_id: str) -> None:
        """Nothing to abort for HTTP single-PUT uploads."""
        pass

    def _put_data(self, cloud_path: "HttpPath", data: bytes) -> None:
        """Upload data to HTTP server using a PUT request.

        Uses self.opener so that any SSL context or auth handlers configured on
        this client are applied (important for HttpsClient with self-signed certs).
        """
        url = str(cloud_path)
        request = urllib.request.Request(url, data=data, method="PUT")
        request.add_header("Content-Type", "application/octet-stream")
        request.add_header("Content-Length", str(len(data)))

        try:
            with self.opener.open(request) as response:
                if response.status not in (200, 201, 204):
                    raise OSError(
                        f"HTTP PUT failed with status {response.status}: {response.reason}"
                    )
        except urllib.error.HTTPError as e:
            if e.code == 405:  # Method Not Allowed
                raise NotImplementedError(f"HTTP server does not support PUT requests for {url}")
            raise OSError(f"HTTP PUT failed: {e}")


HttpClient.HttpPath = HttpClient.CloudPath  # type: ignore


@register_client_class("https")
class HttpsClient(HttpClient):
    pass


HttpsClient.HttpsPath = HttpsClient.CloudPath  # type: ignore
