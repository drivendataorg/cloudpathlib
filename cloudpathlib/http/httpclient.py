from datetime import datetime
import os
import re
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path
from typing import Iterable, Optional, Tuple, Union, Callable
import shutil
import mimetypes
import urllib.response

import pytz

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
    ):
        super().__init__(file_cache_mode, local_cache_dir, content_type_method)
        self.auth = auth

        if self.auth is None:
            self.opener = urllib.request.build_opener()
        else:
            self.openener = urllib.request.build_opener(self.auth)

        self.custom_list_page_parser = custom_list_page_parser

    def _get_metadata(self, cloud_path: HttpPath) -> dict:
        with self.opener.open(cloud_path.as_url()) as response:
            last_modified = response.headers.get("Last-Modified", None)

            if last_modified is not None:
                # per https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified
                last_modified = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")

                # should always be utc https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified#gmt
                last_modified = last_modified.replace(tzinfo=pytz.UTC)

            return {
                "size": int(response.headers.get("Content-Length", 0)),
                "last_modified": last_modified,
                "content_type": response.headers.get("Content-Type", None),
            }

    def _download_file(self, cloud_path: HttpPath, local_path: Union[str, os.PathLike]) -> Path:
        local_path = Path(local_path)
        with self.opener.open(cloud_path.as_url()) as response:
            with open(local_path, "wb") as out_file:
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
        self._upload_file(src, dst)
        if remove_src:
            self._remove(src)
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

        except:  # noqa E722
            raise NotImplementedError(
                "Unable to parse response as a listing of files; please provide a custom parser as `custom_list_page_parser`."
            )

    def _upload_file(self, local_path: Union[str, os.PathLike], cloud_path: HttpPath) -> HttpPath:
        local_path = Path(local_path)
        if self.content_type_method is not None:
            content_type, _ = self.content_type_method(local_path)

        headers = {"Content-Type": content_type or "application/octet-stream"}

        with open(local_path, "rb") as file_data:
            request = urllib.request.Request(
                cloud_path.as_url(), data=file_data.read(), method="PUT", headers=headers
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
            (self.CloudPath((urllib.parse.urljoin(base_url, match))), Path(match).suffix == "")
            for match in parser(response)
        )

    def request(self, url: HttpPath, method: str, **kwargs) -> None:
        request = urllib.request.Request(url.as_url(), method=method, **kwargs)
        with self.opener.open(request) as response:
            return response


HttpClient.HttpPath = HttpClient.CloudPath  # type: ignore
