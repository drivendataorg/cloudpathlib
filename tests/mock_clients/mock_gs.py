from datetime import datetime, timedelta
import os
from pathlib import Path, PurePosixPath
import shutil
from tempfile import TemporaryDirectory

import urllib.parse
from uuid import uuid4
from xml.etree import ElementTree

from google.api_core.exceptions import NotFound, RequestRangeNotSatisfiable
import requests

from .utils import delete_empty_parents_up_to_root

TEST_ASSETS = Path(__file__).parent.parent / "assets"
DEFAULT_GS_BUCKET_NAME = "bucket"


def mocked_client_class_factory(test_dir: str):
    class MockClient:
        def __init__(self, *args, **kwargs):
            # copy test assets for reference in tests without affecting assets
            self.tmp = TemporaryDirectory()
            self.tmp_path = Path(self.tmp.name) / "test_case_copy"
            shutil.copytree(TEST_ASSETS, self.tmp_path / test_dir)

            self.metadata_cache = {}
            self._connection = _MockConnection()
            self._http = MockMPUTransport(self)

        @classmethod
        def create_anonymous_client(cls):
            return cls()

        @classmethod
        def from_service_account_json(cls, *args, **kwargs):
            return cls()

        def __del__(self):
            self.tmp.cleanup()

        def bucket(self, bucket):
            return MockBucket(self.tmp_path, bucket, client=self)

        def list_buckets(self):
            return [DEFAULT_GS_BUCKET_NAME]

        def get_bucket(self, bucket):
            return MockBucket(self.tmp_path, bucket, client=self)

    return MockClient


class MockBlob:
    def __init__(self, root, name, client=None):
        self.bucket = root
        self.name = str(PurePosixPath(name))
        self.metadata = None
        self.client = client

    def delete(self):
        path = self.bucket / self.name
        path.unlink()
        delete_empty_parents_up_to_root(path=path, root=self.bucket)

    def download_to_filename(self, filename, timeout=None, retry=None):
        # if timeout is not None, assume that the test wants a timeout and throw it
        if timeout is not None:
            raise TimeoutError("Download timed out")

        # indicate that retry object made it through to the GS lib
        if retry is not None:
            retry.mocked_retries = 1

        from_path = self.bucket / self.name

        to_path = Path(filename)
        to_path.parent.mkdir(exist_ok=True, parents=True)
        to_path.write_bytes(from_path.read_bytes())

    def download_as_bytes(self, start=None, end=None, timeout=None, retry=None):
        """Download blob content as bytes with optional byte range."""
        # if timeout is not None, assume that the test wants a timeout and throw it
        if timeout is not None:
            raise TimeoutError("Download timed out")

        # indicate that retry object made it through to the GS lib
        if retry is not None:
            retry.mocked_retries = 1

        from_path = self.bucket / self.name
        if not (from_path.exists() and from_path.is_file()):
            raise NotFound(f"blob not found: {self.name}")
        data = from_path.read_bytes()

        # real GCS rejects ranges starting past EOF (an end past EOF is clamped)
        if start is not None and start >= len(data):
            raise RequestRangeNotSatisfiable("The requested range is not satisfiable")

        # Handle byte range if specified
        if start is not None:
            if end is not None:
                return data[start : end + 1]
            else:
                return data[start:]
        return data

    def patch(self):
        if "updated" in self.metadata:
            (self.bucket / self.name).touch()

    def reload(
        self,
        client=None,
        projection="noAcl",
        if_etag_match=None,
        if_etag_not_match=None,
        if_generation_match=None,
        if_generation_not_match=None,
        if_metageneration_match=None,
        if_metageneration_not_match=None,
        timeout=None,
        retry=None,
    ):
        path = self.bucket / self.name
        if not (path.exists() and path.is_file()):
            raise NotFound(f"blob not found: {self.name}")

    def upload_from_filename(self, filename, content_type=None, timeout=None, retry=None):
        # if timeout is not None, assume that the test wants a timeout and throw it
        if timeout is not None:
            raise TimeoutError("Upload timed out")

        # indicate that retry object made it through to the GS lib
        if retry is not None:
            retry.mocked_retries = 1

        data = Path(filename).read_bytes()
        path = self.bucket / self.name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

        self.client.metadata_cache[self.bucket / self.name] = content_type

    def upload_from_string(self, data, content_type=None, timeout=None, retry=None):
        """Upload from bytes/string data."""
        # if timeout is not None, assume that the test wants a timeout and throw it
        if timeout is not None:
            raise TimeoutError("Upload timed out")

        # indicate that retry object made it through to the GS lib
        if retry is not None:
            retry.mocked_retries = 1

        path = self.bucket / self.name
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(data, str):
            path.write_text(data)
        else:
            path.write_bytes(data)

        self.client.metadata_cache[self.bucket / self.name] = content_type

    @property
    def etag(self):
        return "etag"

    @property
    def md5_hash(self):
        return os.environ.get("MOCK_EXPECTED_MD5_HASH", "md5_hash")

    @property
    def size(self):
        path = self.bucket / self.name
        return path.stat().st_size

    @property
    def updated(self):
        path = self.bucket / self.name
        return datetime.fromtimestamp(path.stat().st_mtime)

    @property
    def content_type(self):
        return self.client.metadata_cache.get(self.bucket / self.name, None)

    @property
    def public_url(self) -> str:
        return f"https://storage.googleapis.com{self.bucket}/{self.name}"

    def generate_signed_url(self, version: str, expiration: timedelta, method: str):
        return f"https://storage.googleapis.com{self.bucket}/{self.name}?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=TEST&X-Goog-Date=20240131T185515Z&X-Goog-Expires=3600&X-Goog-SignedHeaders=host&X-Goog-Signature=TEST"


class MockBucket:
    def __init__(self, name, bucket_name, client=None):
        self.name = name
        self.bucket_name = bucket_name
        self.client = client

    def blob(self, blob):
        return MockBlob(self.name, blob, client=self.client)

    def copy_blob(self, blob, destination_bucket, new_name, timeout=None, retry=None):
        # if timeout is not None, assume that the test wants a timeout and throw it
        if timeout is not None:
            raise TimeoutError("Copy timed out")

        # indicate that retry object made it through to the GS lib
        if retry is not None:
            retry.mocked_retries = 1

        data = (self.name / blob.name).read_bytes()
        dst = destination_bucket.name / new_name
        dst.parent.mkdir(exist_ok=True, parents=True)
        dst.write_bytes(data)

    def exists(self):
        if self.bucket_name == DEFAULT_GS_BUCKET_NAME:  # name used by passing tests
            return True
        else:
            return False

    def get_blob(self, blob):
        if (self.name / blob).is_file():
            return MockBlob(self.name, blob, client=self.client)
        else:
            return None

    def list_blobs(self, max_results=None, prefix=None, delimiter=None):
        path = self.name if prefix is None else self.name / prefix
        pattern = "**/*" if delimiter is None else "*"
        blobs, prefixes = [], []
        for item in path.glob(pattern):
            if not item.name.startswith("."):
                if item.is_file():
                    blobs.append(
                        MockBlob(self.name, item.relative_to(self.name), client=self.client)
                    )
                else:
                    prefixes.append(str(item.relative_to(self.name).as_posix()))

        # bucket name for passing tests
        if self.bucket_name == DEFAULT_GS_BUCKET_NAME:
            return MockHTTPIterator(blobs, prefixes, max_results)
        else:
            raise NotFound(
                f"Bucket {self.name} not expected as mock bucket; only '{DEFAULT_GS_BUCKET_NAME}' exists."
            )


class MockHTTPIterator:
    def __init__(self, blobs, sub_directories, max_results):
        self.blobs = blobs
        self.sub_directories = sub_directories
        self.max_results = max_results

    def __iter__(self):
        if self.max_results is None:
            return iter(self.blobs)
        else:
            return iter(self.blobs[: self.max_results])

    def __next__(self):
        yield from iter(self)

    @property
    def prefixes(self):
        return self.sub_directories


class MockTransferManager:
    @staticmethod
    def download_chunks_concurrently(
        blob,
        filename,
        chunk_size=32 * 1024 * 1024,
        download_kwargs=None,
        deadline=None,
        worker_type="process",
        max_workers=8,
        *,
        crc32c_checksum=True,
    ):
        blob.download_to_filename(filename)


def mock_default_auth():
    return "fake-credentials", "fake-default-project"


class _MockConnection:
    """Just enough of google.cloud.storage._http.Connection for the XML MPU URL."""

    API_BASE_URL = "https://storage.googleapis.com"


def _mpu_response(status, headers=None, body=b""):
    response = requests.Response()
    response.status_code = status
    response.headers.update(headers or {})
    response._content = body if isinstance(body, bytes) else body.encode()
    return response


class MockMPUTransport:
    """Fake authorized session implementing the GCS XML multipart-upload API."""

    _XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

    def __init__(self, client):
        self.client = client
        self.uploads = {}

    def request(self, method, url, data=None, headers=None, **kwargs):
        parsed = urllib.parse.urlsplit(url)
        query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        # url path is /{bucket}/{blob}; the mock stores blobs under tmp_path directly
        _, _, blob = parsed.path.lstrip("/").partition("/")
        blob = urllib.parse.unquote(blob)

        if method == "POST" and "uploads" in query:
            upload_id = uuid4().hex
            self.uploads[upload_id] = {
                "blob": blob,
                "parts": {},
                "content_type": (headers or {}).get("content-type"),
            }
            body = (
                f'<InitiateMultipartUploadResult xmlns="{self._XMLNS}">'
                f"<UploadId>{upload_id}</UploadId></InitiateMultipartUploadResult>"
            )
            return _mpu_response(200, body=body)

        upload_id = query.get("uploadId")
        if upload_id not in self.uploads:
            return _mpu_response(404, body="NoSuchUpload")

        if method == "PUT" and "partNumber" in query:
            part_number = int(query["partNumber"])
            etag = f'"mock-etag-{part_number}"'
            self.uploads[upload_id]["parts"][part_number] = (etag, bytes(data))
            return _mpu_response(200, headers={"etag": etag})

        if method == "POST":
            upload = self.uploads.pop(upload_id)
            root = ElementTree.fromstring(data)
            parts = []
            for part_element in root.findall("Part"):
                part_number = int(part_element.find("PartNumber").text)
                etag = part_element.find("ETag").text
                stored_etag, part_data = upload["parts"][part_number]
                assert etag == stored_etag, "ETag mismatch in CompleteMultipartUpload"
                parts.append(part_data)
            # real GCS enforces a 5 MiB minimum for all non-final parts at finalize
            if any(len(part_data) < 5 * 1024 * 1024 for part_data in parts[:-1]):
                return _mpu_response(400, body="<Error><Code>EntityTooSmall</Code></Error>")
            content = b"".join(parts)
            target = self.client.tmp_path / upload["blob"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            if upload["content_type"] is not None:
                self.client.metadata_cache[self.client.tmp_path / upload["blob"]] = upload[
                    "content_type"
                ]
            return _mpu_response(200, body="<CompleteMultipartUploadResult/>")

        if method == "DELETE":
            self.uploads.pop(upload_id, None)
            return _mpu_response(204)

        return _mpu_response(400, body="Unsupported mock MPU request")
