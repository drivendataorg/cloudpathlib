import collections
from datetime import datetime
from pathlib import Path, PurePosixPath
import shutil
from tempfile import TemporaryDirectory

from boto3.session import Session
from botocore.exceptions import ClientError

from .utils import delete_empty_parents_up_to_root

TEST_ASSETS = Path(__file__).parent.parent / "assets"
DEFAULT_S3_BUCKET_NAME = "bucket"

# Since we don't contol exactly when the filesystem finishes writing a file
# and the test files are super small, we can end up with race conditions in
# the tests where the updated file is modified before the source file,
# which breaks our caching logic

NoSuchKey = Session().client("s3").exceptions.NoSuchKey


def mocked_session_class_factory(test_dir: str):
    class MockBoto3Session:
        def __init__(self, *args, **kwargs):
            # copy test assets for reference in tests without affecting assets
            self.tmp = TemporaryDirectory()
            self.tmp_path = Path(self.tmp.name) / "test_case_copy"
            shutil.copytree(TEST_ASSETS, self.tmp_path / test_dir)

            self.metadata_cache = {}

        def __del__(self):
            self.tmp.cleanup()

        def resource(self, item, endpoint_url, config=None):
            return MockBoto3Resource(self.tmp_path, session=self)

        def client(self, item, endpoint_url, config=None):
            return MockBoto3Client(self.tmp_path, session=self)

    return MockBoto3Session


class MockBoto3Resource:
    def __init__(self, root, session=None):
        self.root = root
        self.download_config = None
        self.upload_config = None
        self.session = session

    def Bucket(self, bucket):
        return MockBoto3Bucket(self.root, session=self.session)

    def ObjectSummary(self, bucket, key):
        return MockBoto3ObjectSummary(self.root, key, session=self.session)

    def Object(self, bucket, key):
        return MockBoto3Object(self.root, key, self)


class MockBoto3Object:
    def __init__(self, root, path, resource):
        self.root = root
        self.path = root / path
        self.resource = resource

    def get(self):
        if not self.path.exists() or self.path.is_dir():
            raise NoSuchKey({}, {})
        else:
            return {"key": str(PurePosixPath(self.path))}

    def load(self):
        if not self.path.exists() or self.path.is_dir():
            raise ClientError({}, {})
        else:
            return {"key": str(PurePosixPath(self.path))}

    @property
    def key(self):
        return str(PurePosixPath(self.path).relative_to(PurePosixPath(self.root)))

    def copy_from(self, CopySource=None, Metadata=None, MetadataDirective=None):
        if CopySource["Key"] == str(self.path.relative_to(self.root)):
            # same file, touch
            self.path.touch()
        else:
            self.path.write_bytes((self.root / Path(CopySource["Key"])).read_bytes())

    def download_file(self, to_path, Config=None, ExtraArgs=None):
        to_path = Path(to_path)

        to_path.parent.mkdir(parents=True, exist_ok=True)

        to_path.write_bytes(self.path.read_bytes())
        # track config to make sure it's used in tests
        self.resource.download_config = Config
        self.resource.download_extra_args = ExtraArgs

    def upload_file(self, from_path, Config=None, ExtraArgs=None):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_bytes(Path(from_path).read_bytes())
        self.resource.upload_config = Config

        if ExtraArgs is not None:
            self.resource.session.metadata_cache[self.path] = ExtraArgs.pop("ContentType", None)

    def delete(self):
        self.path.unlink()
        delete_empty_parents_up_to_root(self.path, self.root)
        return {"ResponseMetadata": {"HTTPStatusCode": 204}}

    def copy(self, source, ExtraArgs=None, Config=None):
        # boto3 is more like "copy from"
        source = self.root / source["Key"]
        self.path.parent.mkdir(parents=True, exist_ok=True)

        return shutil.copy(str(source), str(self.path))


class MockBoto3ObjectSummary:
    def __init__(self, root, path, session=None):
        self.path = root / path
        self.session = session

    def get(self):
        if not self.path.exists() or self.path.is_dir():
            raise NoSuchKey({}, {})
        else:
            return {
                "LastModified": datetime.fromtimestamp(self.path.stat().st_mtime),
                "ContentLength": None,
                "ETag": hash(str(self.path)),
                "ContentType": self.session.metadata_cache.get(self.path, None),
                "Metadata": {},
            }


class MockBoto3Bucket:
    def __init__(self, root, session=None):
        self.root = root
        self.session = session

    @property
    def objects(self):
        return MockObjects(self.root, session=self.session)


class MockObjects:
    def __init__(self, root, session=None):
        self.root = root
        self.session = session

    def filter(self, Prefix=""):
        path = self.root / Prefix

        if path.is_file():
            return MockCollection([PurePosixPath(path)], self.root, session=self.session)

        items = [
            PurePosixPath(f)
            for f in path.glob("**/*")
            if f.is_file() and not f.name.startswith(".")
        ]
        return MockCollection(items, self.root, session=self.session)


class MockCollection:
    def __init__(self, items, root, session=None):
        self.root = root
        self.session = session
        s3_obj = collections.namedtuple("s3_obj", "key bucket_name")

        self.full_paths = items
        self.s3_obj_paths = [
            s3_obj(bucket_name=DEFAULT_S3_BUCKET_NAME, key=str(i.relative_to(self.root)))
            for i in items
        ]

    def __iter__(self):
        return iter(self.s3_obj_paths)

    def limit(self, n):
        return self.s3_obj_paths[:n]

    def delete(self):
        any_deleted = False
        for p in self.full_paths:
            if Path(p).exists():
                any_deleted = True
            Path(p).unlink()
            delete_empty_parents_up_to_root(Path(p), self.root)

        if not any_deleted:
            return []
        return [{"ResponseMetadata": {"HTTPStatusCode": 200}}]


class MockBoto3Client:
    def __init__(self, root, session=None):
        self.root = root
        self.session = session

    def get_paginator(self, api):
        return MockBoto3Paginator(self.root, session=self.session)

    def head_bucket(self, Bucket):
        if Bucket == DEFAULT_S3_BUCKET_NAME:  # used in passing tests
            return {"Bucket": Bucket}
        else:
            raise ClientError(
                {
                    "Error": {
                        "Message": f"Bucket {Bucket} not expected as mock bucket; only '{DEFAULT_S3_BUCKET_NAME}' exists."
                    }
                },
                {},
            )

    def list_buckets(self):
        return {"Buckets": [{"Name": DEFAULT_S3_BUCKET_NAME}]}

    def head_object(self, Bucket, Key, **kwargs):
        if (
            not (self.root / Key).exists()
            or (self.root / Key).is_dir()
            or Bucket != DEFAULT_S3_BUCKET_NAME
        ):
            raise ClientError({}, {})
        else:
            return {"key": Key}

    def generate_presigned_url(self, op: str, Params: dict, ExpiresIn: int):
        mock_presigned_url = f"https://{Params['Bucket']}.s3.amazonaws.com/{Params['Key']}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=TEST%2FTEST%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240131T194721Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=TEST"
        return mock_presigned_url

    @property
    def exceptions(self):
        Ex = collections.namedtuple("Ex", "NoSuchKey")
        return Ex(NoSuchKey=NoSuchKey)


class MockBoto3Paginator:
    def __init__(self, root, per_page=2, session=None):
        self.root = root
        self.per_page = per_page
        self.session = session

    def paginate(self, Bucket=None, Prefix="", Delimiter=None):
        new_dir = self.root / Prefix

        if Delimiter == "/":
            items = [f for f in new_dir.iterdir() if not f.name.startswith(".")]
        else:
            items = [f for f in new_dir.rglob("*") if not f.name.startswith(".")]

        for ix in range(0, len(items), self.per_page):
            page = items[ix : ix + self.per_page]
            dirs = [
                {"Prefix": str(_.relative_to(self.root).as_posix())} for _ in page if _.is_dir()
            ]
            files = [
                {
                    "Key": str(_.relative_to(self.root).as_posix()),
                    "Size": (
                        123
                        if not _.relative_to(self.root).exists()
                        else _.relative_to(self.root).stat().st_size
                    ),
                }
                for _ in page
                if _.is_file()
            ]

            # s3 can have "fake" directories where size is 0, but it is listed in "Contents" (see #198)
            # add one in here for testing
            if dirs:
                fake_dir = dirs.pop(-1)
                fake_dir["Size"] = 0
                fake_dir["Key"] = fake_dir.pop("Prefix") + "/"  # fake dirs have '/' appended
                files.append(fake_dir)

            yield {"CommonPrefixes": dirs, "Contents": files}
