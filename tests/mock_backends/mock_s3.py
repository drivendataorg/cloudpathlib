import collections
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
import shutil

from boto3.session import Session

from .utils import delete_empty_parents_up_to_root

TEST_ASSETS = Path(__file__).parent.parent / "assets"

# Since we don't contol exactly when the filesystem finishes writing a file
# and the test files are super small, we can end up with race conditions in
# the tests where the updated file is modified before the source file,
# which breaks our caching logic
WRITE_SLEEP_BUFFER = 0.1

NoSuchKey = Session().client("s3").exceptions.NoSuchKey


class MockBoto3Session:
    def __init__(self):
        # copy test assets for reference in tests without affecting assets
        self.tmp = TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name) / "test_case_copy"
        shutil.copytree(TEST_ASSETS, self.tmp_path)

    def __del__(self):
        self.tmp.cleanup()

    def resource(self, item):
        return MockBoto3Resource(self.tmp_path)

    def client(self, item):
        return MockBoto3Client(self.tmp_path)


class MockBoto3Resource:
    def __init__(self, root):
        self.root = root

    def Bucket(self, bucket):
        return MockBoto3Bucket(self.root)

    def ObjectSummary(self, bucket, key):
        return MockBoto3ObjectSummary(self.root, key)

    def Object(self, bucket, key):
        return MockBoto3Object(self.root, key)


class MockBoto3Object:
    def __init__(self, root, path):
        self.root = root
        self.path = root / path

    def get(self):
        if not self.path.exists() or self.path.is_dir():
            raise NoSuchKey({}, {})
        else:
            return {"key": self.path}

    def copy_from(self, CopySource=None, Metadata=None, MetadataDirective=None):
        if CopySource["Key"] == str(self.path.relative_to(self.root)):
            # same file, touch
            self.path.touch()
        else:
            sleep(WRITE_SLEEP_BUFFER)
            self.path.write_bytes((self.root / Path(CopySource["Key"])).read_bytes)

    def download_file(self, to_path):
        to_path = Path(to_path)
        sleep(WRITE_SLEEP_BUFFER)
        to_path.write_bytes(self.path.read_bytes())

    def upload_file(self, from_path):
        sleep(WRITE_SLEEP_BUFFER)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_bytes(Path(from_path).read_bytes())

    def delete(self):
        self.path.unlink()
        delete_empty_parents_up_to_root(self.path, self.root)
        return {"ResponseMetadata": {"HTTPStatusCode": 204}}

    def copy(self, source):
        # boto3 is more like "copy from"
        source = self.root / source["Key"]
        self.path.parent.mkdir(parents=True, exist_ok=True)

        return shutil.copy(str(source), str(self.path))


class MockBoto3ObjectSummary:
    def __init__(self, root, path):
        self.path = root / path

    def get(self):
        if not self.path.exists() or self.path.is_dir():
            raise NoSuchKey({}, {})
        else:
            return {
                "LastModified": datetime.fromtimestamp(self.path.stat().st_mtime),
                "ContentLength": None,
                "ETag": hash(str(self.path)),
                "ContentType": None,
                "Metadata": {},
            }


class MockBoto3Bucket:
    def __init__(self, root):
        self.root = root

    @property
    def objects(self):
        return MockObjects(self.root)


class MockObjects:
    def __init__(self, root):
        self.root = root

    def filter(self, Prefix=""):
        path = self.root / Prefix
        items = [f for f in path.glob("**/*") if f.is_file() and not f.name.startswith(".")]
        return MockCollection(items, self.root)


class MockCollection:
    def __init__(self, items, root):
        self.root = root
        s3_obj = collections.namedtuple("s3_obj", "key bucket_name")

        self.full_paths = items
        self.s3_obj_paths = [
            s3_obj(bucket_name="bucket", key=str(i.relative_to(self.root))) for i in items
        ]

    def __iter__(self):
        return iter(self.s3_obj_paths)

    def limit(self, n):
        return self.s3_obj_paths[:n]

    def delete(self):
        for p in self.full_paths:
            p.unlink()
            delete_empty_parents_up_to_root(p, self.root)

        return [{"ResponseMetadata": {"HTTPStatusCode": 200}}]


class MockBoto3Client:
    def __init__(self, root):
        self.root = root

    def get_paginator(self, api):
        return MockBoto3Paginator(self.root)

    @property
    def exceptions(self):
        Ex = collections.namedtuple("Ex", "NoSuchKey")
        return Ex(NoSuchKey=NoSuchKey)


class MockBoto3Paginator:
    def __init__(self, root, per_page=2):
        self.root = root
        self.per_page = per_page

    def paginate(self, Bucket=None, Prefix="", Delimiter=None):
        new_dir = self.root / Prefix
        items = []
        for f in new_dir.iterdir():
            if not f.name.startswith("."):
                items.append(f)

        for ix in range(0, len(items), self.per_page):
            page = items[ix : ix + self.per_page]
            dirs = [{"Prefix": str(_.relative_to(self.root))} for _ in page if _.is_dir()]
            files = [{"Key": str(_.relative_to(self.root))} for _ in page if _.is_file()]
            yield {"CommonPrefixes": dirs, "Contents": files}
