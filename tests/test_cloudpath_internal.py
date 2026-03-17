from pathlib import PurePosixPath

from cloudpathlib.local import LocalS3Client
from cloudpathlib.cloudpath import _resolve


def test_resolve():
    assert _resolve(PurePosixPath("/a/b/c")) == "/a/b/c"
    assert _resolve(PurePosixPath("/a/b/c/")) == "/a/b/c"
    assert _resolve(PurePosixPath("/a/b/c//")) == "/a/b/c"
    assert _resolve(PurePosixPath("//a/b/c//")) == "/a/b/c"
    assert _resolve(PurePosixPath("a/b/c")) == "/a/b/c"
    assert _resolve(PurePosixPath("a/b/c/")) == "/a/b/c"
    assert _resolve(PurePosixPath("a//b/c")) == "/a/b/c"
    assert _resolve(PurePosixPath("a////b/c")) == "/a/b/c"
    assert _resolve(PurePosixPath("a/b/./c")) == "/a/b/c"
    assert _resolve(PurePosixPath("a/b/../c")) == "/a/c"
    assert _resolve(PurePosixPath("a/b/../../c")) == "/c"
    assert _resolve(PurePosixPath("")) == "/"


def test_lazy_path_and_url_properties_are_cached():
    p = LocalS3Client().CloudPath("s3://bucket/dir/file.txt")

    first_path = p._path
    first_url = p._url

    assert first_path is p._path
    assert first_url is p._url


def test_glob_prefilter_and_include_dirs_choices():
    client = LocalS3Client()
    root = client.CloudPath("s3://bucket/glob-choices")
    (root / "nested").mkdir(parents=True, exist_ok=True)
    (root / "nested" / "one.txt").write_text("1")
    (root / "nested" / "two.csv").write_text("2")

    calls = []
    original = client._list_dir_raw

    def wrapped(*args, **kwargs):
        calls.append(kwargs.copy())
        yield from original(*args, **kwargs)

    client._list_dir_raw = wrapped
    try:
        list(root.glob("**/*.txt"))
        list(root.glob("nested/*"))
        list(root.glob("**/*.txt", case_sensitive=False))
    finally:
        client._list_dir_raw = original

    assert calls[0]["recursive"] is True
    assert calls[0]["include_dirs"] is False
    assert calls[0]["prefilter_pattern"] == "**/*.txt"

    assert calls[1]["recursive"] is True
    assert calls[1]["include_dirs"] is True
    assert calls[1]["prefilter_pattern"] is None

    assert calls[2]["recursive"] is True
    assert calls[2]["include_dirs"] is False
    assert calls[2]["prefilter_pattern"] is None


def test_glob_dir_only_patterns():
    client = LocalS3Client()
    root = client.CloudPath("s3://bucket/glob-dir-only")
    (root / "alpha").mkdir(parents=True, exist_ok=True)
    (root / "alpha" / "file.txt").write_text("alpha")
    (root / "beta.txt").write_text("beta")

    slash_pattern = sorted(str(p) for p in root.glob("*/"))
    deep_dir_pattern = sorted(str(p) for p in root.glob("**"))

    assert slash_pattern == [f"{root}/alpha/"]
    assert f"{root}/alpha/" in deep_dir_pattern
    assert all(not p.endswith("beta.txt") for p in deep_dir_pattern)
