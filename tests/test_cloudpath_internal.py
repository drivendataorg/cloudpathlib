from pathlib import PurePosixPath

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
