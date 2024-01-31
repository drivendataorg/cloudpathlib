from pathlib import PurePosixPath
import sys

import pytest

from cloudpathlib import CloudPath


def test_properties(rig):
    assert rig.create_cloud_path("a/b/c/d").name == "d"
    assert rig.create_cloud_path("a/b/c/d.file").name == "d.file"

    assert rig.create_cloud_path("a/b/c/d").stem == "d"
    assert rig.create_cloud_path("a/b/c/d.file").stem == "d"

    assert rig.create_cloud_path("a/b/c/d").suffix == ""
    assert rig.create_cloud_path("a/b/c/d.file").suffix == ".file"

    assert rig.create_cloud_path("a/b/c/d").suffixes == []
    assert rig.create_cloud_path("a/b/c/d.tar").suffixes == [".tar"]
    assert rig.create_cloud_path("a/b/c/d.tar.gz").suffixes == [".tar", ".gz"]


def test_with_suffix(rig):
    assert (
        str(rig.create_cloud_path("a/b/c/d.file").with_suffix(".png"))
        == f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/a/b/c/d.png"
    )


def test_with_stem(rig):
    assert (
        str(rig.create_cloud_path("a/b/c/old.file").with_stem("new"))
        == f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/a/b/c/new.file"
    )


def test_no_op_actions(rig):
    path = rig.create_cloud_path("a/b/c/d.file")
    assert path == path.absolute()
    assert path == path.resolve()
    assert path == path.resolve(strict=True)
    assert path.is_absolute()


def test_relative_to(rig, azure_rig, gs_rig):
    assert rig.create_cloud_path("bucket/path/to/file.txt").relative_to(
        rig.create_cloud_path("bucket/path")
    ) == PurePosixPath("to/file.txt")
    with pytest.raises(ValueError):
        assert rig.create_cloud_path("bucket/b/c/d.file").relative_to(
            rig.create_cloud_path("bucket/z")
        )

    if sys.version_info >= (3, 12):
        assert rig.create_cloud_path("bucket/path/to/file.txt").relative_to(
            rig.create_cloud_path("other_bucket/path2"), walk_up=True
        ) == PurePosixPath("../../bucket/path/to/file.txt")

    with pytest.raises(ValueError):
        assert rig.create_cloud_path("a/b/c/d.file").relative_to(PurePosixPath("/a/b/c"))
    other_rig = azure_rig if rig.cloud_prefix != azure_rig.cloud_prefix else gs_rig
    path = CloudPath(f"{rig.cloud_prefix}bucket/path/to/file.txt")
    other_cloud_path = CloudPath(f"{other_rig.cloud_prefix}bucket/path")
    with pytest.raises(ValueError):
        assert path.relative_to(other_cloud_path)

    assert rig.create_cloud_path("a/b/c/d.file").is_relative_to(rig.create_cloud_path("a/b/"))
    assert not rig.create_cloud_path("a/b/c/d.file").is_relative_to(rig.create_cloud_path("b/c"))
    assert not rig.create_cloud_path("a/b/c/d.file").is_relative_to(PurePosixPath("/a/b/c"))
    assert not path.is_relative_to(other_cloud_path)


def test_joins(rig):
    assert rig.create_cloud_path("a") / "b" == rig.create_cloud_path("a/b")
    assert rig.create_cloud_path("a") / PurePosixPath("b") == rig.create_cloud_path("a/b")
    assert rig.create_cloud_path("a/b/c/d") / "../../b" == rig.create_cloud_path("a/b/b")

    assert rig.create_cloud_path("a/b/c/d").match("**/c/*")
    assert not rig.create_cloud_path("a/b/c/d").match("**/c")
    assert rig.create_cloud_path("a/b/c/d").match("a/*/c/d")

    if sys.version_info >= (3, 12):
        assert rig.create_cloud_path("a/b/c/d").match("A/*/C/D", case_sensitive=False)

    assert rig.create_cloud_path("a/b/c/d").anchor == rig.cloud_prefix
    assert rig.create_cloud_path("a/b/c/d").parent == rig.create_cloud_path("a/b/c")

    assert rig.create_cloud_path("a/b/c/d").parents == (
        rig.create_cloud_path("a/b/c"),
        rig.create_cloud_path("a/b"),
        rig.create_cloud_path("a"),
        rig.path_class(f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}"),
        rig.path_class(f"{rig.cloud_prefix}{rig.drive}"),
    )

    assert rig.create_cloud_path("a").joinpath("b", "c") == rig.create_cloud_path("a/b/c")
    assert rig.create_cloud_path("a").joinpath(PurePosixPath("b"), "c") == rig.create_cloud_path(
        "a/b/c"
    )

    assert rig.create_cloud_path("a/b/c").samefile(rig.create_cloud_path("a/b/c"))

    assert (
        rig.create_cloud_path("a/b/c").as_uri()
        == f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/a/b/c"
    )

    assert rig.create_cloud_path("a/b/c/d").parts == (
        rig.cloud_prefix,
        rig.drive,
        rig.test_dir,
        "a",
        "b",
        "c",
        "d",
    )


def test_with_segments(rig):
    assert rig.create_cloud_path("a/b/c/d").with_segments(
        "x", "y", "z"
    ) == rig.client_class().CloudPath(f"{rig.cloud_prefix}x/y/z")


def test_is_junction(rig):
    assert not rig.create_cloud_path("a/b/foo").is_junction()


def test_equality(rig):
    assert rig.create_cloud_path("a/b/foo") == rig.create_cloud_path("a/b/foo")
    assert hash(rig.create_cloud_path("a/b/foo")) == hash(rig.create_cloud_path("a/b/foo"))

    assert rig.create_cloud_path("a/b/foo") != rig.create_cloud_path("a/b/bar")
    assert hash(rig.create_cloud_path("a/b/foo")) != hash(rig.create_cloud_path("a/b/bar"))

    cp = rig.create_cloud_path("a/b/foo")
    assert cp != str(cp)
    assert cp != repr(cp)
    assert hash(cp) != hash(str(cp))
    assert hash(cp) != hash(repr(cp))


def test_sorting(rig):
    cp1 = rig.create_cloud_path("a/b/c")
    cp2 = rig.create_cloud_path("a/c/b")
    assert cp1 < cp2
    assert cp1 <= cp2
    assert not cp1 > cp2
    assert not cp1 >= cp2

    assert cp2 > cp1
    assert cp2 >= cp1
    assert not cp2 < cp1
    assert not cp2 <= cp1

    assert rig.create_cloud_path("a/b/c") <= rig.create_cloud_path("a/b/c")
    assert rig.create_cloud_path("a/b/c") >= rig.create_cloud_path("a/b/c")

    assert sorted(
        [
            rig.create_cloud_path("a/c/b"),
            rig.create_cloud_path("a/b/c"),
            rig.create_cloud_path("d/e/f"),
        ]
    ) == [
        rig.create_cloud_path("a/b/c"),
        rig.create_cloud_path("a/c/b"),
        rig.create_cloud_path("d/e/f"),
    ]

    with pytest.raises(TypeError):
        assert cp1 < str(cp1)
    with pytest.raises(TypeError):
        assert cp1 <= str(cp1)
    with pytest.raises(TypeError):
        assert cp1 > str(cp1)
    with pytest.raises(TypeError):
        assert cp1 >= str(cp1)
