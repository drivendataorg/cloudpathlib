import pytest


def test_joins(rig):
    assert rig.create_cloud_path("a/b/c/d").name == "d"
    assert rig.create_cloud_path("a/b/c/d.file").name == "d.file"
    assert rig.create_cloud_path("a/b/c/d.file").stem == "d"
    assert rig.create_cloud_path("a/b/c/d.file").suffix == ".file"
    assert rig.create_cloud_path("a/b/c/d.tar.gz").suffixes == [".tar", ".gz"]
    assert (
        str(rig.create_cloud_path("a/b/c/d.file").with_suffix(".png"))
        == f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/a/b/c/d.png"
    )

    assert rig.create_cloud_path("a") / "b" == rig.create_cloud_path("a/b")
    assert rig.create_cloud_path("a/b/c/d") / "../../b" == rig.create_cloud_path("a/b/b")

    assert rig.create_cloud_path("a/b/c/d").match("**/c/*")
    assert not rig.create_cloud_path("a/b/c/d").match("**/c")
    assert rig.create_cloud_path("a/b/c/d").match("a/*/c/d")

    assert rig.create_cloud_path("a/b/c/d").anchor == rig.cloud_prefix
    assert rig.create_cloud_path("a/b/c/d").parent == rig.create_cloud_path("a/b/c")

    assert rig.create_cloud_path("a/b/c/d").parents == [
        rig.create_cloud_path("a/b/c"),
        rig.create_cloud_path("a/b"),
        rig.create_cloud_path("a"),
        rig.path_class(f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}"),
        rig.path_class(f"{rig.cloud_prefix}{rig.drive}"),
    ]

    assert rig.create_cloud_path("a").joinpath("b", "c") == rig.create_cloud_path("a/b/c")

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
