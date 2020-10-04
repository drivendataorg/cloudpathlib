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
