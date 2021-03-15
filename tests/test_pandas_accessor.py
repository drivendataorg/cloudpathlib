from cloudpathlib.pandas import cloud  # noqa

import pandas as pd


def test_joins(rig):
    s = pd.Series(
        [
            f"{rig.cloud_prefix}bucket/a/b/c.txt",
            f"{rig.cloud_prefix}bucket/a/b/c",
            f"{rig.cloud_prefix}bucket/a/d/e.txt",
        ]
    )

    # make sure we don't register the default `path` accessor from pandas-path
    assert not hasattr(s, "path")

    # test path manipulations
    assert s.cloud.name.tolist() == ["c.txt", "c", "e.txt"]
    assert s.cloud.stem.tolist() == ["c", "c", "e"]
    assert s.cloud.parent.tolist() == [
        f"{rig.cloud_prefix}bucket/a/b",
        f"{rig.cloud_prefix}bucket/a/b",
        f"{rig.cloud_prefix}bucket/a/d",
    ]

    # test cloud specific methods
    if hasattr(rig.path_class, "bucket"):
        assert s.cloud.bucket.tolist() == ["bucket"] * 3
    elif hasattr(rig.path_class, "container"):
        assert s.cloud.container.tolist() == ["bucket"] * 3

    # test joins work as expected
    s = pd.Series(
        [
            f"{rig.cloud_prefix}bucket/a/b",
            f"{rig.cloud_prefix}bucket/a/c",
            f"{rig.cloud_prefix}bucket/a/d",
        ]
    )

    assert (s.cloud / ["file1.txt", "file2.txt", "file3.txt"]).tolist() == [
        f"{rig.cloud_prefix}bucket/a/b/file1.txt",
        f"{rig.cloud_prefix}bucket/a/c/file2.txt",
        f"{rig.cloud_prefix}bucket/a/d/file3.txt",
    ]
