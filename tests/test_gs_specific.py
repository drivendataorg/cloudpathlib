import pytest

from cloudpathlib import GSPath
from cloudpathlib.local import LocalGSPath


@pytest.mark.parametrize("path_class", [GSPath, LocalGSPath])
def test_gspath_properties(path_class):
    p = path_class("gs://mybucket")
    assert p.blob == ""
    assert p.bucket == "mybucket"

    p2 = path_class("gs://mybucket/")
    assert p2.blob == ""
    assert p2.bucket == "mybucket"
