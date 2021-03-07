from pathlib import Path

from pydantic import BaseModel, ValidationError
import pytest

from cloudpathlib.anypath import AnyPath


def test_pydantic_cloudpath(rig):
    class PydanticModel(BaseModel):
        cloud_path: rig.path_class

    cp = rig.create_cloud_path("a/b/c")

    obj = PydanticModel(cloud_path=cp)
    assert obj.cloud_path == cp

    obj = PydanticModel(cloud_path=str(cp))
    assert obj.cloud_path == cp

    with pytest.raises(ValidationError):
        _ = PydanticModel(cloud_path=0)


def test_pydantic_anypath(rig):
    class PydanticModel(BaseModel):
        any_path: AnyPath

    cp = rig.create_cloud_path("a/b/c")

    obj = PydanticModel(any_path=cp)
    assert obj.any_path == cp

    obj = PydanticModel(any_path=str(cp))
    assert obj.any_path == cp

    obj = PydanticModel(any_path=Path("a/b/c"))
    assert obj.any_path == Path("a/b/c")

    obj = PydanticModel(any_path="a/b/c")
    assert obj.any_path == Path("a/b/c")

    with pytest.raises(ValidationError):
        obj = PydanticModel(any_path=0)
