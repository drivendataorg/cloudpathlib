# Integrations with Other Libraries

## Pydantic

!!! warning

    As of version `0.15.1`, `cloudpathlib` is not compatible with
    version `2.0.0` or greater of Pydantic. If you want to annotate
    your pydantic models with cloudpathlib types, you need to use
    `pydantic<2`.

`cloudpathlib` integrates with [Pydantic](https://pydantic-docs.helpmanual.io/)'s data validation. You can declare fields with cloud path classes, and Pydantic's validation mechanisms will run inputs through the cloud path's constructor.

```python
from cloudpathlib import S3Path
from pydantic import BaseModel

class MyModel(BaseModel):
    s3_file: S3Path

inst = MyModel(s3_file="s3://mybucket/myfile.txt")
inst.s3_file
#> S3Path('s3://mybucket/myfile.txt')
```

This also works with the `AnyPath` polymorphic class. Inputs will get dispatched and instantiated as the appropriate class.

```python
from cloudpathlib import AnyPath
from pydantic import BaseModel

class FancyModel(BaseModel):
    path: AnyPath

fancy1 = FancyModel(path="s3://mybucket/myfile.txt")
fancy1.path
#> S3Path('s3://mybucket/myfile.txt')

fancy2 = FancyModel(path="mydir/myfile.txt")
fancy2.path
#> PosixPath('mydir/myfile.txt')
```

---
<sup>Examples created with [reprexlite](https://github.com/jayqi/reprexlite)</sup>
