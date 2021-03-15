# Integrations with Other Libraries

## Pydantic

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


## Pandas and [pandas-path](https://github.com/drivendataorg/pandas-path)

[`pandas-path`](https://github.com/drivendataorg/pandas-path) provides `pathlib` functions through as custom accessor (`.path`). The library also supports registering custom accessors for any class that implements that pathlib API.

### The `.cloud` accessor

We expose a `.cloud` accessor on `pandas.Series` and `pandas.Index` objects if you import it. This allows you to access any `CloudPath` method or property directly on a Series by just adding `.cloud`.

To use the `.cloud` accessor, you must have `pandas-path` installed through pip:

```
pip install pandas-path
```

All you need to do to register the accessor is `from cloudpathlib.pandas import cloud`.

For example:

```python
from cloudpathlib.pandas import cloud
import pandas as pd

pd.Series([
    's3://cats/1.jpg',
    's3://cats/2.jpg',
    's3://dogs/1.jpg',
    's3://dogs/2.jpg',
]).cloud.bucket
#> 0    cats
#> 1    cats
#> 2    dogs
#> 3    dogs
#> dtype: object
```

The way `pandas-path` works, it converts the items in the Series from strings to `CloudPath` objects and then back to strings again before returning so you don't end up with Python complex objects in your DataFrame. Because of this, you should set the default client if you need to pass any parameters to the client.

For example, let's say that the account `special_account` had access to `special_bucket` but that the credentials were not accessible through an environment variable or credentials file.

```python
from cloudpathlib import S3Client
from cloudpathlib.pandas import cloud

import pandas as pd

# default client will get used by `.cloud` accessor if we set ahead of time
client = S3Client(aws_access_key_id="special_account", aws_secret_access_key="special_key")
client.set_as_default_client()

pd.Series([
    's3://special_bucket/cats/1.jpg',
    's3://special_bucket/cats/2.jpg',
    's3://special_bucket/dogs/1.jpg',
    's3://special_bucket/dogs/2.jpg',
]).cloud.key
#> 0    cats/1.jpg
#> 1    cats/2.jpg
#> 2    dogs/1.jpg
#> 3    dogs/2.jpg
#> dtype: object
```

---
<sup>Examples created with [reprexlite](https://github.com/jayqi/reprexlite)</sup>
