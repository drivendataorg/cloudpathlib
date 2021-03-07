# AnyPath Polymorphic Class

`cloudpathlib` implements a special `AnyPath` polymorphic class. This class will automatically instantiate a cloud path instance or a `pathlib.Path` instance appropriately from your input. It's also a virtual superclass of `CloudPath` and `Path`, so `isinstance` and `issubclass` checks will work in the expected way.

This functionality can be handy for situations when you want to support both local filepaths and cloud storage filepaths. If you use `AnyPath`, your code can switch between them seamlessly based on the contents of provided filepaths with needing any `if`-`else` conditional blocks.

## Example

```python
from cloudpathlib import AnyPath

path = AnyPath("mydir/myfile.txt")
path

cloud_path = AnyPath("s3://mybucket/myfile.txt")
cloud_path

isinstance(path, AnyPath)
isinstance(cloud_path, AnyPath)
```

---
<sup>Examples created with [reprexlite](https://github.com/jayqi/reprexlite)</sup>
