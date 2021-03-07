# AnyPath Polymorphic Class

`cloudpathlib` implements a special `AnyPath` polymorphic class. This class will automatically instantiate a cloud path instance or a `pathlib.Path` instance appropriately from your input. It's also a virtual superclass of `CloudPath` and `Path`, so `isinstance` and `issubclass` checks will work in the expected way.

This functionality can be handy for situations when you want to support both local filepaths and cloud storage filepaths. If you use `AnyPath`, your code can switch between them seamlessly based on the contents of provided filepaths with needing any `if`-`else` conditional blocks.

## Example

```python
from cloudpathlib import AnyPath

path = AnyPath("mydir/myfile.txt")
path
#> PosixPath('mydir/myfile.txt')

cloud_path = AnyPath("s3://mybucket/myfile.txt")
cloud_path
#> S3Path('s3://mybucket/myfile.txt')

isinstance(path, AnyPath)
#> True
isinstance(cloud_path, AnyPath)
#> True
```

## How It Works

The constructor for `AnyPath` will first attempt to run the input through the `CloudPath` base class' constructor, which will validate the input against registered concrete `CloudPath` implementations. This will accept inputs that are already a cloud path class or a string with the appropriate URI scheme prefix (e.g., `s3://`). If no implementation validates successfully, it will then try to run the input through the `Path` constructor. If the `Path` constructor fails and raises a `TypeError`, then the `AnyPath` constructor will raise an `AnyPathTypeError` exception.

The virtual superclass functionality with `isinstance` and `issubclass` with the `__instancecheck__` and `__subclasscheck__` special methods per [PEP 3119](https://www.python.org/dev/peps/pep-3119/#overloading-isinstance-and-issubclass)'s specification.

---
<sup>Examples created with [reprexlite](https://github.com/jayqi/reprexlite)</sup>
