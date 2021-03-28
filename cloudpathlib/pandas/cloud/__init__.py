try:
    from pandas_path.accessor import register_path_accessor
except ImportError:
    raise ImportError("To use the .cloud accessor, you must pip install pandas-path.")

from ...cloudpath import CloudPath


register_path_accessor("cloud", CloudPath)
