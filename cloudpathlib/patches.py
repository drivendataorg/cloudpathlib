import os

from .cloudpath import CloudPath


def _cloudpath_open(*args, **kwargs):
    if isinstance(args[0], CloudPath):
        return args[0].open(*args[1:], **kwargs)
    else:
        return open(*args, **kwargs)


def patch_open():
    open = _cloudpath_open


def _dispatch_to_pathlib(path, pathlib_func, os_func, pathlib_args=None, pathlib_kwargs=None, *args, **kwargs):
    if pathlib_args is None:
        pathlib_args = args

    if pathlib_kwargs is None:
        pathlib_kwargs = kwargs

    if isinstance(path, CloudPath):
        return pathlib_func(path, *pathlib_args, **pathlib_kwargs)
    else:
        return os_func(*args, **kwargs)


def _cloudpath_os_listdir(path="."):
    return _dispatch_to_pathlib(path, lambda path: list(path.iterdir()), os.listdir, path=path)


def _cloudpath_os_lstat(path, *, dir_fd=None):
    return _dispatch_to_pathlib(path, CloudPath.stat, os.lstat, path, dir_fd=dir_fd)

def _cloudpath_os_mkdir(path, mode=0o777, *, dir_fd=None):
    return _dispatch_to_pathlib(path, CloudPath.mkdir, os.mkdir, path, dir_fd=dir_fd)

def _cloudpath_os_makedirs(name, mode=0o777, exist_ok=False):
    pass

def _cloudpath_os_remove(path, *, dir_fd=None):
    pass

def _cloudpath_os_removedirs(name):
    pass

def _cloudpath_os_rename(src, dst, *, src_dir_fd=None, dst_dir_fd=None):
    pass

def _cloudpath_os_renames(old, new):
    pass

def _cloudpath_os_replace(src, dst, *, src_dir_fd=None, dst_dir_fd=None):
    pass

def _cloudpath_os_rmdir(path, *, dir_fd=None):
    pass

def _cloudpath_os_scandir(path='.'):
    pass

def _cloudpath_os_stat(path, *, dir_fd=None, follow_symlinks=True):
    if isinstance(path, CloudPath):
        return path.stat()
    else:
        return os.stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)

def _cloudpath_os_unlink(path, *, dir_fd=None):
    pass

def _cloudpath_os_walk(top, topdown=True, onerror=None, followlinks=False):
    pass

def _cloudpath_os_path_basename(path):
    pass

def _cloudpath_os_path_exists(path):
    pass

def _cloudpath_os_path_getatime(path):
    pass

def _cloudpath_os_path_getmtime(path):
    pass

def _cloudpath_os_path_getctime(path):
    pass

def _cloudpath_os_path_getsize(path):
    pass

def _cloudpath_os_path_isfile(path):
    pass

def _cloudpath_os_path_isdir(path):
    pass

def _cloudpath_os_path_join(path, *paths):
    pass

def _cloudpath_os_path_split(path):
    pass

def _cloudpath_os_path_splitext(path):
    pass


def patch_os_function():
    os.listdir = _cloudpath_os_listdir

