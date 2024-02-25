import builtins
from contextlib import contextmanager
import glob
import os
import os.path

from cloudpathlib.exceptions import InvalidGlobArgumentsError

from .cloudpath import CloudPath


def _check_first_arg(*args, **kwargs):
    return isinstance(args[0], CloudPath)


def _check_first_arg_first_index(*args, **kwargs):
    return isinstance(args[0][0], CloudPath)


def _check_first_arg_or_root_dir(*args, **kwargs):
    return isinstance(args[0], CloudPath) or isinstance(kwargs.get("root_dir", None), CloudPath)


def _patch_factory(original_version, cpl_version, cpl_check=_check_first_arg):
    _original = original_version

    def _patched_version(*args, **kwargs):
        if cpl_check(*args, **kwargs):
            return cpl_version(*args, **kwargs)
        else:
            return _original(*args, **kwargs)

    original_version = _patched_version
    return _patched_version


@contextmanager
def patch_open():
    patched = _patch_factory(
        builtins.open,
        CloudPath.open,
    )
    original_open = builtins.open
    builtins.open = patched

    original_fspath = CloudPath.__fspath__
    CloudPath.__fspath__ = (
        lambda x: x
    )  # turn off `fspath` -> str since we patch everything to handle CloudPath

    try:
        yield patched
    finally:
        builtins.open = original_open
        CloudPath.__fspath__ = original_fspath


def _cloudpath_fspath(path):
    return path  # no op, since methods should all handle cloudpaths when patched


def _cloudpath_os_listdir(path="."):
    return list(path.iterdir())


def _cloudpath_lstat(path, *, dir_fd=None):
    return path.stat()


def _cloudpath_mkdir(path, *, dir_fd=None):
    return path.mkdir()


def _cloudpath_os_makedirs(name, mode=0o777, exist_ok=False):
    return CloudPath.mkdir(name, parents=True, exist_ok=exist_ok)


def _cloudpath_os_remove(path, *, dir_fd=None):
    return path.unlink()


def _cloudpath_os_removedirs(name):
    for d in name.parents:
        d.rmdir()


def _cloudpath_os_rename(src, dst, *, src_dir_fd=None, dst_dir_fd=None):
    return src.rename(dst)


def _cloudpath_os_renames(old, new):
    old.rename(new)  # move file
    _cloudpath_os_removedirs(old)  # remove previous directories if empty


def _cloudpath_os_replace(src, dst, *, src_dir_fd=None, dst_dir_fd=None):
    return src.rename(dst)


def _cloudpath_os_rmdir(path, *, dir_fd=None):
    return path.rmdir()


def _cloudpath_os_scandir(path="."):
    return path.iterdir()


def _cloudpath_os_stat(path, *, dir_fd=None, follow_symlinks=True):
    return path.stat()


def _cloudpath_os_unlink(path, *, dir_fd=None):
    return path.unlink()


def _cloudpath_os_walk(top, topdown=True, onerror=None, followlinks=False):
    try:
        dirs, files = [], []
        for p in top.iterdir():
            dirs.append(p) if p.is_dir() else files.append(p)

        if topdown:
            yield (top, files, dirs)

        for d in dirs:
            yield from _cloudpath_os_walk(d, topdown=topdown, onerror=onerror)

        if not topdown:
            yield (top, files, dirs)

    except Exception as e:
        if onerror is not None:
            onerror(e)
        else:
            raise


def _cloudpath_os_path_basename(path):
    return path.name


def __common(parts):
    i = 0

    try:
        while all(item[i] == parts[0][i] for item in parts[1:]):
            i += 1
    except IndexError:
        pass

    return parts[0][:i]


def _cloudpath_os_path_commonpath(paths):
    common = __common([p.parts for p in paths])
    return paths[0].client.CloudPath(*common)


def _cloudpath_os_path_commonprefix(list):
    common = __common([str(p) for p in list])
    return common


def _cloudpath_os_path_dirname(path):
    return path.parent


def _cloudpath_os_path_getatime(path):
    return (path.stat().st_atime,)


def _cloudpath_os_path_getmtime(path):
    return (path.stat().st_mtime,)


def _cloudpath_os_path_getctime(path):
    return (path.stat().st_ctime,)


def _cloudpath_os_path_getsize(path):
    return (path.stat().st_size,)


def _cloudpath_os_path_join(path, *paths):
    for p in paths:
        path /= p
    return path


def _cloudpath_os_path_split(path):
    return path.parent, path.name


def _cloudpath_os_path_splitext(path):
    return str(path)[: -len(path.suffix)], path.suffix


@contextmanager
def patch_os_functions():
    os_level = [
        ("fspath", os.fspath, _cloudpath_fspath),
        ("listdir", os.listdir, _cloudpath_os_listdir),
        ("lstat", os.lstat, _cloudpath_lstat),
        ("mkdir", os.mkdir, _cloudpath_mkdir),
        ("makedirs", os.makedirs, _cloudpath_os_makedirs),
        ("remove", os.remove, _cloudpath_os_remove),
        ("removedirs", os.removedirs, _cloudpath_os_removedirs),
        ("rename", os.rename, _cloudpath_os_rename),
        ("renames", os.renames, _cloudpath_os_renames),
        ("replace", os.replace, _cloudpath_os_replace),
        ("rmdir", os.rmdir, _cloudpath_os_rmdir),
        ("scandir", os.scandir, _cloudpath_os_scandir),
        ("stat", os.stat, _cloudpath_os_stat),
        ("unlink", os.unlink, _cloudpath_os_unlink),
        ("walk", os.walk, _cloudpath_os_walk),
    ]

    os_originals = {}

    for name, original, cloud in os_level:
        os_originals[name] = original
        patched = _patch_factory(original, cloud)
        setattr(os, name, patched)

    os_path_level = [
        ("basename", os.path.basename, _cloudpath_os_path_basename, _check_first_arg),
        (
            "commonpath",
            os.path.commonpath,
            _cloudpath_os_path_commonpath,
            _check_first_arg_first_index,
        ),
        (
            "commonprefix",
            os.path.commonprefix,
            _cloudpath_os_path_commonprefix,
            _check_first_arg_first_index,
        ),
        ("dirname", os.path.dirname, _cloudpath_os_path_dirname, _check_first_arg),
        ("exists", os.path.exists, CloudPath.exists, _check_first_arg),
        ("getatime", os.path.getatime, _cloudpath_os_path_getatime, _check_first_arg),
        ("getmtime", os.path.getmtime, _cloudpath_os_path_getmtime, _check_first_arg),
        ("getctime", os.path.getctime, _cloudpath_os_path_getctime, _check_first_arg),
        ("getsize", os.path.getsize, _cloudpath_os_path_getsize, _check_first_arg),
        ("isfile", os.path.isfile, CloudPath.is_file, _check_first_arg),
        ("isdir", os.path.isdir, CloudPath.is_dir, _check_first_arg),
        ("join", os.path.join, _cloudpath_os_path_join, _check_first_arg),
        ("split", os.path.split, _cloudpath_os_path_split, _check_first_arg),
        ("splitext", os.path.splitext, _cloudpath_os_path_splitext, _check_first_arg),
    ]

    os_path_originals = {}

    for name, original, cloud, check in os_path_level:
        os_path_originals[name] = original
        patched = _patch_factory(original, cloud, cpl_check=check)
        setattr(os.path, name, patched)

    try:
        yield
    finally:
        for name, original in os_originals.items():
            setattr(os, name, original)

        for name, original in os_path_originals.items():
            setattr(os.path, name, original)


def _get_root_dir_pattern_from_pathname(pathname):
    # get first wildcard
    for i, part in enumerate(pathname.parts):
        if "*" in part:
            root_parts = pathname.parts[:i]
            pattern_parts = pathname.parts[i:]
            break

    root_dir = pathname._new_cloudpath(*root_parts)
    pattern = "/".join(pattern_parts)

    return root_dir, pattern


def _cloudpath_glob_iglob(
    pathname, *, root_dir=None, dir_fd=None, recursive=False, include_hidden=False
):
    # if both are cloudpath, root_dir and pathname must share a parent, otherwise we don't know
    # where to start the pattern
    if isinstance(pathname, CloudPath) and isinstance(root_dir, CloudPath):
        if not pathname.is_relative_to(root_dir):
            raise InvalidGlobArgumentsError(
                f"If both are CloudPaths, root_dir ({root_dir}) must be a parent of pathname ({pathname})."
            )

        else:
            pattern = pathname.relative_to(root_dir)

    elif isinstance(pathname, CloudPath):
        if root_dir is not None:
            InvalidGlobArgumentsError(
                "If pathname is a CloudPath, root_dir must also be a CloudPath or None."
            )

        root_dir, pattern = _get_root_dir_pattern_from_pathname(pathname)

    elif isinstance(root_dir, CloudPath):
        pattern = pathname

    else:
        raise InvalidGlobArgumentsError(
            "At least one of pathname or root_dir must be a CloudPath."
        )

    return root_dir.glob(pattern)


def _cloudpath_glob_glob(
    pathname, *, root_dir=None, dir_fd=None, recursive=False, include_hidden=False
):
    return list(
        _cloudpath_glob_iglob(
            pathname,
            root_dir=root_dir,
            dir_fd=dir_fd,
            recursive=recursive,
            include_hidden=include_hidden,
        )
    )


@contextmanager
def patch_glob():
    original_glob = glob.glob
    glob.glob = _patch_factory(
        glob.glob, _cloudpath_glob_glob, cpl_check=_check_first_arg_or_root_dir
    )

    original_iglob = glob.iglob
    glob.iglob = _patch_factory(
        glob.iglob, _cloudpath_glob_iglob, cpl_check=_check_first_arg_or_root_dir
    )

    try:
        yield
    finally:
        glob.glob = original_glob
        glob.iglob = original_iglob
