"""
CloudFile module providing file-like objects for cloud storage.

This module provides the CloudFile class which implements a file-like interface
for reading and writing files from cloud storage providers. It supports streaming,
seeking, and smart caching that only downloads to disk when the entire file is read.
"""

import io
import os
from pathlib import Path
from typing import Any, BinaryIO, Optional, Union, cast
from warnings import warn

from .cloudpath import CloudPath
from .cloudstream import CloudStream
from .exceptions import (
    CloudPathException, 
    CloudPathIsADirectoryError, 
    CloudPathNotExistsError,
    CloudFileException,
    CloudFileSeekError,
    CloudFileReadError,
    CloudFileWriteError,
)


class CloudFile:
    """
    A file-like object for reading and writing cloud storage files.
    
    CloudFile provides a file-like interface for cloud storage files with the following features:
    - Streaming reads that don't require downloading the entire file
    - Seeking support for random access
    - Write support with automatic upload on flush/close
    
    Parameters
    ----------
    cloud_path : CloudPath
        The cloud path to the file
    mode : str, default "rb"
        The file mode. Supported modes: "rb", "wb", "ab", "r+b", "w+b", "a+b"
    buffering : int, default -1
        The buffer size. -1 means system default
    encoding : Optional[str], default None
        Text encoding (not used in binary mode)
    errors : Optional[str], default None
        Error handling for text mode (not used in binary mode)
    newline : Optional[str], default None
        Newline handling for text mode (not used in binary mode)
    force_overwrite_from_cloud : Optional[bool], default None
        Whether to force overwrite local cache from cloud
    force_overwrite_to_cloud : Optional[bool], default None
        Whether to force overwrite cloud from local cache
    """
    
    def __init__(
        self,
        cloud_path: CloudPath,
        mode: str = "rb",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        force_overwrite_from_cloud: Optional[bool] = None,
        force_overwrite_to_cloud: Optional[bool] = None,
    ):
        if not isinstance(cloud_path, CloudPath):
            raise TypeError("cloud_path must be a CloudPath instance")
        
        self.cloud_path = cloud_path
        self._mode = mode
        self.buffering = buffering
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self.force_overwrite_from_cloud = force_overwrite_from_cloud
        self.force_overwrite_to_cloud = force_overwrite_to_cloud
        
        # Validate mode
        if mode not in ("rb", "wb", "ab", "r+b", "w+b", "a+b"):
            raise ValueError(f"Unsupported mode: {mode}")
        
        # Check if trying to open a directory
        if cloud_path.exists() and not cloud_path.is_file():
            raise CloudPathIsADirectoryError(
                f"Cannot open directory, only files. Tried to open ({cloud_path})"
            )
        
        # Check exclusive creation mode
        if mode == "wb" and cloud_path.exists():
            raise FileExistsError(f"Cannot open existing file ({cloud_path}) for creation.")
        
        self._closed = False
        self._position = 0
        self._size = None
        self._dirty = False
        self._has_written = False
        self._init_file_state()
    
    def _init_file_state(self):
        if "r" in self._mode and self.cloud_path.exists():
            try:
                self._size = self.cloud_path.stat().st_size
            except Exception:
                self._size = None
        elif "w" in self._mode:
            self._size = 0
        elif "a" in self._mode:
            if self.cloud_path.exists():
                try:
                    self._size = self.cloud_path.stat().st_size
                    self._position = self._size
                except Exception:
                    self._size = None
            else:
                self._size = 0
                self._position = 0
    
    def read(self, size: Optional[int] = None) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if "r" not in self._mode:
            raise CloudFileReadError("File not opened for reading")
        data = self.cloud_path.client._stream_read(self.cloud_path, self._position, size)
        self._position += len(data)
        return data
    
    def readline(self, size: Optional[int] = None) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if "r" not in self._mode:
            raise CloudFileReadError("File not opened for reading")
        line = b""
        while True:
            chunk = self.read(1)
            if not chunk:
                break
            line += chunk
            if chunk == b"\n":
                break
            if size is not None and len(line) >= size:
                break
        return line
    
    def readlines(self, hint: Optional[int] = None) -> list[bytes]:
        lines = []
        while True:
            line = self.readline()
            if not line:
                break
            lines.append(line)
            if hint and len(lines) >= hint:
                break
        return lines
    
    def write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if "w" not in self._mode and "a" not in self._mode and "r+" not in self._mode:
            raise CloudFileWriteError("File not opened for writing")
        written = self.cloud_path.client._stream_write(self.cloud_path, data, self._position)
        self._position += written
        self._dirty = True
        self._has_written = True
        if self._size is not None:
            self._size = max(self._size, self._position)
        return written
    
    def writelines(self, lines: list[bytes]) -> None:
        for line in lines:
            self.write(line)
    
    def seek(self, offset: int, whence: int = 0) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if whence == 0:
            new_position = offset
        elif whence == 1:
            new_position = self._position + offset
        elif whence == 2:
            if self._size is None:
                try:
                    self._size = self.cloud_path.stat().st_size
                except Exception:
                    raise CloudFileSeekError("Cannot seek from end without knowing file size")
            new_position = self._size + offset
        else:
            raise CloudFileSeekError(f"Invalid whence value: {whence}")
        if new_position < 0:
            raise CloudFileSeekError("Cannot seek to negative position")
        self._position = new_position
        return self._position
    
    def tell(self) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        return self._position
    
    def truncate(self, size: Optional[int] = None) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if "w" not in self._mode and "a" not in self._mode and "r+" not in self._mode:
            raise CloudFileWriteError("File not opened for writing")
        if size is None:
            size = self._position
        self.cloud_path.client._stream_truncate(self.cloud_path, size)
        self._size = size
        if self._position > size:
            self._position = size
        self._dirty = True
        return size
    
    def flush(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if self._dirty:
            self.cloud_path.client._stream_flush(self.cloud_path)
            self._dirty = False
    
    def close(self) -> None:
        if self._closed:
            return
        self.flush()
        self._closed = True
    
    def __enter__(self) -> "CloudFile":
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
    
    def __iter__(self) -> "CloudFile":
        return self
    
    def __next__(self) -> bytes:
        line = self.readline()
        if not line:
            raise StopIteration
        return line
    
    @property
    def closed(self) -> bool:
        return self._closed
    
    @property
    def mode(self) -> str:
        return self._mode
    
    @mode.setter
    def mode(self, value: str):
        self._mode = value
    
    @property
    def name(self) -> str:
        return str(self.cloud_path)
    
    def readable(self) -> bool:
        return "r" in self.mode
    
    def writable(self) -> bool:
        return "w" in self.mode or "a" in self.mode or "r+" in self.mode
    
    def seekable(self) -> bool:
        return True 