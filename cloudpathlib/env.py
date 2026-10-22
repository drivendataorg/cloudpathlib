"""Environment-variable configuration shared across the library.

Every knob here has a keyword argument or constant that takes precedence when given; the
environment variable only supplies the default. Values are read when they are needed (client
construction, opening a stream), not at import time.
"""

import os
from typing import Optional

from .exceptions import InvalidConfigurationException

MiB = 1024 * 1024

# Bytes fetched per ranged request when reading a stream, held before a part is uploaded
# when the provider has no multipart API, and moved per iteration in stream-to-stream copies.
# 5 MiB matches the multi-MiB block sizes of comparable tools (fsspec/s3fs/gcsfs) so request
# latency does not dominate sequential throughput; reads never fetch past the end of the
# object, so small objects only pay for their actual size.
DEFAULT_STREAMING_BUFFER_SIZE = 5 * MiB

# Requests one open stream may have in flight (background part uploads while writing,
# read-ahead of the next buffers while reading sequentially). 4 keeps per-stream memory to
# about 4 buffers (20 MiB at the default size) while hiding most per-request latency; for
# comparison boto3's TransferConfig uses 10 threads per transfer. 1 is fully sequential.
DEFAULT_STREAMING_MAX_CONCURRENCY = 4


def env_flag(name: str) -> bool:
    """Boolean from an environment variable (`1`/`true`, case-insensitive; unset is False)."""
    return os.environ.get(name, "False").lower() in ["1", "true"]


def env_int(name: str, default: int, minimum: int = 1) -> int:
    """Positive integer from an environment variable, or `default` when unset or empty."""
    raw = os.environ.get(name, "")
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise InvalidConfigurationException(
            f"Environment variable {name}={raw!r} must be an integer."
        ) from None
    if value < minimum:
        raise InvalidConfigurationException(
            f"Environment variable {name}={raw!r} must be at least {minimum}."
        )
    return value


def streaming_buffer_size() -> int:
    """`CLOUDPATHLIB_STREAMING_BUFFER_SIZE` in bytes, default 5 MiB."""
    return env_int("CLOUDPATHLIB_STREAMING_BUFFER_SIZE", DEFAULT_STREAMING_BUFFER_SIZE)


def streaming_max_concurrency() -> int:
    """`CLOUDPATHLIB_STREAMING_MAX_CONCURRENCY`, default 4."""
    return env_int("CLOUDPATHLIB_STREAMING_MAX_CONCURRENCY", DEFAULT_STREAMING_MAX_CONCURRENCY)


def streaming_part_size(provider: Optional[str], minimum: int) -> int:
    """Multipart upload part size in bytes for `provider` (e.g. `s3`, `azure`, `gs`).

    `CLOUDPATHLIB_<PROVIDER>_STREAMING_PART_SIZE` wins over `CLOUDPATHLIB_STREAMING_PART_SIZE`;
    both default to, and may not go below, the provider's `minimum` non-final part size.
    """
    generic = env_int("CLOUDPATHLIB_STREAMING_PART_SIZE", minimum, minimum=minimum)
    if provider is None:
        return generic
    return env_int(
        f"CLOUDPATHLIB_{provider.upper()}_STREAMING_PART_SIZE", generic, minimum=minimum
    )
