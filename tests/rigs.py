"""Which test rigs exist, which ones live tests run against, and how much concurrency each
one gets.

This is the single source of truth for both the test suite (`tests/conftest.py` builds the rig
fixtures and the `CLOUDPATHLIB_TEST_RIGS` selection from it) and CI (`.github/workflows/tests.yml`
builds its live-test matrix by running `make live-rigs`). Keep it dependency-free so it can be
imported without the test requirements installed.

Run a single rig the way CI runs it with `make test-live-cloud-rig RIG=<rig>`.
"""

import json
import os

# rigs that talk to a network backend; only these are meaningful for live tests
NETWORK_RIGS = ("azure", "azure_gen2", "gs", "s3", "custom_s3")

# rigs backed by the local filesystem or a local test server; these behave identically whether
# or not USE_LIVE_CLOUD is set, so live runs skip them by default
LOCAL_RIGS = ("local_azure", "local_s3", "local_gs", "http", "https")

ALL_RIGS = NETWORK_RIGS + LOCAL_RIGS

# xdist workers for a live run of a single rig; these tests are IO-bound, so this is higher
# than the core count of a CI runner
DEFAULT_LIVE_WORKERS = 8

# our custom S3 test server is a single small instance, so don't hammer it (it does handle 8,
# which is ~40% faster, if that leg ever needs speeding up)
LIVE_WORKERS = {"custom_s3": 4}


def live_workers(rig: str) -> int:
    """Number of xdist workers a live run of `rig` should use."""
    if rig not in ALL_RIGS:
        raise ValueError(f"Unknown rig {rig!r}; valid rigs are: {', '.join(ALL_RIGS)}")

    return LIVE_WORKERS.get(rig, DEFAULT_LIVE_WORKERS)


def custom_s3_endpoint() -> str:
    """Endpoint url for the custom (non-AWS) S3 rig.

    An unset *or empty* `CUSTOM_S3_ENDPOINT` means "not configured": CI gives each live-test
    job only the secrets for the provider it tests, so the others arrive as empty strings, and
    an empty endpoint url would otherwise look like a configured endpoint.
    """
    return os.getenv("CUSTOM_S3_ENDPOINT") or "https://s3.us-west-1.drivendatabws.com"


if __name__ == "__main__":
    # `make live-rigs`; CI turns this into one live-test job per rig
    print(json.dumps(list(NETWORK_RIGS)))
