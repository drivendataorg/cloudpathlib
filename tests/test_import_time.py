"""Test that importing cloudpathlib doesn't eagerly load cloud provider SDKs.

These tests verify that lazy loading is working correctly by checking that
heavy cloud SDK modules are not loaded until actually needed.
"""

import subprocess
import sys


def test_import_does_not_load_cloud_sdks():
    """Verify that importing cloudpathlib doesn't eagerly load cloud provider SDKs.

    Cloud SDKs (google-cloud-storage, boto3, azure-storage-blob) are heavy and
    add significant import time. They should only be loaded when actually used.
    """
    # Run a subprocess to get a clean import state
    code = """
import sys

# Import cloudpathlib
import cloudpathlib

# Check that cloud SDKs are NOT loaded yet
cloud_sdk_modules = [
    'google.cloud.storage',
    'google.api_core',
    'boto3',
    'botocore',
    'azure.storage.blob',
]

loaded = [m for m in cloud_sdk_modules if m in sys.modules]
if loaded:
    print(f"FAIL: These modules were eagerly loaded: {loaded}")
    sys.exit(1)
else:
    print("PASS: No cloud SDK modules were eagerly loaded")
    sys.exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Test failed: {result.stdout}\n{result.stderr}"


def test_accessing_path_class_does_not_load_other_sdks():
    """Verify that accessing one Path class doesn't load other provider SDKs.

    When you access S3Path, it shouldn't load GCS or Azure SDKs.
    """
    code = """
import sys

# Access S3Path class (but don't instantiate)
from cloudpathlib import S3Path

# GCS and Azure should NOT be loaded just from accessing S3Path
other_sdks = [
    'google.cloud.storage',
    'google.api_core',
    'azure.storage.blob',
]

loaded = [m for m in other_sdks if m in sys.modules]
if loaded:
    print(f"FAIL: These unrelated modules were loaded: {loaded}")
    sys.exit(1)
else:
    print("PASS: Other provider SDKs were not loaded")
    sys.exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Test failed: {result.stdout}\n{result.stderr}"


def test_accessing_gs_path_class_does_not_load_other_sdks():
    """Verify that accessing GSPath class doesn't load S3 or Azure SDKs."""
    code = """
import sys

# Access GSPath class (but don't instantiate)
from cloudpathlib import GSPath

# S3 and Azure should NOT be loaded
other_sdks = [
    'boto3',
    'botocore',
    'azure.storage.blob',
]

loaded = [m for m in other_sdks if m in sys.modules]
if loaded:
    print(f"FAIL: These unrelated modules were loaded: {loaded}")
    sys.exit(1)
else:
    print("PASS: Other provider SDKs were not loaded")
    sys.exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Test failed: {result.stdout}\n{result.stderr}"


def test_accessing_azure_path_class_does_not_load_other_sdks():
    """Verify that accessing AzureBlobPath class doesn't load S3 or GCS SDKs."""
    code = """
import sys

# Access AzureBlobPath class (but don't instantiate)
from cloudpathlib import AzureBlobPath

# S3 and GCS should NOT be loaded
other_sdks = [
    'boto3',
    'botocore',
    'google.cloud.storage',
    'google.api_core',
]

loaded = [m for m in other_sdks if m in sys.modules]
if loaded:
    print(f"FAIL: These unrelated modules were loaded: {loaded}")
    sys.exit(1)
else:
    print("PASS: Other provider SDKs were not loaded")
    sys.exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Test failed: {result.stdout}\n{result.stderr}"


def test_submodule_import_s3_does_not_load_other_sdks():
    """Verify that importing from cloudpathlib.s3 doesn't load other SDKs."""
    code = """
import sys

# Import from submodule directly
from cloudpathlib.s3 import S3Path

# GCS and Azure should NOT be loaded
other_sdks = [
    'google.cloud.storage',
    'google.api_core',
    'azure.storage.blob',
]

loaded = [m for m in other_sdks if m in sys.modules]
if loaded:
    print(f"FAIL: These unrelated modules were loaded: {loaded}")
    sys.exit(1)
else:
    print("PASS: Other provider SDKs were not loaded")
    sys.exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Test failed: {result.stdout}\n{result.stderr}"


def test_submodule_import_gs_does_not_load_other_sdks():
    """Verify that importing from cloudpathlib.gs doesn't load other SDKs."""
    code = """
import sys

# Import from submodule directly
from cloudpathlib.gs import GSPath

# S3 and Azure should NOT be loaded
other_sdks = [
    'boto3',
    'botocore',
    'azure.storage.blob',
]

loaded = [m for m in other_sdks if m in sys.modules]
if loaded:
    print(f"FAIL: These unrelated modules were loaded: {loaded}")
    sys.exit(1)
else:
    print("PASS: Other provider SDKs were not loaded")
    sys.exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Test failed: {result.stdout}\n{result.stderr}"


def test_submodule_import_azure_does_not_load_other_sdks():
    """Verify that importing from cloudpathlib.azure doesn't load other SDKs."""
    code = """
import sys

# Import from submodule directly
from cloudpathlib.azure import AzureBlobPath

# S3 and GCS should NOT be loaded
other_sdks = [
    'boto3',
    'botocore',
    'google.cloud.storage',
    'google.api_core',
]

loaded = [m for m in other_sdks if m in sys.modules]
if loaded:
    print(f"FAIL: These unrelated modules were loaded: {loaded}")
    sys.exit(1)
else:
    print("PASS: Other provider SDKs were not loaded")
    sys.exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Test failed: {result.stdout}\n{result.stderr}"


def test_import_time_reasonable():
    """Verify that importing cloudpathlib takes less than 500ms.

    Before lazy loading, importing cloudpathlib would load all cloud SDKs,
    taking 1-2 seconds. With lazy loading, it should be under 500ms.
    """
    code = """
import time
start = time.perf_counter()
import cloudpathlib
elapsed = time.perf_counter() - start
print(f"{elapsed:.3f}")
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Import failed: {result.stderr}"

    elapsed = float(result.stdout.strip())
    # Allow up to 500ms for import (generous for CI environments)
    assert elapsed < 0.5, f"Import took {elapsed:.3f}s, expected < 0.5s"
