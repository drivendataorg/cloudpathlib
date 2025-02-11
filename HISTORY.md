# cloudpathlib Changelog

## Unreleased

- Fixed `CloudPath(...) / other` to correctly attempt to fall back on `other`'s `__rtruediv__` implementation, in order to support classes that explicitly support the `/` with a `CloudPath` instance. Previously, this would always raise a `TypeError` if `other` were not a `str` or `PurePosixPath`. (PR [#479](https://github.com/drivendataorg/cloudpathlib/pull/479))
- Add `md5` property to `GSPath`, updated LocalGSPath to include `md5` property, updated mock_gs.MockBlob to include `md5_hash` property.
- Fixed an uncaught exception on Azure Gen2 storage accounts with HNS enabled when used with `DefaultAzureCredential`. (Issue [#486](https://github.com/drivendataorg/cloudpathlib/issues/486))

## v0.20.0 (2024-10-18)

- Added support for custom schemes in CloudPath and Client subclases. (Issue [#466](https://github.com/drivendataorg/cloudpathlib/issues/466), PR [#467](https://github.com/drivendataorg/cloudpathlib/pull/467))
- Fixed `ResourceNotFoundError` on Azure gen2 storage accounts with HNS enabled and issue that some Azure credentials do not have `account_name`. (Issue [#470](https://github.com/drivendataorg/cloudpathlib/issues/470), Issue [#476](https://github.com/drivendataorg/cloudpathlib/issues/476), PR [#478](https://github.com/drivendataorg/cloudpathlib/pull/478))
- Added support for Python 3.13 (Issue [#472](https://github.com/drivendataorg/cloudpathlib/issues/472), [PR #474](https://github.com/drivendataorg/cloudpathlib/pull/474)):
  - [`.full_match` added](https://docs.python.org/3.13/library/pathlib.html#pathlib.PurePath.full_match)
  - [`.from_uri` added](https://docs.python.org/3.13/library/pathlib.html#pathlib.Path.from_uri)
  - [`follow_symlinks` kwarg added to `is_file`](https://docs.python.org/3.13/library/pathlib.html#pathlib.Path.is_file) added as no-op
  - [`follow_symlinks` kwarg added to `is_dir`](https://docs.python.org/3.13/library/pathlib.html#pathlib.Path.is_dir) added as no-op
  - [`newline` kwarg added to `read_text`](https://docs.python.org/3.13/library/pathlib.html#pathlib.Path.read_text)
  - [`recurse_symlinks` kwarg added to `glob`](https://docs.python.org/3.13/library/pathlib.html#pathlib.Path.glob) added as no-op
  - [`pattern` parameter for `glob` can be PathLike](https://docs.python.org/3.13/library/pathlib.html#pathlib.Path.glob)
  - [`recurse_symlinks` kwarg added to `rglob`](https://docs.python.org/3.13/library/pathlib.html#pathlib.Path.rglob) added as no-op
  - [`pattern` parameter for `rglob` can be PathLike](https://docs.python.org/3.13/library/pathlib.html#pathlib.Path.rglob)
  - [`.parser` property added](https://docs.python.org/3/library/pathlib.html#pathlib.PurePath.parser)


## v0.19.0 (2024-08-29)

- Fixed an error that occurred when loading and dumping `CloudPath` objects using pickle multiple times. (Issue [#450](https://github.com/drivendataorg/cloudpathlib/issues/450), PR [#454](https://github.com/drivendataorg/cloudpathlib/pull/454), thanks to [@kujenga](https://github.com/kujenga))
- Fixed typo in `FileCacheMode` where values were being filled by environment variable `CLOUPATHLIB_FILE_CACHE_MODE` instead of `CLOUDPATHLIB_FILE_CACHE_MODE`. (PR [#424](https://github.com/drivendataorg/cloudpathlib/pull/424), thanks to [@mynameisfiber](https://github.com/drivendataorg/cloudpathlib/pull/424))
- Fixed `CloudPath` cleanup via `CloudPath.__del__` when `Client` encounters an exception during initialization and does not create a `file_cache_mode` attribute. (Issue [#372](https://github.com/drivendataorg/cloudpathlib/issues/372), thanks to [@bryanwweber](https://github.com/bryanwweber))
- Removed support for Python 3.7 and pinned minimal `boto3` version to Python 3.8+ versions. (PR [#407](https://github.com/drivendataorg/cloudpathlib/pull/407))
- Changed `GSClient` to use the native `exists()` method from the Google Cloud Storage SDK. (PR [#420](https://github.com/drivendataorg/cloudpathlib/pull/420), thanks to [@bachya](https://github.com/bachya))
- Changed default clients to be lazily instantiated (Issue [#428](https://github.com/drivendataorg/cloudpathlib/issues/428), PR [#432](https://github.com/drivendataorg/cloudpathlib/issues/432))
- Fixed `download_to` to check for the existence of the cloud file (Issue [#430](https://github.com/drivendataorg/cloudpathlib/issues/430), PR [#433](https://github.com/drivendataorg/cloudpathlib/pull/433))
- Added env vars `CLOUDPATHLIB_FORCE_OVERWRITE_FROM_CLOUD` and `CLOUDPATHLIB_FORCE_OVERWRITE_TO_CLOUD`. (Issue [#393](https://github.com/drivendataorg/cloudpathlib/issues/393), PR [#437](https://github.com/drivendataorg/cloudpathlib/pull/437))
- Fixed `glob` for `cloudpathlib.local.LocalPath` and subclass implementations to match behavior of cloud versions for parity in testing. (Issue [#415](https://github.com/drivendataorg/cloudpathlib/issues/415), PR [#436](https://github.com/drivendataorg/cloudpathlib/pull/436))
- Changed how `cloudpathlib.local.LocalClient` and subclass implementations track the default local storage directory (used to simulate the cloud) used when no local storage directory is explicitly provided. (PR [#436](https://github.com/drivendataorg/cloudpathlib/pull/436), PR [#462](https://github.com/drivendataorg/cloudpathlib/pull/462))
    - Changed `LocalClient` so that client instances using the default storage access the default local storage directory through the `get_default_storage_dir` rather than having an explicit reference to the path set at instantiation. This means that calling `get_default_storage_dir` will reset the local storage for all clients using the default local storage, whether the client has already been instantiated or is instantiated after resetting. This fixes unintuitive behavior where `reset_local_storage` did not reset local storage when using the default client. (Issue [#414](https://github.com/drivendataorg/cloudpathlib/issues/414))
    - Added a new `local_storage_dir` property to `LocalClient`. This will return the current local storage directory used by that client instance.
    by reference through the `get_default_ rather than with an explicit.
- Refined the return type annotations for `CloudPath.open()` to match the behavior of `pathlib.Path.open()`. The method now returns specific types (`TextIOWrapper`, `FileIO`, `BufferedRandom`, `BufferedWriter`, `BufferedReader`, `BinaryIO`, `IO[Any]`) based on the provided `mode`, `buffering`, and `encoding` arguments. ([Issue #465](https://github.com/drivendataorg/cloudpathlib/issues/465), [PR #464](https://github.com/drivendataorg/cloudpathlib/pull/464))
- Added Azure Data Lake Storage Gen2 support (Issue [#161](https://github.com/drivendataorg/cloudpathlib/issues/161), PR [#450](https://github.com/drivendataorg/cloudpathlib/pull/450)), thanks to [@M0dEx](https://github.com/M0dEx) for PR [#447](https://github.com/drivendataorg/cloudpathlib/pull/447) and PR [#449](https://github.com/drivendataorg/cloudpathlib/pull/449)

## v0.18.1 (2024-02-26)

- Fixed import error due to incompatible `google-cloud-storage` by not using `transfer_manager` if it is not available. ([Issue #408](https://github.com/drivendataorg/cloudpathlib/issues/408), [PR #410](https://github.com/drivendataorg/cloudpathlib/pull/410))

Includes all changes from v0.18.0.

**Note: This is the last planned Python 3.7 compatible release version.**

## 0.18.0 (2024-02-25) (Yanked)

- Implement sliced downloads in GSClient. (Issue [#387](https://github.com/drivendataorg/cloudpathlib/issues/387), PR [#389](https://github.com/drivendataorg/cloudpathlib/pull/389))
- Implement `as_url` with presigned parameter for all backends. (Issue [#235](https://github.com/drivendataorg/cloudpathlib/issues/235), PR [#236](https://github.com/drivendataorg/cloudpathlib/pull/236))
- Stream to and from Azure Blob Storage. (PR [#403](https://github.com/drivendataorg/cloudpathlib/pull/403))
- Implement `file:` URI scheme support for `AnyPath`. (Issue [#401](https://github.com/drivendataorg/cloudpathlib/issues/401), PR [#404](https://github.com/drivendataorg/cloudpathlib/pull/404))

**Note: This version was [yanked](https://pypi.org/help/#yanked) due to incompatibility with google-cloud-storage <2.7.0 that causes an import error.**

## 0.17.0 (2023-12-21)

- Fix `S3Client` cleanup via `Client.__del__` when `S3Client` encounters an exception during initialization. (Issue [#372](https://github.com/drivendataorg/cloudpathlib/issues/372), PR [#373](https://github.com/drivendataorg/cloudpathlib/pull/373), thanks to [@bryanwweber](https://github.com/bryanwweber))
- Skip mtime checks during upload when force_overwrite_to_cloud is set to improve upload performance. (Issue [#379](https://github.com/drivendataorg/cloudpathlib/issues/379), PR [#380](https://github.com/drivendataorg/cloudpathlib/pull/380), thanks to [@Gilthans](https://github.com/Gilthans))

## v0.16.0 (2023-10-09)
 - Add "CloudPath" as return type on `__init__` for mypy issues. ([Issue #179](https://github.com/drivendataorg/cloudpathlib/issues/179), [PR #342](https://github.com/drivendataorg/cloudpathlib/pull/342))
 - Add `with_stem` to all path types when python version supports it (>=3.9). ([Issue #287](https://github.com/drivendataorg/cloudpathlib/issues/287), [PR #290](https://github.com/drivendataorg/cloudpathlib/pull/290), thanks to [@Gilthans](https://github.com/Gilthans))
 - Add `newline` parameter to the `write_text` method to align to `pathlib` functionality as of Python 3.10. [PR #362](https://github.com/drivendataorg/cloudpathlib/pull/362), thanks to [@pricemg](https://github.com/pricemg).
 - Add support for Python 3.12 ([PR #364](https://github.com/drivendataorg/cloudpathlib/pull/364))
 - Add `CLOUDPATHLIB_LOCAL_CACHE_DIR` env var for setting local_cache_dir default for clients ([Issue #352](https://github.com/drivendataorg/cloudpathlib/issues/352), [PR #357](https://github.com/drivendataorg/cloudpathlib/pull/357))
 - Add `CONTRIBUTING.md` instructions for contributors ([Issue #213](https://github.com/drivendataorg/cloudpathlib/issues/213), [PR #367](https://github.com/drivendataorg/cloudpathlib/pull/367))

## v0.15.1 (2023-07-12)

- Compatibility with pydantic >= 2.0.0. ([PR #349](https://github.com/drivendataorg/cloudpathlib/pull/349))

## v0.15.0 (2023-06-16)

- Changed return type for `CloudPathMeta.__call__` to fix problems with pyright/pylance ([PR #330](https://github.com/drivendataorg/cloudpathlib/pull/330))
- Make `CloudPath.is_valid_cloudpath` a TypeGuard so that type checkers can know the subclass if `is_valid_cloudpath` is called ([PR #337](https://github.com/drivendataorg/cloudpathlib/pull/337))
- Added `follow_symlinks` to `stat` for 3.11.4 compatibility (see [bpo 39906](https://github.com/python/cpython/issues/84087))
- Add `follow_symlinks` to `is_dir` implementation for CPython `glob` compatibility (see [CPython PR #104512](https://github.com/python/cpython/pull/104512))

## v0.14.0 (2023-05-13)

- Changed to pyproject.toml-based build.
- Changed type hints from custom type variable `DerivedCloudPath` to [`typing.Self`](https://docs.python.org/3/library/typing.html#typing.Self) ([PEP 673](https://docs.python.org/3/library/typing.html#typing.Self)). This adds a dependency on the [typing-extensions](https://pypi.org/project/typing-extensions/) backport package from Python versions lower than 3.11.
- Fixed a runtime key error when an S3 object does not have the `Content-Type` metadata set. ([Issue #331](https://github.com/drivendataorg/cloudpathlib/issues/331), [PR #332](https://github.com/drivendataorg/cloudpathlib/pull/332))

## v0.13.0 (2023-02-15)

 - Implement `file_cache_mode`s to give users finer-grained control over when and how the cache is cleared. ([Issue #10](https://github.com/drivendataorg/cloudpathlib/issues/10), [PR #314](https://github.com/drivendataorg/cloudpathlib/pull/314))
 - Speed up listing directories for Google Cloud Storage. ([PR #318](https://github.com/drivendataorg/cloudpathlib/pull/318))
 - Add compatibility for Python 3.11 ([PR #317](https://github.com/drivendataorg/cloudpathlib/pull/317))

## v0.12.1 (2023-01-04)

 - Fix glob logic for buckets; add regression test; add error on globbing all buckets ([Issue #311](https://github.com/drivendataorg/cloudpathlib/issues/311), [PR #312](https://github.com/drivendataorg/cloudpathlib/pull/312))

## v0.12.0 (2022-12-30)

 - API Change: `S3Client` supports an `extra_args` kwarg now to pass extra args down to `boto3` functions; this enables Requester Pays bucket access and bucket encryption. (Issues [#254](https://github.com/drivendataorg/cloudpathlib/issues/254), [#180](https://github.com/drivendataorg/cloudpathlib/issues/180); [PR #307](https://github.com/drivendataorg/cloudpathlib/pull/307))
 - Speed up glob! ([Issue #274](https://github.com/drivendataorg/cloudpathlib/issues/274), [PR #304](https://github.com/drivendataorg/cloudpathlib/pull/304))
 - Ability to list buckets/containers a user has access to. ([Issue #48](https://github.com/drivendataorg/cloudpathlib/issues/48), [PR #307](https://github.com/drivendataorg/cloudpathlib/pull/307))
 - Remove overly specific status check and assert in production code on remove. ([Issue #212](https://github.com/drivendataorg/cloudpathlib/issues/212), [PR #307](https://github.com/drivendataorg/cloudpathlib/pull/307))
 - Update docs, including accessing public buckets. ([Issue #271](https://github.com/drivendataorg/cloudpathlib/issues/271), [PR #307](https://github.com/drivendataorg/cloudpathlib/pull/307))

## v0.11.0 (2022-12-18)

 - API change: Add `ignore` parameter to `CloudPath.copytree` in order to match `shutil` API. ([Issue #145](https://github.com/drivendataorg/cloudpathlib/issues/145), [PR #272](https://github.com/drivendataorg/cloudpathlib/pull/272))
 - Use the V2 version for listing objects `list_objects_v2` in `S3Client`. ([Issue #155](https://github.com/drivendataorg/cloudpathlib/issues/155), [PR #302](https://github.com/drivendataorg/cloudpathlib/pull/302))
 - Add abilty to use `.exists` to check for a raw bucket/container (no additional path components). ([Issue #291](https://github.com/drivendataorg/cloudpathlib/issues/291), [PR #302](https://github.com/drivendataorg/cloudpathlib/pull/302))
 - Prevent data loss when renaming by skipping files that would be renamed to the same thing. ([Issue #277](https://github.com/drivendataorg/cloudpathlib/issues/277), [PR #278](https://github.com/drivendataorg/cloudpathlib/pull/278))
 - Speed up common `glob`/`rglob` patterns. ([Issue #274](https://github.com/drivendataorg/cloudpathlib/issues/274), [PR #276](https://github.com/drivendataorg/cloudpathlib/pull/276))


## v0.10.0 (2022-08-18)

 - API change: Make `stat` on base class method instead of property to follow `pathlib` ([Issue #234](https://github.com/drivendataorg/cloudpathlib/issues/234), [PR #250](https://github.com/drivendataorg/cloudpathlib/pull/250))
 - Fixed "S3Path.exists() returns True on partial matches." ([Issue #208](https://github.com/drivendataorg/cloudpathlib/issues/208), [PR #244](https://github.com/drivendataorg/cloudpathlib/pull/244))
 - Make `AnyPath` subclass of `AnyPath` ([Issue #246](https://github.com/drivendataorg/cloudpathlib/issues/246), [PR #251](https://github.com/drivendataorg/cloudpathlib/pull/251))
 - Skip docstrings if not present to avoid failing under `-00` ([Issue #238](https://github.com/drivendataorg/cloudpathlib/issues/238), [PR #249](https://github.com/drivendataorg/cloudpathlib/pull/249))
 - Add `py.typed` file so mypy runs ([Issue #243](https://github.com/drivendataorg/cloudpathlib/issues/243), [PR #248](https://github.com/drivendataorg/cloudpathlib/pull/248))

## v0.9.0 (2022-06-03)
 - Added `absolute` to `CloudPath` (does nothing as `CloudPath` is always absolute) ([PR #230](https://github.com/drivendataorg/cloudpathlib/pull/230))
 - Added `resolve` to `CloudPath` (does nothing as `CloudPath` is resolved in advance) ([Issue #151](https://github.com/drivendataorg/cloudpathlib/issues/151), [PR #230](https://github.com/drivendataorg/cloudpathlib/pull/230))
 - Added `relative_to` to `CloudPath` which returns a `PurePosixPath` ([Issue #149](https://github.com/drivendataorg/cloudpathlib/issues/149), [PR #230](https://github.com/drivendataorg/cloudpathlib/pull/230))
 - Added `is_relative_to` to `CloudPath` ([Issue #149](https://github.com/drivendataorg/cloudpathlib/issues/149), [PR #230](https://github.com/drivendataorg/cloudpathlib/pull/230))
 - Added `is_absolute` to `CloudPath` (always true as `CloudPath` is always absolute) ([PR #230](https://github.com/drivendataorg/cloudpathlib/pull/230))
 - Accept and delegate `read_text` parameters to cached file ([PR #230](https://github.com/drivendataorg/cloudpathlib/pull/230))
 - Added `exist_ok` parameter to `touch` ([PR #230](https://github.com/drivendataorg/cloudpathlib/pull/230))
 - Added `missing_ok` parameter to `unlink`, which defaults to True. This diverges from pathlib to maintain backward compatibility ([PR #230](https://github.com/drivendataorg/cloudpathlib/pull/230))
 - Fixed missing root object entries in documentation's Intersphinx inventory ([Issue #211](https://github.com/drivendataorg/cloudpathlib/issues/211), [PR #237](https://github.com/drivendataorg/cloudpathlib/pull/237))

## v0.8.0 (2022-05-19)

 - Fixed pickling of `CloudPath` objects not working. ([Issue #223](https://github.com/drivendataorg/cloudpathlib/issues/223), [PR #224](https://github.com/drivendataorg/cloudpathlib/pull/224))
 - Added functionality to [push the MIME (media) type to the content type property on cloud providers by default. ([Issue #222](https://github.com/drivendataorg/cloudpathlib/issues/222), [PR #226](https://github.com/drivendataorg/cloudpathlib/pull/226))

## v0.7.1 (2022-04-06)

- Fixed inadvertent inclusion of tests module in package. ([Issue #173](https://github.com/drivendataorg/cloudpathlib/issues/173), [PR #219](https://github.com/drivendataorg/cloudpathlib/pull/219))

## v0.7.0 (2022-02-16)

- Fixed `glob` and `rglob` functions by using pathlib's globbing logic rather than fnmatch. ([Issue #154](https://github.com/drivendataorg/cloudpathlib/issues/154))
- Fixed `iterdir` to not include self. ([Issue #15](https://github.com/drivendataorg/cloudpathlib/issues/15))
- Fixed error when calling `suffix` and `suffixes` on a cloud path with no suffix. ([Issue #120](https://github.com/drivendataorg/cloudpathlib/issues/120))
- Changed `parents` return type from list to tuple, to better match pathlib's tuple-like `_PathParents` return type.
- Remove support for Python 3.6. [Issue #186](https://github.com/drivendataorg/cloudpathlib/issues/186)

## v0.6.5 (2022-01-25)

- Fixed error when "directories" created on AWS S3 were reported as files. ([Issue #148](https://github.com/drivendataorg/cloudpathlib/issues/148), [PR #190](https://github.com/drivendataorg/cloudpathlib/pull/190))
- Fixed bug where GCE machines can instantiate default client, but we don't attempt it. ([Issue #191](https://github.com/drivendataorg/cloudpathlib/issues/191)
- Support `AWS_ENDPOINT_URL` environment variable to set the `endpoint_url` for `S3Client`. ([PR #193](https://github.com/drivendataorg/cloudpathlib/pull/193))

## v0.6.4 (2021-12-29)

- Fixed error where `BlobProperties` type hint causes import error if Azure dependencies not installed.

## v0.6.3 (2021-12-29)

- Fixed error when using `rmtree` on nested directories for Google Cloud Storage and Azure Blob Storage. ([Issue #184](https://github.com/drivendataorg/cloudpathlib/issues/184), [PR #185](https://github.com/drivendataorg/cloudpathlib/pull/185))
- Fixed broken builds due mypy errors in azure dependency ([PR #177](https://github.com/drivendataorg/cloudpathlib/pull/177))
- Fixed dev tools for building and serving documentation locally ([PR #178](https://github.com/drivendataorg/cloudpathlib/pull/178))

## v0.6.2 (2021-09-20)

- Fixed error when importing `cloudpathlib` for missing `botocore` dependency when not installed with S3 dependencies. ([PR #168](https://github.com/drivendataorg/cloudpathlib/pull/168))

## v0.6.1 (2021-09-17)

- Fixed absolute documentation URLs to point to the new versioned documentation pages.
- Fixed bug where `no_sign_request` couldn't be used to download files since our code required list permissions to the bucket to do so. ([Issue #169](https://github.com/drivendataorg/cloudpathlib/issues/169), [PR #168](https://github.com/drivendataorg/cloudpathlib/pull/168)).

## v0.6.0 (2021-09-07)

- Added `no_sign_request` parameter to `S3Client` instantiation for anonymous requests for public resources on S3. See [documentation](https://cloudpathlib.drivendata.org/stable/api-reference/s3client/#cloudpathlib.s3.s3client.S3Client.__init__) for more details. ([#164](https://github.com/drivendataorg/cloudpathlib/pull/164))

## v0.5.0 (2021-08-31)

- Added `boto3_transfer_config` parameter to `S3Client` instantiation, which allows passing a `boto3.s3.transfer.TransferConfig` object and is useful for controlling multipart and thread use in uploads and downloads. See [documentation](https://cloudpathlib.drivendata.org/stable/api-reference/s3client/#cloudpathlib.s3.s3client.S3Client.__init__) for more details. ([#150](https://github.com/drivendataorg/cloudpathlib/pull/150))

## v0.4.1 (2021-05-29)

- Added support for custom S3-compatible object stores. This functionality is available via the `endpoint_url` keyword argument when instantiating an `S3Client` instance. See [documentation](https://cloudpathlib.drivendata.org/stable/authentication/#accessing-custom-s3-compatible-object-stores) for more details. ([#138](https://github.com/drivendataorg/cloudpathlib/pull/138) thanks to [@YevheniiSemendiak](https://github.com/YevheniiSemendiak))
- Added `CloudPath.upload_from` which uploads the passed path to this CloudPath (issuse [#58](https://github.com/drivendataorg/cloudpathlib/issues/58))
- Added support for common file transfer functions based on `shutil`. Issue [#108](https://github.com/drivendataorg/cloudpathlib/issues/108). PR [#142](https://github.com/drivendataorg/cloudpathlib/pull/142).
  - `CloudPath.copy` copy a file from one location to another. Can be cloud -> local or cloud -> cloud. If `client` is not the same, the file transits through the local machine.
  - `CloudPath.copytree` reucrsively copy a directory from one location to another. Can be cloud -> local or cloud -> cloud. Uses `CloudPath.copy` so if `client` is not the same, the file transits through the local machine.

## v0.4.0 (2021-03-13)

- Added rich comparison operator support to cloud paths, which means you can now use them with `sorted`. ([#129](https://github.com/drivendataorg/cloudpathlib/pull/129))
- Added polymorphic class `AnyPath` which creates a cloud path or `pathlib.Path` instance appropriately for an input filepath. See new [documentation](https://cloudpathlib.drivendata.org/stable/anypath-polymorphism/) for details and example usage. ([#130](https://github.com/drivendataorg/cloudpathlib/pull/130))
- Added integration with [Pydantic](https://pydantic-docs.helpmanual.io/). See new [documentation](https://cloudpathlib.drivendata.org/stable/integrations/#pydantic) for details and example usage. ([#130](https://github.com/drivendataorg/cloudpathlib/pull/130))
- Exceptions: ([#131](https://github.com/drivendataorg/cloudpathlib/pull/131))
    - Changed all custom `cloudpathlib` exceptions to be located in new `cloudpathlib.exceptions` module.
    - Changed all custom `cloudpathlib` exceptions to subclass from new base `CloudPathException`. This allows for easy catching of any custom exception from `cloudpathlib`.
    - Changed all custom exceptions names to end with `Error` as recommended by [PEP 8](https://www.python.org/dev/peps/pep-0008/#exception-names).
    - Changed various functions to throw new `CloudPathFileExistsError`, `CloudPathIsADirectoryError` or `CloudPathNotADirectoryError` exceptions instead of a generic `ValueError`.
    - Removed exception exports from the root `cloudpathlib` package namespace. Import from `cloudpathlib.exceptions` instead if needed.
- Fixed `download_to` method to handle case when source is a file and destination is a directory. ([#121](https://github.com/drivendataorg/cloudpathlib/pull/121) thanks to [@genziano](https://github.com/genziano))
- Fixed bug where `hash(...)` of a cloud path was not consistent with the equality operator. ([#129](https://github.com/drivendataorg/cloudpathlib/pull/129))
- Fixed `AzureBlobClient` instantiation to throw new error `MissingCredentialsError` when no credentials are provided, instead of `AttributeError`. `LocalAzureBlobClient` has also been changed to accordingly error under those conditions. ([#131](https://github.com/drivendataorg/cloudpathlib/pull/131))
- Fixed `GSClient` to instantiate as anonymous with public access only when instantiated with no credentials, instead of erroring. ([#131](https://github.com/drivendataorg/cloudpathlib/pull/131))

## v0.3.0 (2021-01-29)

- Added a new module `cloudpathlib.local` with utilities for mocking cloud paths in tests. The module has "Local" substitute classes that use the local filesystem in place of cloud storage. See the new documentation article ["Testing code that uses cloudpathlib"](https://cloudpathlib.drivendata.org/stable/testing_mocked_cloudpathlib/) to learn more about how to use them. ([#107](https://github.com/drivendataorg/cloudpathlib/pull/107))

## v0.2.1 (2021-01-25)

- Fixed bug where a `NameError` was raised if the Google Cloud Storage dependencies were not installed (even if using a different storage provider).

## v0.2.0 (2021-01-23)

- Added support for Google Cloud Storage. Instantiate with URIs prefixed by `gs://` or explicitly using the `GSPath` class. ([#113](https://github.com/drivendataorg/cloudpathlib/pull/113) thanks to [@wolfgangwazzlestrauss](https://github.com/wolfgangwazzlestrauss))
- Changed backend logic to reduce number of network calls to cloud. This should result in faster cloud path operations, especially when dealing with many small files. ([#110](https://github.com/drivendataorg/cloudpathlib/issues/110), [#111](https://github.com/drivendataorg/cloudpathlib/pull/111))

## v0.1.2 (2020-11-14)

- Fixed `CloudPath` instantiation so that reinstantiating with an existing `CloudPath` instance will reuse the same client, if a new client is not explicitly passed. This addresses the edge case of non-idempotency when reinstantiating a `CloudPath` instance with a non-default client. ([#104](https://github.com/drivendataorg/cloudpathlib/pull/104))

## v0.1.1 (2020-10-15)

- Fixed a character-encoding bug when building from source on Windows. ([#98](https://github.com/drivendataorg/cloudpathlib/pull/98))

## v0.1.0 (2020-10-06)

- Initial release of cloudpathlib with support for Amazon S3 and Azure Blob Storage! 🎉
