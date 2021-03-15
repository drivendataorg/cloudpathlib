# cloudpathlib Changelog

## v0.4.1 (unreleased)
 - Add `.cloud` accessor to `pandas.Series` and `pandas.Index` through [pandas-path](https://github.com/drivendataorg/pandas-path) integration. Registered if user does `from cloudpathlib.pandas import cloud`.

## v0.4.0 (2021-03-13)

- Added rich comparison operator support to cloud paths, which means you can now use them with `sorted`. ([#129](https://github.com/drivendataorg/cloudpathlib/pull/129))
- Added polymorphic class `AnyPath` which creates a cloud path or `pathlib.Path` instance appropriately for an input filepath. See new [documentation](http://https://cloudpathlib.drivendata.org/anypath-polymorphism/) for details and example usage. ([#130](https://github.com/drivendataorg/cloudpathlib/pull/130))
- Added integration with [Pydantic](https://pydantic-docs.helpmanual.io/). See new [documentation](http://https://cloudpathlib.drivendata.org/integrations/#pydantic) for details and example usage. ([#130](https://github.com/drivendataorg/cloudpathlib/pull/130))
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

- Added a new module `cloudpathlib.local` with utilities for mocking cloud paths in tests. The module has "Local" substitute classes that use the local filesystem in place of cloud storage. See the new documentation article ["Testing code that uses cloudpathlib"](https://cloudpathlib.drivendata.org/testing_mocked_cloudpathlib/) to learn more about how to use them. ([#107](https://github.com/drivendataorg/cloudpathlib/pull/107))

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
