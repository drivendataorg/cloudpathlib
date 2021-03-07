# History

## v0.4.0 (Unreleased)

- Added rich comparison operator support to cloud paths, which means you can now use them with `sorted`.
- Added polymorphic class `AnyPath` which creates a cloud path or `pathlib.Path` instance appropriately for an input filepath. See new [documentation](http://https://cloudpathlib.drivendata.org/anypath-polymorphism/) for details and example usage.
- Added integration with [Pydantic](https://pydantic-docs.helpmanual.io/). See new [documentation](http://https://cloudpathlib.drivendata.org/integrations/#pydantic) for details and example usage.
- Exceptions:
    - Changed all custom `cloudpathlib` exceptions to be located in new `cloudpathlib.exceptions` module.
    - Changed all custom `cloudpathlib` exceptions to subclass from new base `CloudPathException`. This allows for easy catching of any custom exception from `cloudpathlib`.
    - Changed all custom exceptions names to end with `Error` as recommended by [PEP 8](https://www.python.org/dev/peps/pep-0008/#exception-names).
    - Changed various functions to throw new `CloudPathFileExistsError`, `CloudPathIsADirectoryError` or `CloudPathNotADirectoryError` exceptions instead of a generic `ValueError`.
    - Removed exception exports from the root `cloudpathlib` package namespace. Import from `cloudpathlib.exceptions` instead if needed.
- Fixed bug where `hash(...)` of a cloud path was not consistent with the equality operator.
- Fixed `AzureBlobClient` instantiation to throw new error `MissingCredentialsError` when no credentials are provided, instead of `AttributeError`.
- Fixed `GSClient` to instantiate as anonymous with public access only when instantiated with no credentials, instead of erroring.

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
