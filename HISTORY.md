# History

## v0.1.2 (Unreleased)

- Fixed `CloudPath` instantiation so that reinstantiating with an existing `CloudPath` instance will reuse the same client, if a new client is not explicitly passed. This addresses the edge case of non-idempotency when reinstantiating a `CloudPath` instance with a non-default client. ([#104](https://github.com/drivendataorg/cloudpathlib/pull/104))

## v0.1.1 (2020-10-15)

- Fixed a character-encoding bug when building from source on Windows. ([#98](https://github.com/drivendataorg/cloudpathlib/pull/98))

## v0.1.0 (2020-10-06)

- Initial release of cloudpathlib with support for Amazon S3 and Azure Blob Storage! 🎉
