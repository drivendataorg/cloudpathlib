# `cloudpathlib` Contribution Guidelines

Thanks for offering to help on `cloudpathlib`! We welcome contributions. This document will help you get started finding issues, developing fixes, and submitting PRs.

First, a few guidelines:

 - Follow the [code of conduct](CODE_OF_CONDUCT.md).
 - PRs from outside contributors will not be accepted without an issue. We respect your time and want to make sure any work you do will be reviewed, so please wait for a maintainer to sign off on the issue before getting started. 
 - If you are looking just to make a contribution, look at issues with [label "good first issue"](https://github.com/drivendataorg/cloudpathlib/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22).


## How to contribute

0. As noted above, please file an [issue](https://github.com/drivendataorg/cloudpathlib/issues) if you are not fixing an existing issue.
1. Fork the repo, clone it locally, and create a [local environment](#local-development).
2. Make changes in your local version of the repository.
3. Make sure that the [tests](#tests) pass locally.
4. Update the package [documentation](#documentation), if applicable.
5. Go through the items in the final [PR checklist](#pr-checklist).
6. [Submit a PR](#submitting-a-pr)
 
For some guidance on working with the code, see the sections on [code standards](#code-standards-and-tips) and [code architecture](#code-architecture).


## Local development

Create a Python environment to work in. If you're working on Windows, a bash shell will make your life easier. We recommend [git bash](https://gitforwindows.org/), [cygwin](https://www.cygwin.com/), or [WSL](https://learn.microsoft.com/en-us/windows/wsl/install) so that you can use the `make` commands, but it is totally optional.

You can see all the available developer commands by running `make`:

```
❯ make
clean                remove all build, test, coverage and Python artifacts
clean-build          remove build artifacts
clean-pyc            remove Python file artifacts
clean-test           remove test and coverage artifacts
dist                 builds source and wheel package
docs-setup           setup docs pages based on README.md and HISTORY.md
docs                 build the static version of the docs
docs-serve           serve documentation to livereload while you work
format               run black to format codebase
install              install the package to the active Python's site-packages
lint                 check style with black, flake8, and mypy
release              package and upload a release
reqs                 install development requirements
test                 run tests with mocked cloud SDKs
test-debug           rerun tests that failed in last run and stop with pdb at failures
test-live-cloud      run tests on live cloud backends
perf                 run performance measurement suite for s3 and save results to perf-results.csv
```

Once you have your Python environment, you can install all the dev dependencies with:

```bash
make reqs
```

This will also install an editable version of `cloudpathlib` with all the extras into your environment.

## Tests

### Commands

There is a robust test suite that covers most of the core functionality of the library. There are a few different testing commands that are useful to developers.

The most common way when developing is to run the full test suite with mocked, local backends (no network calls):

```bash
make test
```

If you have a test fail or want to be able to interactively debug if a test fails, you can use a different command. This will run pytest with `pdb`, and `last-fail` so it will drop you into a debugger if a test fails and only run the tests that failed last time:

```bash
make test-debug
```

Finally, you may want to run your tests against live servers to ensure that the behavior against a provider's server is not different from the mocked provider. You will need credentials configured for each of the providers you run against. You can run the live tests with:

```bash
make test-live-cloud
```

### Test rigs

Since we want behavior parity across providers, nearly all of the tests are written in a provider-agnositc way. Each test is passed a test rig as a fixture, and the rig provides the correct way for generating cloudpaths for testing. The test rigs are defined in [`conftest.py`](tests/conftest.py).

**Almost none of the tests instantiate `CloudPath` or a `*Client` class directly.**

When a test suite runs against the rig, the rig does the following steps on setup:
 - Creates a folder on the provider with a unique ID just for this test run.
 - Copies the contents of [`tests/assets/`](tests/assets/) into that folder.
 - Sets the rigs `client_class` using `set_as_default_client` so that any `CloudPath` with the right prefix created in the test will use the rig's client.

When the tests finish, if it is using a live server, the test files will be deleted from the provider.

If you want to speed up your testing during development, you may comment out some of the rigs in [`conftest.py`](tests/conftest.py). Don't commit this change, and make sure you run against all the rigs before submitting a PR.

### Authoring tests

We want our test suite coverage to be comprehensive, so PRs need to add tests if they add new functionality. If you are adding a new feature, you will need to add tests for it. If you are changing an existing feature, you will need to update the tests to match the new behavior.

The tests are written in `pytest`. You can read the [pytest documentation](https://docs.pytest.org/en/stable/) for more information on how to write tests with pytest.

Your best guide will be reading the existing test suite, and making sure you're using the rig when possible.

For example, if you want a `CloudPath` referring to a file that actually exists, you may want to do something like this:

```python
# note that dir_0/file0_0.txt is in tests/assets, so you can expect to exist on the provider
cp = rig.create_cloud_path("dir_0/file0_0.txt")

# if you don't need the file to actually exist, you can use the same method
cp2 = rig.create_cloud_path("path/that/does/not/exist.txt")
```

If you are testing functionality on the `*Client` class, you can get an instance of the class for the rig with:

```python
new_client = rig.client_class()
```

## Documentation

We also aim to have robust and comprehensive documentation. For public API functions, we provide docstrings that explain how things work to end users, and these are automatically built into the documentation. For more complex topics, we write specific documentation.

We use [mkdocs](https://www.mkdocs.org/) to generate the documentation.


### Building

To build the latest version of the documentation, you can run:

```bash
make docs
```

### Serving

While you are developing, you can serve a local version of the docs to see what your changes look like. This will auto-reload for most changes to the docs:

```bash
make docs-serve
```

Note that the main page (`index.md`), the changelog (`HISTORY.md`), and the contributing page (`CONTRIBUTING.md`) are all generated from the files in the project root. If you want to make changes to the documentation, you should make them in the root of the project and then run `make docs-setup` to update the other files. **The dev server does not automatically pick up these changes.** You will need to stop the server and restart it to see the changes.

### Authoring

Documentation pages can either be authored in normal Markdown or in a runnable jupyter notebook. Notebooks are useful for showing examples of how to use the library. You can see an example of a notebook in [`docs/docs/why_cloudpathlib.ipynb`](docs/docs/why_cloudpathlib.ipynb).

Note: generating the documentation **does not** execute any notebooks, it just converts them. You need to restart and run all notebook cells to make sure the notebook executes top-to-bottom and has the latest results before committing it.

### Docstrings

For public APIs, please add a docstring that will appear in the documentation automatically. Since public APIs are type-hinted, there is no need to list the function parameters, their types, and the return types in a specific format in the docstring. Instead, you should describe what the function does and any relevant information for the user.

## Submitting a PR

Once you have everything working as expected locally, submit a PR. The PR will be automatically tested by GitHub Actions. If the tests fail, you will need to fix the issue and push a new commit to the PR. If the tests pass (except the live tests, as noted below), you will need to get a maintainer to review the PR and merge it.

### PR checklist

Here's a checklist from the PR template to make sure that you did all the required steps:

```
 - [ ] I have read and understood `CONTRIBUTING.md`
 - [ ] Confirmed an issue exists for the PR, and the text `Closes #issue` appears in the PR summary (e.g., `Closes #123`).
 - [ ] Confirmed PR is rebased onto the latest base
 - [ ] Confirmed failure before change and success after change
 - [ ] Any generic new functionality is replicated across cloud providers if necessary
 - [ ] Tested manually against live server backend for at least one provider
 - [ ] Added tests for any new functionality
 - [ ] Linting passes locally
 - [ ] Tests pass locally
 - [ ] Updated `HISTORY.md` with the issue that is addressed and the PR you are submitting. If the top section is not `## UNRELEASED``, then you need to add a new section to the top of the document for your change.
```

### PR CI/CD test run

If you are not a maintainer, a maintainer will have to approve your PR to run the test suite in GitHub Actions. No need to ping a maintainer, it will be seen as part of our regular review.

Even once the tests run, two jobs will fail. This is expected. The failures are: (1) The live tests, and (2) the install tests. Both of these require access to the live backends, which are not available to outside contributors. If everything else passes, you can ignore these failiures. A mainter will take the following steps:

 - Create a branch off the main repo for your PR's changes
 - Merge your PR into that new branch
 - Run CI/CD on the repo-local branch which has access to the live backends
 - Confirm the live tests pass as expected. (If not, you will need to fix the issue and create another PR into this reopo-local branch.)
 - Once they pass, merge the repo-local branch into the main branch.

For example, see a [repo-local branch running the live tests in this PR](https://github.com/drivendataorg/cloudpathlib/pull/354).

## Code standards and tips

### Adding dependencies

We want `cloudpathlib` to be as lightweight as possible. Our strong preference is to not take any external dependencies for the library outside of the official software development kit (SDK) for the cloud provider. If you want to add a dependency, please open an issue to discuss it first. Library depencies are tracked in `pyproject.toml`.

Dependencies that are only needed for building documentation, development, linting, formatting, or testing can be added to `requirements-dev.txt`, and are not subject to the same scrutiny.


### Linting and formatting

Any code changes need to follow the code standards of the project. We use `black` for formatting code (as configured in `pyproject.toml`), and `flake8` for linting (as configured in `setup.cfg`).

To apply these styles to your code, you can run:

```bash
make format
```

To ensure that your code is properly formatted and linted and passes type checking, you can run:

```bash
make lint
```

### Type hinting

Any public APIs need to have proper type annotations, which are checked by `mypy` (as configured in `pyproject.toml`) when you run the `make lint` command. These need to pass. If you are adding a new public API, you will need to add type annotations to it. If you are changing an existing public API, you will need to update the type annotations to match the new API. If you are adding a private API, you do not need to add type annotations, but you should consider it. Only use `# type: ignore` or `Any` if there is not other way.

As mentioned, to ensure your code passes `mypy` type checking, you can run:

```bash
make lint
```

#### Interactive testing

To interactively test the library, we recommend creating a Jupyter notebook in the root of the project called `sandbox.ipynb`. We `.gitignore` a `sandbox.ipynb` file by default for this purpose. You can import the library and run commands in the notebook to test functionality. This is useful for testing new features or debugging issues.

It's best to start the notebook with cells to autoreload the library:

```python
%load_ext autoreload
%autoreload 2
```

Then you can import the library and work with the CloudPath class:

```python
from cloudpathlib import CloudPath

cp = CloudPath("s3://my-test-bucket/")
```

### Credentials and cloud access

For certain tests and development scenarios, you will need to have access to the relevant cloud provider. You can put the authentication credentials into a `.env` file in the root of the project. The `.env` file is ignored by git, so you can put your credentials there without worrying about them being committed. See the [authentication documentation](https://cloudpathlib.drivendata.org/stable/authentication/) for information on what variables to set.

If you need to test against a cloud provider you do not have access to, you can reach out to the maintainers, who may be willing to grant credentials for testing purposes.

#### Mocking providers in tests

All of the cloud providers have mocked versions of their SDKs that are used for running local tests. These are in [`tests/mock_clients`](tests/mock_clients). If you are adding a feature that makes a call to the underlying client SDK, and it is not already mocked, you will need to mock it.

In general, these mocks actually store files in a temporary directory on the file system and then use those files to behave like the real versions of the library do. This way we can run full test suites without making any network calls.

The mocks are set up in each rig in `conftest.py` as long as `USE_LIVE_CLOUD` is not set to `"1"`.

#### Performance testing

Listing files with `client._list_dir`, `CloudPath.glob`, `CloudPath.rglob`, and `CloudPath.walk` are all performance-sensitive operations for large directories on cloud storage. If you change code related to any of these methods, make sure to run the performance tests. In your PR description, include the results on your machine prior to making your changes and the results with your changes.

These can be run with `make perf`. This will generate a report like:

```
                                  Performance suite results: (2023-10-08T13:18:04.774823)                                  
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━┓
┃ Test Name      ┃ Config Name                ┃ Iterations ┃           Mean ┃              Std ┃            Max ┃ N Items ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━┩
│ List Folders   │ List shallow recursive     │         10 │ 0:00:00.862476 │ ± 0:00:00.020222 │ 0:00:00.898143 │   5,500 │
│ List Folders   │ List shallow non-recursive │         10 │ 0:00:00.884997 │ ± 0:00:00.086678 │ 0:00:01.117775 │   5,500 │
│ List Folders   │ List normal recursive      │         10 │ 0:00:01.248844 │ ± 0:00:00.095575 │ 0:00:01.506868 │   7,877 │
│ List Folders   │ List normal non-recursive  │         10 │ 0:00:00.060042 │ ± 0:00:00.003986 │ 0:00:00.064052 │     113 │
│ List Folders   │ List deep recursive        │         10 │ 0:00:02.004731 │ ± 0:00:00.130264 │ 0:00:02.353263 │   7,955 │
│ List Folders   │ List deep non-recursive    │         10 │ 0:00:00.054268 │ ± 0:00:00.003314 │ 0:00:00.062116 │      31 │
│ Glob scenarios │ Glob shallow recursive     │         10 │ 0:00:01.056946 │ ± 0:00:00.160470 │ 0:00:01.447082 │   5,500 │
│ Glob scenarios │ Glob shallow non-recursive │         10 │ 0:00:00.978217 │ ± 0:00:00.091849 │ 0:00:01.230822 │   5,500 │
│ Glob scenarios │ Glob normal recursive      │         10 │ 0:00:01.510334 │ ± 0:00:00.101108 │ 0:00:01.789393 │   7,272 │
│ Glob scenarios │ Glob normal non-recursive  │         10 │ 0:00:00.058301 │ ± 0:00:00.002621 │ 0:00:00.063299 │      12 │
│ Glob scenarios │ Glob deep recursive        │         10 │ 0:00:02.784629 │ ± 0:00:00.099764 │ 0:00:02.981882 │   7,650 │
│ Glob scenarios │ Glob deep non-recursive    │         10 │ 0:00:00.051322 │ ± 0:00:00.002653 │ 0:00:00.054844 │      25 │
│ Walk scenarios │ Walk shallow               │         10 │ 0:00:00.905571 │ ± 0:00:00.076332 │ 0:00:01.113957 │   5,500 │
│ Walk scenarios │ Walk normal                │         10 │ 0:00:01.441215 │ ± 0:00:00.014923 │ 0:00:01.470414 │   7,272 │
│ Walk scenarios │ Walk deep                  │         10 │ 0:00:02.461520 │ ± 0:00:00.031832 │ 0:00:02.539132 │   7,650 │
└────────────────┴────────────────────────────┴────────────┴────────────────┴──────────────────┴────────────────┴─────────┘
```

To see how it is used in PR, you can [see an example here](https://github.com/drivendataorg/cloudpathlib/pull/364).

### Exceptions

Different backends may raise different exception classses when something goes wrong. To make it easy for users to catch exceptions that are agnostic of the backend, we generally will catch and raise a specific exception from [`exceptions.py`](cloudpathlib/exceptions.py) for any exception that we understand. You can add new exceptions to this file if any are needed for new features.



## Code architecture

The best guide to the style and architecture is to read the code itself, but we provide some overarching guidance here.

### Cloud provider abstraction

We want to support as many providers as possible. Therefore, our goal is to keep the surface area of the `*Client` class small so it is easy to build extensions and new backends.

Generic functionality like setting defaults and caching are implemented in the [`Client` class](cloudpathlib/client.py). This also defines the interface that the `*Client` backends for each provider needs to implement.

Each provider has its own `*Client` class that implements the interface defined in `Client`. The `*Client` classes are responsible for implementing the interface defined in `Client` for that specific backend. For an example, you could look at the [`S3Client`](cloudpathlib/s3/s3client.py).


### `CloudPath` abstraction

The core [`cloudpath.py`](cloudpathlib/cloudpath.py) file provides most of the method implementations in a provider-agnostic way. Most feature changes will happen in the `CloudPath` class, unless there is a provider specific issue. There are a number of ways that functionality gets implemented:

 - Some methods are implemented from scratch for cloud paths
 - Some methods are implemented by calling the `pathlib.Path` version on either (1) the file in the cache if concrete, or (2) a `PurePosixPath` conversion of the CloudPath if not concrete.
 - Some methods that are not relevant for a cloud backend are not implemented.

Any code that needs to interact with the provider does so by calling methods on the `CloudPath.client`, which is an instance of the `Client` class so all the methods are provider-agnostic.

Some methods are implemented on the `*Path` class for the specific provider. This is reserved for two cases: (1) provider-specific properties, like `S3Path.bucket` or `AzureBlobPath.container`, and (2) methods that are more efficiently implemented in a provider-specific way, like `S3Path.stat`. 

### Adding a new provider

Adding a new provider is relatively straightforward. If you are extending `cloudpathlib`, but don't intend to make your provider part of the core library, implement the following pieces.

#### A `*Client` class that inherits from `Client`

```python
from cloudpathlib.client import Client

class MyClient(Client):
    # implementation here...
```

#### A `*Path` class that inherits from `CloudPath`

```python
from cloudpathlib.cloudpath import CloudPath

class MyPath(CloudPath):
    cloud_prefix: str = "my-prefix://"
    client: "MyClient"

    # implementation here...
```

This `*Path` class should also be registered, if you want dispatch from `CloudPath` to work (see the next section).

If you do intend to make your provider part of the code library, you will also need to do the following steps:

 - Export the client and path classes in `cloudpathlib/__init__.py`
 - Write a mock backend for local backend testing in `tests/mock_clients`
 - Add a rig to run tests against the backend in `tests/conftest.py`
 - Ensure documentation is updated

### Register your provider for dispatch from `CloudPath`

Register your provider so `CloudPath("my-prefix://my-bucket/my-file.txt")` will return a `MyPath` object:

```python
from cloudpathlib.client import Client, register_client_class
from cloudpathlib.cloudpath import CloudPath, register_path_class

@register_client_class("my-prefix")
class MyClient(Client):
    # implementation here...

@register_path_class("my-prefix")
class MyPath(CloudPath):
    cloud_prefix: str = "my-prefix://"
    client: "MyClient"

    # implementation here...
```

If you are submitting a PR to add this to the official `cloudpathlib` library, you will also need to do the following steps:

 - Export the client and path classes in `cloudpathlib/__init__.py`
 - Write a mock backend for local backend testing in `tests/mock_clients`
 - Add a rig to run tests against the backend in `tests/conftest.py`
 - Ensure documentation is updated

## Governance

Ultimately, the maintainers of `cloudpathlib` need to use their discretion when accepting new features. Proposed contributions may not be accepted for a variety of reasons. Some proposed contributions to the library may be judged to introduce too large a maintenance burden. Some proposed contributions may be judged to be out of scope for the project. Still other contributions may be judged to to not fit the API for stylistic reasons or the technical direction of the project.

We appreciate your understanding if your contribution is not accepted. The maintainers will do our best to explain our reasoning, but ultimately we need to make decisions that we feel are in the best interest of the project as a whole. It can be frustrating if you don't agree, but we hope you will understand that we are trying to make the best decisions we can.
