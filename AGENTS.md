# Agent Instructions for `cloudpathlib`

This file defines the default operating guide for coding agents working in this repository.

## 1. Source of truth

Follow repository standards in this order:

1. `CONTRIBUTING.md`
2. `.github/pull_request_template.md`
3. CI workflows in `.github/workflows/*.yml`
4. Tooling config: `pyproject.toml`, `setup.cfg`, `Makefile`, `requirements-dev.txt`
5. `.env.example` and `docs/docs/authentication.md`
6. Existing patterns in `cloudpathlib/`, `tests/`, and `tests/mock_clients/`

Notes:

- Edit root docs as the source of truth: `README.md`, `HISTORY.md`, and `CONTRIBUTING.md`.
- `docs/docs/index.md`, `docs/docs/changelog.md`, and `docs/docs/contributing.md` are generated
  from those root files via `make docs-setup` / `make docs`.
- Do not introduce new workflows, linters, formatters, or build tools unless explicitly
  requested.

## 2. Scope and contributor policy

- Keep changes focused and task-specific.
- Generic functionality should be replicated across providers when appropriate.
- If working as an outside contributor, make sure an issue exists and has maintainer sign-off
  before starting substantial work.

## 3. Compatibility guardrails

- CI covers Linux, macOS, and Windows.
- CI covers Python 3.9 through 3.14.
- Preserve the repository's existing compatibility patterns (`typing_extensions`, version guards,
  `pathlib` shims) instead of rewriting toward newer-version-only syntax.
- Do not introduce Python 3.10+/3.11+ syntax or stdlib dependencies unless they are properly
  guarded and compatible with the supported matrix.

## 4. Architecture and exceptions

- Put provider-agnostic behavior in `cloudpathlib/cloudpath.py` or `cloudpathlib/client.py` when
  possible.
- Keep provider `*Path` / `*Client` code focused on backend-specific properties or materially
  better backend-specific implementations.
- When a failure mode is understood and should be backend-agnostic, translate provider errors to
  `cloudpathlib.exceptions`.
- If adding or extending a provider, update dispatch/registration, exports, tests, mocks, and docs
  together.

## 5. Required local commands

- Install dev dependencies with `make reqs`.
- Use the existing project commands:
  - `make format`
  - `make lint`
  - `make test`
  - `make test-live-cloud`
  - `make docs`
  - `make dist`
  - `make perf`
- Use `make dist` whenever a change touches packaging, optional dependencies, imports, or
  installability.
- If CloudPath method support changes, run `python docs/make_support_table.py` and update
  `README.md`.
- Notebook-backed docs are not executed by the docs build; if you edit a notebook under
  `docs/docs/`, re-run all cells before commit.

## 6. Testing guidance

- Default development loop: `make test`.
- Prefer the provider-agnostic rigs from `tests/conftest.py`.
- In tests, use `rig.create_cloud_path(...)` and rig-backed clients unless the test is
  specifically about dispatch, direct instantiation, or client construction.
- If a change adds or alters SDK calls, update the corresponding mock implementation in
  `tests/mock_clients/`.
- Temporary local shortcuts like commenting out rigs in `tests/conftest.py` are fine during
  development, but must not be committed.
- Validate affected providers, not just one backend.
- For performance-sensitive changes (`_list_dir`, `glob`, `rglob`, `walk`), run `make perf` and
  include before/after results in PR notes.

## 7. Live backend validation

- Encourage contributors to run live backend tests locally when they have access.
- Copy `.env.example` to `.env` and use `docs/docs/authentication.md` as the canonical credential
  guide.
- Common live-test variables include:
  - AWS: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`, `AWS_ENDPOINT_URL`,
    `LIVE_S3_BUCKET`
  - Azure: `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_STORAGE_GEN2_CONNECTION_STRING`,
    `LIVE_AZURE_CONTAINER`, `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`
  - GCS: `GOOGLE_APPLICATION_CREDENTIALS`, `GCP_PROJECT_ID`, `GCP_SA_KEY`, `LIVE_GS_BUCKET`
  - Custom S3: `CUSTOM_S3_BUCKET`, `CUSTOM_S3_ENDPOINT`, `CUSTOM_S3_KEY_ID`,
    `CUSTOM_S3_SECRET_KEY`
- Run live tests with `USE_LIVE_CLOUD=1 make test-live-cloud`.
- Live tests create and delete cloud files.
- If live backend access is unavailable, run mocked tests locally and document the live-test gap
  clearly for maintainers.

## 8. Commit and PR hygiene

Before each commit:

1. Inspect the staged diff and keep only task-relevant files.
2. Confirm no secrets or local credential files are included (`.env`, keys, tokens, bucket
   creds).
3. Confirm the appropriate formatting, linting, tests, and docs updates were completed for the
   changed surface.
4. Make atomic commits with clear intent.

Before opening or updating a PR:

1. Rebase onto the latest base branch.
2. Confirm an issue exists and include `Closes #<issue>` in the PR body.
3. Confirm the change is demonstrated by failure-before/fix-after evidence.
4. Update `HISTORY.md` under `## UNRELEASED` to satisfy this repo's PR checklist; include
   issue and PR references when they are known.
5. Verify API changes include doc/docstring updates and generated-doc refresh as needed.
6. If working from an external contributor context, note any live-backend limitations that
   maintainers need to validate on a repo-local branch.
