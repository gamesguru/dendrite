---
title: Contributing
description: Guidelines for contributing to Zendrite development
---

Everyone is welcome to contribute to Zendrite!
We aim to make it as easy as possible to get started.

## Contribution types

We are a small team maintaining a large project.
As a result, we cannot merge every feature, even if it is bug-free and useful, because we then commit to maintaining it indefinitely.

We will accept the following with caveats:

- documentation fixes, provided they do not add additional instructions which can end up going out-of-date, e.g example configs, shell commands.
- performance fixes, provided they do not add significantly more maintenance burden.
- additional functionality on existing features, provided the functionality is small and maintainable.
- additional functionality that, in its absence, would impact the ecosystem e.g spam and abuse mitigations.
- test-only changes, provided they help improve coverage or test tricky code.

The following items are at risk of not being accepted:

- Configuration or CLI changes, particularly ones which increase the overall configuration surface.

## Getting up and running

See the [Installation](/installation/planning) section for information on how to build an instance of Zendrite.
You will likely need this in order to test your changes.

## Pre-commit hooks

This project uses [pre-commit](https://pre-commit.com/) to run checks before each commit.
Install the hooks with:

```bash
pre-commit install
```

The hooks run automatically on `git commit` and include:

- **golangci-lint** — Go linting and formatting (see below)
- **markdownlint** — Markdown style enforcement
- **prettier** — Code formatting for non-Go files (YAML, JS, CSS, etc.)
- **yamllint** — YAML validation
- **hadolint** — Dockerfile linting
- **editorconfig-checker** — Consistent editor settings

## Code style

Go formatting is handled automatically by `golangci-lint`, which runs `gofmt`, `gofumpt`, and `gci` (import ordering).
You do not need to run these manually — pre-commit will apply them on each commit.

Import ordering follows the `gci` convention configured in `.golangci.yaml`:

1. Standard library
2. Third-party packages
3. `codefloe.com/pat-s/zendrite` (local packages)

## Comments

Please make sure that the comments adequately explain _why_ your code does what it does.
If there are statements that are not obvious, please comment what they do.

We also have some special tags which we use for searchability:

- `// TODO:` for places where a future review, rewrite or refactor is likely required.
- `// FIXME:` for places where we know there is an outstanding bug that needs a fix.
- `// NOTSPEC:` for places where the behaviour specifically does not match what the [Matrix Specification](https://spec.matrix.org/) prescribes, along with a description of _why_ that is the case.

## Linting

Linting is configured in `.golangci.yaml` and runs via [golangci-lint](https://github.com/golangci/golangci-lint).
It runs automatically as a pre-commit hook, but you can also run it manually:

```bash
golangci-lint run
```

If you are receiving linter warnings that you are certain are spurious and want to silence them, you can annotate the relevant lines or methods with a `// nolint:` comment.
Please avoid doing this if you can.

## Unit tests

We also have unit tests which we run via:

```bash
go test -race ./...
```

This runs both SQLite and PostgreSQL tests.
If you wish to execute Postgres tests, you'll either need to have Postgres installed locally (`createdb` will be used) or have a remote/containerized Postgres instance available.

To configure the connection to a remote Postgres, you can use the following environment variables:

```bash
POSTGRES_USER=postgres
POSTGRES_PASSWORD=yourPostgresPassword
POSTGRES_HOST=localhost
POSTGRES_DB=postgres # the superuser database to use
```

In general, we like submissions that come with tests.
Anything that proves that the code is functioning as intended is great, and to ensure that we will find out quickly in the future if any regressions happen.

We use the standard [Go testing package](https://gobyexample.com/testing) for this, alongside some helper functions in our own `test` package.

## Continuous integration

When a Pull Request is submitted, continuous integration jobs are run automatically by Forgejo Actions to ensure that the code builds and works.
CI will run `golangci-lint` and then execute unit tests split by package group (syncapi, clientapi, roomserver, userapi, federationapi, and other) with both SQLite and PostgreSQL.

You can see the progress of any CI jobs at the bottom of the Pull Request page, or by looking at the Actions tab of the repository.

We generally won't accept a submission unless all of the CI jobs are passing.
We do understand though that sometimes the tests get things wrong — if that's the case, please also raise a pull request to fix the relevant tests!

### Running checks locally

To save waiting for CI to finish after every commit, it is ideal to run the checks locally before pushing, fixing errors first.
This also saves other people time as only so many PRs can be tested at a given time.

The simplest way is to rely on the pre-commit hooks, which run `golangci-lint` and other checks on each commit.
You can also run them manually against all files:

```bash
pre-commit run --all-files
```

To run the full test suite locally:

```bash
go test -race ./...
```

If these steps report no problems, the code should be able to pass the CI tests.
