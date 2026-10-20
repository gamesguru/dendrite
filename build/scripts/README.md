# Dev Scripts

These scripts help with Zendrite development.

Run `./build/scripts/build-test-lint.sh` to build all commands, lint the repository, and run the unit tests with the race detector.
The script requires Go and `prek` on `PATH` and can be invoked from any working directory.
Builds and tests use the `goolm` tag, matching the `just` recipes.

To run linting alone, use `just lint` or `prek run golangci-lint --all-files` from the repository root.
`prek` manages the golangci-lint version pinned in `.pre-commit-config.yaml`, using the checked-in v2 configuration and its `test` and `goolm` build tags.
The obsolete `find-lint.sh` installer and its `fast` argument have been removed.
Linting does not use `--fix` or back up and overwrite module files.
Set `GOGC` to control garbage collection during linting if needed.
