---
title: Coverage
description: Running tests with code coverage analysis
---

## Running unit tests with coverage enabled

Running unit tests with coverage enabled can be done with the following commands.
This will generate an `integrationcover.log`:

```bash
go test -covermode=atomic -coverpkg=./... -coverprofile=integrationcover.log $(go list ./... | grep -v '/cmd/')
go tool cover -func=integrationcover.log
```

To view coverage as an HTML report:

```bash
go tool cover -html=integrationcover.log -o coverage.html
```
