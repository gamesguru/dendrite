---
title: Building/Installing Dendrite
description: How to build and install Dendrite from source
---

Dendrite has numerous utility commands in addition to the actual server binaries.
Build them all from the root of the source repo with:

```sh
go build -o bin/ ./cmd/...
```

The resulting binaries will be placed in the `bin` subfolder.

## Installing Dendrite

You can install the Dendrite binary into `$GOPATH/bin` by using `go install`:

```sh
go install ./cmd/dendrite
```

Alternatively, you can specify a custom path for the binary to be written to using `go build`:

```sh
go build -o /usr/local/bin/ ./cmd/dendrite
```
