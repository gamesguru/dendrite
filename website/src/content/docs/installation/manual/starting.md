---
title: Starting Zendrite
description: Running your Zendrite server
---

Once you have completed all preparation and installation steps, you can start your Zendrite deployment by executing the `zendrite` binary:

```bash
./zendrite -config /path/to/zendrite.yaml
```

By default, Zendrite will listen HTTP on port 8008.
If you want to change the addresses or ports that Zendrite listens on, you can use the `-http-bind-address` and `-https-bind-address` command line arguments:

```bash
./zendrite -config /path/to/zendrite.yaml \
    -http-bind-address 1.2.3.4:12345 \
    -https-bind-address 1.2.3.4:54321
```
