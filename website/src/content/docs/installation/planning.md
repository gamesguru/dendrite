---
title: Planning your installation
description: Database choices, system requirements, and dependencies for Zendrite
---

## Database

Zendrite can run with either a PostgreSQL or a SQLite backend.
There are considerable tradeoffs to consider:

- **PostgreSQL**: Needs to run separately to Zendrite, needs to be installed and configured separately and will use more resources over all, but will be **considerably faster** than SQLite.
  PostgreSQL has much better write concurrency which will allow Zendrite to process more tasks in parallel.
  This will be necessary for federated deployments to perform adequately.

- **SQLite**: Built into Zendrite, therefore no separate database engine is necessary and is quite a bit easier to set up, but will be much slower than PostgreSQL in most cases.
  SQLite only allows a single writer on a database at a given time, which will significantly restrict Zendrite's ability to process multiple tasks in parallel.

At this time, we **recommend the PostgreSQL database engine** for all production deployments.

## Requirements

Zendrite will run on Linux, macOS and Windows Server.
It should also run fine on variants of BSD such as FreeBSD and OpenBSD.
We have not tested Zendrite on AIX, Solaris, Plan 9 or z/OS — your mileage may vary with these platforms.

It is difficult to state explicitly the amount of CPU, RAM or disk space that a Zendrite installation will need, as this varies considerably based on a number of factors.
In particular:

- The number of users using the server;
- The number of rooms that the server is joined to — federated rooms in particular will typically use more resources than rooms with only local users;
- The complexity of rooms that the server is joined to — rooms with more members coming and going will typically be of a much higher complexity.

Some tasks are more expensive than others, such as joining rooms over federation, running state resolution or sending messages into very large federated rooms with lots of remote users.
Therefore you should plan accordingly and ensure that you have enough resources available to endure spikes in CPU or RAM usage, as these may be considerably higher than the idle resource usage.

At an absolute minimum, Zendrite will expect 1GB RAM.
For a comfortable day-to-day deployment which can participate in federated rooms for a number of local users, be prepared to assign 2-4 CPU cores and 8GB RAM — more if your user count increases.

If you are running PostgreSQL on the same machine, allow extra headroom for this too, as the database engine will also have CPU and RAM requirements of its own.
Running too many heavy services on the same machine may result in resource starvation and processes may end up being killed by the operating system if they try to use too much memory.

## Dependencies

In order to install Zendrite, you will need to satisfy the following dependencies.

### Go

At this time, Zendrite supports being built with Go 1.25 or later.
We do not support building Zendrite with older versions of Go than this.
If you are installing Go using a package manager, you should check (by running `go version`) that you are using a suitable version before you start.

### PostgreSQL

If using the PostgreSQL database engine, you should install PostgreSQL 12 or later.

### NATS Server

Zendrite comes with a built-in [NATS Server](https://github.com/nats-io/nats-server) and therefore does not need this to be manually installed.

### Reverse proxy

A reverse proxy such as [Caddy](https://caddyserver.com), [NGINX](https://www.nginx.com) or [HAProxy](http://www.haproxy.org) is useful for deployments.
Configuring these is not covered in this documentation.

### Windows

Finally, if you want to build Zendrite on Windows, you will need `gcc` in the path.
The best way to achieve this is by installing and building Zendrite under [MinGW-w64](https://www.mingw-w64.org/).
