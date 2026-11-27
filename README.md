# nexstream-publisher

[![CI](https://github.com/trungpham25051999/nexstream-publisher/actions/workflows/ci.yml/badge.svg)](https://github.com/trungpham25051999/nexstream-publisher/actions/workflows/ci.yml) ![C++20](https://img.shields.io/badge/C%2B%2B-20-00599C?logo=cplusplus) ![SQLite](https://img.shields.io/badge/queue-SQLite%20WAL-003B57?logo=sqlite) [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Sprint 06 provides `nexstream-agent`, a durable Linux monitoring Agent, while retaining the
one-shot `nexstream-publisher` protocol test tool.

## Scope warning

The Agent uses nonblocking OpenSSL TLS 1.3 with mandatory chain and hostname verification, SQLite
WAL (`synchronous=FULL`), real Linux `/proc` CPU/RAM collection and bounded retry/draining. GPU,
disk and network are explicitly `UNSUPPORTED` because contracts v1 has no fields for them.

## Build and test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON \
  -DCMAKE_PREFIX_PATH=/absolute/path/to/nexstream-contracts-prefix \
  -DSQLITE3_ROOT=/absolute/path/to/sqlite-development-prefix \
  -DNEXSTREAM_INGEST_BINARY=/absolute/path/to/nexstream-ingest
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
```

Run the Agent through HAProxy:

```sh
./build/nexstream-agent \
  --address 127.0.0.1 --port 9443 \
  --tls-hostname gateway.local --ca-file /run/secrets/nexstream-ca.pem \
  --queue-db /var/lib/nexstream/agent.db \
  --agent-id agent-01 --host-id host-01 --key-id key-2026-01 \
  --secret-file /run/secrets/nexstream-api-key
```

Exit codes: `0` confirmed SUCCESS, `3` retry/unresolved, `4` permanent failure, `5` protocol
failure. The default transaction deadline is 10 seconds and covers connect, authentication, batch
send, and BatchAck receive.

Limits: 64 KiB frame, 1 MiB aggregate batch, 50 records, 128-byte identifiers, 512-byte secret,
and bounded receive/write buffers. Numeric IPv4 avoids blocking DNS; TLS hostname verification is
independent and mandatory in the Agent.

Queue defaults: maximum 24-hour pending retention, 256 MiB logical quota, 16 MiB filesystem
headroom, 50 records/batch and drop-oldest pending under quota pressure. Permanent failures are
quarantined. One publisher thread guarantees one active batch; drain attempts are rate limited and
failures use capped exponential backoff with jitter.

See [status](docs/STATUS.md), [process](docs/PROCESS.md), and
[executed tests](tests/results/README.md).
