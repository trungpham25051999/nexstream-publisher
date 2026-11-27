# Building nexstream-publisher

Requirements: Linux, CMake 3.20+, C++20, Protobuf 3.21+, OpenSSL and SQLite3 development packages
(Ubuntu 24.04: `protobuf-compiler libprotobuf-dev libssl-dev libsqlite3-dev`), the installed
`nexstream-contracts` v0.1.0 package (see `nexstream-ingest/BUILDING.md`) and a built
`nexstream-ingest` gateway binary for the gateway integration test.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON \
  -DCMAKE_PREFIX_PATH="$HOME/.local/nexstream-contracts" \
  -DNEXSTREAM_INGEST_BINARY=/absolute/path/to/nexstream-ingest
cmake --build build --parallel 2
ctest --test-dir build -L unit --output-on-failure      # unit suite (what CI runs)
ctest --test-dir build -L integration --output-on-failure  # needs the authenticated gateway stack
```

The integration test starts the gateway, which requires PostgreSQL and Kafka from
`nexstream-deploy`. CI pins the gateway to `nexstream-ingest` tag `v0.5.0`.
