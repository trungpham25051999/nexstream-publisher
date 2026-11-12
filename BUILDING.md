# Building nexstream-publisher

Requires Linux, CMake 3.20+, C++20, Protobuf 3.21+, a reviewed contracts checkout, and the built
Sprint 02 Gateway executable for integration tests.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON \
  -DNEXSTREAM_CONTRACTS_ROOT=/absolute/path/to/nexstream-contracts \
  -DNEXSTREAM_INGEST_BINARY=/absolute/path/to/nexstream-ingest
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
```

Contract and Gateway paths are explicit interim workspace dependencies. Tagged/installable
artifacts remain required before independent release.
