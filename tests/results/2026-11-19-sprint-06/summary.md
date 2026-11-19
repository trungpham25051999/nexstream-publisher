# Sprint 06 actual results

| Classification | Count |
|---|---:|
| PASS | 17 |
| FAIL | 0 |
| BLOCKED | 0 |
| NOT RUN | 1 |

- Debug, Release and final ASan/UBSan: 7/7 CTest executables passed each.
- Full local pipeline: eight Kafka records, key `pg-host`, real CPU/RAM JSON, zero successful-queue rows.
- Wrong hostname retained 3 pending records; untrusted CA retained 11 pending records.
- Compose CLI topology is NOT RUN because the plugin is unavailable; equivalent services ran manually.
- GPU/Disk/Network are `UNSUPPORTED` under Protobuf v1.

Initial ASan found a 102,408-byte SQLite constructor failure-path leak. The constructor now closes
partially opened handles; the final sanitizer run passed.
