# Sprint 03 test cases

| ID | Scenario | Actual observation | Status |
|---|---|---|---|
| TC-PUB-001 | Nonblocking connect/transaction | Connected to real Sprint 02 Gateway and completed protocol | PASS |
| TC-PUB-002 | Partial write, EINTR, EAGAIN | Real full send queue resumed and delivered every byte | PASS |
| TC-PUB-003 | Fragmented receive | One-byte fragments reconstructed into AuthResult | PASS |
| TC-PUB-004 | Auth then batch sequence | Gateway accepted Auth, expected_count=0, telemetry and BatchEnd | PASS |
| TC-PUB-005 | SUCCESS/PERMANENT handling | SUCCESS released; permanent result disabled automatic retry | PASS |
| TC-PUB-006 | UNKNOWN/retryable handling | UNKNOWN, RETRYABLE_FAILURE and RETRY_LATER retained IDs | PASS |
| TC-PUB-007 | Timeout/disconnect | 100 ms fixture deadline and disconnect returned retry with stable IDs | PASS |
| TC-PUB-008 | Frame/batch/memory limits | 51 records, duplicate/oversize ID and tiny batch budget rejected | PASS |
| TC-PUB-009 | Actual BatchEnd count | Two records with unspecified expected count accepted by Gateway | PASS |
| TC-PUB-010 | Malformed/mismatched response | Zero-length framing and conflicting/mismatched ACK rejected safely | PASS |

The 100 ms timeout is a deterministic test override. Production/default transaction deadline is
10 seconds. No throughput, latency percentile, CPU, RSS, or load benchmark was run.

## Sprint 06

| ID | Scenario | Actual observation | Status |
|---|---|---|---|
| TC-AGENT-001 | TLS 1.3 positive | Agent verified ephemeral CA and `localhost`, then published through HAProxy | PASS |
| TC-AGENT-002 | Hostname mismatch | `wrong.example` failed; queued identities remained | PASS |
| TC-AGENT-003 | Untrusted CA | Unknown CA failed; queued identities remained | PASS |
| TC-AGENT-004 | Nonblocking TLS deadline/I/O | WANT_READ/WANT_WRITE use poll under original transaction deadline | PASS |
| TC-AGENT-005 | WAL durability | WAL/FULL close-reopen retained records | PASS |
| TC-AGENT-006 | Crash transaction | SIGKILL rolled back partial row and preserved prior commit | PASS |
| TC-AGENT-007 | ACK retention rules | SUCCESS deleted, UNKNOWN retained, permanent result quarantined | PASS |
| TC-AGENT-008 | Retention/drop-oldest | 24-hour cleanup and quota evicted oldest pending first | PASS |
| TC-AGENT-009 | Disk-full/headroom | Tiny quota and `/dev/full` failed safely | PASS |
| TC-AGENT-010 | Stable identity | Duplicate ID was idempotent; retry retained original ID | PASS |
| TC-AGENT-011 | Linux collector | Real `/proc` CPU/RAM values satisfied v1 bounds | PASS |
| TC-AGENT-012 | Unsupported adapters | GPU/Disk/Network reported `UNSUPPORTED`; no fake values emitted | PASS |
| TC-AGENT-013 | Scheduler isolation | Sampling continued while publishing blocked | PASS |
| TC-AGENT-014 | Backoff/jitter | Exponential delay, cap, reset and normal jitter were bounded | PASS |
| TC-AGENT-015 | One active batch/rate limit | Single publisher thread maintained one batch and minimum drain interval | PASS |
| TC-AGENT-016 | Full local pipeline | Agent→HAProxy→Gateway→PG→Kafka produced eight records; queue emptied | PASS |
| TC-AGENT-017 | Measurement probe | Collector and SQLite usage measured on this host | PASS |
| TC-AGENT-018 | Docker Compose CLI topology | Compose plugin unavailable; equivalent services ran manually | NOT RUN |

GPU/Disk/Network are capability statuses rather than fabricated telemetry. The measurement probe is
a local micro-measurement, not a capacity or production benchmark.
