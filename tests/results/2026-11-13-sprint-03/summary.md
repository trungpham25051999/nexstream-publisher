# Sprint 03 test execution

- Date: 2026-11-13
- Revision: local uncommitted `main` workspace
- Environment: Debian GNU/Linux, kernel 6.1.0-40-amd64, x86_64
- Toolchain: GCC 12.2.0, CMake 3.25.1, Protobuf 3.21.12
- Integration dependency: corresponding Debug/Release/ASan Sprint 02 Gateway binaries
- Contract schema hashes unchanged from Sprint 02 evidence

| Test ID | Observed | Status |
|---|---|---|
| TC-PUB-001 | nonblocking client completed transaction with real Gateway | PASS |
| TC-PUB-002 | real EAGAIN + EINTR + partial send preserved all bytes | PASS |
| TC-PUB-003 | one-byte response fragments decoded | PASS |
| TC-PUB-004 | auth and batch sequence accepted | PASS |
| TC-PUB-005 | SUCCESS and PERMANENT_FAILURE mapped correctly | PASS |
| TC-PUB-006 | UNKNOWN/retryable results retained stable IDs | PASS |
| TC-PUB-007 | timeout/disconnect produced retry outcome with unchanged IDs | PASS |
| TC-PUB-008 | count/ID/batch byte limits rejected | PASS |
| TC-PUB-009 | expected_count=0 and actual BatchEnd count interoperated | PASS |
| TC-PUB-010 | malformed framing and conflicting ACK failed safe | PASS |

Totals for Sprint 03 cases: **PASS 10, FAIL 0, BLOCKED 0, NOT RUN 0**.

Debug, Release, and ASan/UBSan each passed 4/4 CTest executables; Gateway integration also passed
five consecutive Debug repetitions. Kafka, TLS, PostgreSQL,
SQLite, real collectors, load tests, and benchmarks were not run and are outside Sprint 03.
