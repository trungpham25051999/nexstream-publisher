# nexstream-publisher process through Sprint 06

`nonblocking connect → verified TLS 1.3 → AuthRequest → AuthResult → BatchStart → Telemetry* → BatchEnd → BatchAck`.
One monotonic 10-second deadline covers the complete transaction. Socket operations use `poll`,
retry `EINTR`, wait after `EAGAIN`, and preserve partial read/write state.

The input Batch is immutable. SUCCESS releases confirmed records. UNKNOWN, RETRYABLE_FAILURE,
RETRY_LATER, disconnect, and timeout return stable message IDs for retry. PERMANENT_FAILURE is not
automatically retried. Conflicting/mismatched ACKs fail safe as protocol errors and retain identity.

`expected_count=0` means unspecified; BatchEnd always sends the actual record count. Generated
Protobuf code remains in the build tree from unchanged Sprint 01 schemas.

Evidence: `tests/results/2026-11-13-sprint-03/`. There is no persistence or automatic retry loop;
the caller owns retained records.

Sprint 06 adds `nexstream-agent`: steady-clock sampling commits stable CSPRNG message IDs to SQLite
before network work. A separate publisher thread fetches at most 50 oldest pending records.
Kafka-confirmed SUCCESS deletes, UNKNOWN/retry/timeout remains pending, and permanent failure moves
to quarantine. Queue or network failure cannot block sampling. Sprint 06 evidence is under
`tests/results/2026-11-19-sprint-06/`.
