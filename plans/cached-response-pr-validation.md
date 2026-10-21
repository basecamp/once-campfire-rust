# Early authenticated cache: cold and mixed workload validation

The early cache proposal builds on [cached-read preparation and header-key hardening](https://github.com/basecamp/once-campfire-rust/pull/49).
It reuses the completed pipeline's positive authority only for identical requests
within the same observed SQLite generation, signed-cookie validity and hourly
activity window. It does not introduce a time-only authentication cache.
Eligibility is deliberately conservative: conditional validators, HEAD, bodies,
ranges, upgrades, cache-bypass directives and encrypted session data continue
through the existing kit pipeline.

The proposal also records room preference side effects as `LastRoomPreference`
controller metadata and uses one cookie factory for ordinary visits and early
hits. A final response-cookie allowlist rejects unexpected, missing or mismatched
cookie effects. Preference values are no longer inferred from response headers.

## Motivation from the C# port

This investigation was prompted by the recent
[devmobasa/once-campfire-dotnet C# port](https://github.com/devmobasa/once-campfire-dotnet),
pinned to `3286f07aa8d00b21e749b8c8c03eb3d5ad3c2ce1`. Its
[`PageCacheHitMiddleware`](https://github.com/devmobasa/once-campfire-dotnet/blob/3286f07aa8d00b21e749b8c8c03eb3d5ad3c2ce1/src/Campfire.Web/Views/Caching/PageCacheHitMiddleware.cs),
`PageTicketMemo` and `PageAuthorityCache` showed why it was ahead of the starting
Rust implementation: it could reuse admitted authority and encoded responses
before routing, while Rust rebuilt the routed context and repeated authentication
and resource queries before reaching its response cache. Both still observed
SQLite commits. The useful insight was where the cache hit occurred and which
proven authority could be reused, rather than a compiler-setting difference.

The Rust implementation follows that architectural insight through its existing
signed-cookie verifier, immutable per-process secrets, SQLite generation observer
and bounded response store. It does not copy C#'s dynamic key-ring mechanism.

## Checks

805 Rust workspace tests pass, with 11 pre-existing ignored, using both
`CAMPFIRE_REQUIRE_SEED=1` and `CAMPFIRE_REQUIRE_MEDIA_VECTORS=1`. Formatting and
workspace/all-target Clippy with `-D warnings` pass. Regression coverage includes
session revocation/token rotation, permission changes, foreign commits, delayed
renders, cookie expiry, activity deadlines, clock rollback, flash, HEAD,
validators, encodings, repeated-header grouping and unexpected cookie effects.

## Additional workload method

The two normal release binaries are preparation `d7a6790` and the early-cache
implementation at `d4ad287`. The run's source/runner receipt is `4a8aae7`; raw
metadata records their SHA-256 hashes, seed hash and shared verification revision
`ec02debb3e44f40b8778a6a42db3c4515b2f9eaf`. The release profile is unchanged and
profiler/allocator instrumentation is disabled.

Both use server CPUs 0–1, generator CPUs 2–3, two Tokio workers and the same
four-CPU cgroup quota. Each condition starts a fresh process and Rails-built seed
copy. Two distinct users (`david`, `jason`) are verified by SQLite user IDs and
membership queries, then separately signed in. The shared contract builder's
legacy current-email label is supplied with each actual user's email so its SQL
expectations are identity-specific; the shared client and validators are unchanged.

Mixed reads use 16 total connections, eight per user, 5 seconds of warmup and
20-second measurement windows, with two alternating target-order repetitions.
Room and sidebar reads run with paced write-rate limits of 0, 10 and 100 POSTs/s
into the separate HQ room. Every read and POST is checked by the shared route/body
contracts. Each returned message ID, creator, room and body token is independently
checked through a read-only SQLite connection. JSONL write receipts and consistent
SQLite backups are retained for later rechecking. Generator utilization at or
above 85% rejects a sample.

Rate limits are caps, not guaranteed arrivals. Achieved write throughput and write
p99 are reported alongside reads. Unequal achieved write rates describe overload
behavior and cannot establish a matched-write-load read speedup. There are no
Cable subscribers in this workload, and two users do not model large production
chat capacity.

Cold response-cache checks use 32 distinct query variants per route/repetition,
64 per target in total, with one validated request on a fresh HTTP connection
for each key. View fragments are already warm; this measures a cold completed
response cache, not a wholly cold server or database.

```sh
SERVER_CPUS=0-1 LOADGEN_CPUS=2-3 RUBY=ruby3.3 bench/cached-workloads \
  --target preparation=/path/to/preparation \
  --target early=/path/to/early \
  --reps 2 --secs 20 --warmup 5 --concurrency 16 --cold 32 \
  --out tmp/bench/pr-cached-workloads
bench/cached-workloads-report tmp/bench/pr-cached-workloads
```

Raw receipts, contracts, audit backups and results belong in ignored
`tmp/bench/pr-cached-workloads/`; result JSON and credentials are not committed.
A failed setup smoke run and the short successful harness smoke run are excluded
from the measurement results.

## Earlier warm-read evidence

The earlier three-repeat, single-user warm-read comparison at `b314160` measured
44.6–84.7% higher throughput than the pinned C# ReadyToRun build across all eight
route/concurrency cells, with lower median p99 in every cell and 62,488,810
validated responses. Those measurements precede the typed-cookie cleanup and
are historical evidence for the architecture change, not a new C# measurement
for this PR revision. Across those eight cells, the geometric mean gain over C#
was 62.5%, with 34.0% less server CPU/request; compared with the preceding Rust
revision, throughput was 2.46× and CPU/request fell 59.4%. These are controlled
warm-read results on this cloud machine, not language-wide or write-heavy claims.
Both servers used two CPUs, the generator two separate CPUs, the same seed,
gzip, shared route/body validation, 5-second warmups and 20-second windows. The
published desktop hardware/private harness could not be reproduced exactly.

| Route | Connections | Rust req/s | C# req/s | Rust / C# | CPU µs Rust / C# | p99 ms Rust / C# |
|---|---:|---:|---:|---:|---:|---:|
| room_show | 16 | 54,978 | 29,772 | 1.85× | 35.2 / 58.3 | 0.696 / 2.549 |
| room_show | 64 | 58,906 | 40,749 | 1.45× | 33.0 / 46.4 | 2.615 / 4.175 |
| messages_page | 16 | 65,985 | 36,333 | 1.82× | 29.4 / 48.7 | 0.582 / 1.399 |
| messages_page | 64 | 64,186 | 40,957 | 1.57× | 30.4 / 46.0 | 2.285 / 4.047 |
| sidebar | 16 | 66,998 | 38,420 | 1.74× | 29.0 / 44.7 | 0.597 / 1.326 |
| sidebar | 64 | 74,278 | 50,790 | 1.46× | 26.5 / 37.2 | 2.123 / 3.197 |
| search | 16 | 62,564 | 37,593 | 1.66× | 31.1 / 46.7 | 0.587 / 1.384 |
| search | 64 | 68,041 | 45,030 | 1.51× | 28.7 / 41.9 | 2.261 / 3.451 |

See the [complete pinned study and profiling limits](https://github.com/mingley/once-campfire-rust/blob/0c4bc9137c655e4028ca10445105b3350f1ba6db/plans/cached-response-early-hit.md).

## Results

The complete 24-condition run passed all validators and independent durable-write
audits. The table reports medians of two repetitions; it is a limited workload
sample, not a statistical confidence interval.

2 alternating repetitions, 16 read connections, david,jason; all requests validated and writes audited.

| Route | Write-rate limit/s | Read req/s preparation → early | Ratio | Achieved write/s preparation / early | Writer p99 ms preparation / early | Achieved write rates |
|---|---:|---:|---:|---:|---:|---|
| room_show | 0 | 21,543 → 53,701 | 2.49× | 0.00 / 0.00 | — | no writes |
| room_show | 10 | 18,568 → 47,953 | 2.58× | 9.89 / 9.89 | 16.02 / 24.82 | within 5% each repetition |
| room_show | 100 | 3,158 → 4,032 | 1.28× | 79.76 / 60.48 | 25.17 / 36.59 | unequal |
| sidebar | 0 | 24,973 → 59,071 | 2.37× | 0.00 / 0.00 | — | no writes |
| sidebar | 10 | 25,925 → 60,888 | 2.35× | 9.90 / 9.89 | 8.52 / 10.28 | within 5% each repetition |
| sidebar | 100 | 17,930 → 43,045 | 2.40× | 90.61 / 90.71 | 8.48 / 8.03 | within 5% each repetition |

Rates are caps, not guaranteed arrivals. Rows with unequal achieved write rates describe overload behavior and do not establish matched-write-load read speedups.


Cold response-cache variants, fragments already warm, one fresh HTTP connection/request.

| Route | Requests/target | Median latency ms preparation → early |
|---|---:|---:|
| room_show | 64 | 0.742 → 0.582 |
| messages_page | 64 | 0.716 → 0.585 |
| sidebar | 64 | 0.708 → 0.542 |
| search | 64 | 0.847 → 0.637 |

Validated timed reads: 15,238,266; audited durable writes: 14,453; peak generator utilization: 0.637.

At comparable achieved write rates, read throughput was 2.35–2.58× the preparation
build. Write latency did not improve consistently: at the 10/s room workload,
writer p99 increased from 16.02 to 24.82 ms, while sidebar writer p99 increased
from 8.52 to 10.28 ms. These are material tradeoffs alongside the read gains.

At the 100/s room limit, both builds fell short of the requested rate and the
early build completed about 24% fewer writes (60.48 versus 79.76/s), with higher
writer p99. Its read ratio is therefore **not a matched-load speedup**. This proposal
does not claim improvement in every case; scheduling/SQLite contention under
room-write overload needs further investigation before recommending deployment.
The sidebar workload at that limit maintained comparable achieved write rates.

Cold response-cache checks showed lower median latency on these warm-fragment
requests, but their 64 samples per route/target do not establish behavior for
cold fragments, startup, large user populations or tail latency.

The upstream proposal carries the same Rust production and test sources as the
measured evidence branch; unrelated C#/allocator orchestration remains in the
linked study rather than the proposed runtime patch.
