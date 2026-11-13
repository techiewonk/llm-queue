# Architecture

The architecture is documented on the [docs site](https://techiewonk.github.io/llm-queue/), one
page per topic:

- [Architecture overview](explanation/architecture.md): purpose, deployment topology, components
- [The life of a call](explanation/call-lifecycle.md): caller, queue, worker, reply; direct path
- [Admission](explanation/admission.md): the one atomic store script that admits a call
- [Attempts and retries](explanation/attempts.md): timers, outcomes, refunds, retry budget
- [Failures and recovery](explanation/resilience.md): circuit breaker, store outage, failure model
- [Streaming](explanation/streaming.md): how events reach the caller
- [Storage](explanation/storage.md): key layout and store usage
- [Background tasks](explanation/background.md): the loops each process runs
- [Performance](explanation/performance.md): measured numbers and where they come from
- [Extension points](reference/extension-points.md): hooks an app registers
