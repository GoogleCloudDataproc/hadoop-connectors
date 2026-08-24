# 📋 Google Cloud Hadoop Connectors - Code Review & Style Guidelines

## 🎯 Purpose & Scope
This guide defines coding standards, architecture principles, performance rules, concurrency guidelines, testing practices, and PR hygiene for the `hadoop-connectors` repository. Reviewers and AI coding agents MUST validate all changes against these guidelines, prioritizing **stability, thread safety, high performance, and ecosystem compatibility**.

---

## 🌍 1. Open Source & Ecosystem Compatibility
*The `hadoop-connectors` library forms the foundation for Big Data workloads on Google Cloud (Apache Spark, Hadoop, Hive, Flink, Presto/Trino, Dataproc). Incompatibilities or performance regressions can cause cluster-wide outages.*

* **License Compliance:** Every source file **MUST** start with the standard Apache License 2.0 header.
* **Dependency Hygiene & Shading:** Do not introduce unvetted third-party dependencies. Dependencies (e.g. Guava, gRPC, Protobuf, Jackson, Google Cloud SDKs) must be properly shaded/relocated in `pom.xml` to prevent classpath conflicts in user applications.
* **Binary & Semantic Backward Compatibility:** Public APIs (`GoogleHadoopFileSystem`, `GoogleCloudStorage`, public configuration keys) must maintain backward compatibility. Do not modify public method signatures without formal deprecation cycles.
* **Security & Credential Governance:** **NEVER** log, expose, or commit credentials, private keys, service account tokens, or authorization headers. Sanitize exception messages to prevent token leaks.

---

## ⚡ 2. Big Data Performance & Resource Optimization
*Code runs in high-throughput, multi-threaded worker environments. GC pressure and I/O bottlenecks directly impact job execution costs.*

* **Zero-Copy & Direct Buffering:** Utilize zero-copy transfers, direct byte buffers, and vectored I/O (`readVectored`) where applicable for maximum I/O throughput.
* **Inner Loop Allocation Control:** Do **NOT** allocate large objects, temporary byte arrays, Java Streams, or lambdas inside tight data-plane read/write inner loops (e.g., `read()`, `write()`, payload copy loops). Use primitive loops to minimize garbage collection (GC) pauses.
* **Efficient Parameterized Logging:** Avoid string concatenation in log statements (e.g., `logger.atFine().log("Read " + bytes)`). Always use parameterized logging (e.g. `logger.atFine().log("Read %d bytes", bytes)`. Avoid logging inside high-frequency data path methods unless guarded by explicit debug flags.
* **Low-Overhead Metrics:** Metric tracking (e.g., `GhfsThreadLocalStats`, latency counters, event bus notifications) **MUST** have near-zero execution overhead. Avoid lock contention when updating metrics (use `Atomic` counters or thread-local storage). Ensure metrics are not double-counted (e.g., during stream wraps or repeated `close()` calls).

---

## 🧵 3. Concurrency, Thread Safety & Resource Management
*Filesystem operations are invoked concurrently by multiple parallel executor tasks and threads.*

* **Avoid Redundant Synchronization:** Do **NOT** add redundant `synchronized` modifiers to wrapper classes or methods where the underlying stream (such as Hadoop's `FSDataOutputStream`) is single-threaded by specification, or where higher-layer synchronization already guarantees safety.
* **Atomic State Transitions & TOCTOU:** Avoid Time-Of-Check to Time-Of-Use (TOCTOU) race conditions (e.g., checking `if (!closed && channel.isOpen())` before inserting into a pool/queue). Use `ReentrantLock` or `AtomicBoolean`/`AtomicInteger` primitives for critical state transitions.
* **Strict Resource Cleanup & Idempotent Closure:** All streams, channels, and client objects implementing `AutoCloseable` **MUST** be managed via `try-with-resources`. Implementations of `close()` **MUST** be idempotent, thread-safe, and handle repeated invocations gracefully without throwing redundant exceptions or leaking channels.
* **Thread & Channel Cleanup:** Gracefully shut down background thread pools (`ExecutorService`), gRPC channels, and background monitoring tasks to prevent zombie thread leaks.

---

## 🛠️ 4. Google Java Best Practices & Exception Handling
*Code adheres strictly to Google Java Style and Guava best practices.*

* **Guava Preconditions:** Validate input parameters at method entry points using `Preconditions.checkNotNull`, `Preconditions.checkArgument`, and `Preconditions.checkState`. Do not write custom null-checking or state assertion logic.
* **Null Safety:** Annotate public method parameters and return types with `@Nullable` or `@NonNull` where appropriate.
* **Exception Translation & Clean Logging:**
  * Lower-level GCS/gRPC exceptions (`StorageException`, `StatusRuntimeException`) must be translated into standard Hadoop `IOException` subclasses (`FileNotFoundException`, `FileAlreadyExistsException`, `AccessDeniedException`).
  * **No Duplicate Stack Traces:** Do NOT log and rethrow exceptions in `close()` or low-level helpers. Let top-level exception handlers or callers log failures to avoid polluting server log files.
* **Import Hygiene:** Do NOT use wildcard imports (`import java.util.*`). Resolve class name collisions (e.g., `FileAlreadyExistsException`) using explicit full imports or inline package qualifiers.

---

## ⚙️ 5. Configuration & Naming Standards
*Configuration flags control connector behavior across thousands of cluster nodes.*

* **Key Naming Conventions:** Property names must strictly follow standard prefix conventions (e.g., `fs.gs.<connector>.<property>`).
* **Defaults & Documentation:** Every new configuration key in `GoogleHadoopFileSystemConfiguration` must define a clear constant, default value, getter method, and a corresponding entry in `docs/configuration.md`.
* **Feature Flag Isolation:** Temporary integration or experimental feature flags must be cleanly isolated and scheduled for removal after validation.

---

## 🧪 6. Testing Rigor & Quality
*Tests must validate real-world edge cases without introducing test flakiness.*

* **Google Truth Assertions:** Use Google Truth (`assertThat(...)`) for all assertions instead of JUnit `assertEquals`.
* **Precise Exception Assertions:** Use `assertThrows(ExpectedException.class, () -> ...)` rather than catching generic `IOException` or `Exception` to avoid masking unexpected runtime errors.
* **Fakes Over Mocks:** Use `FakeGoogleCloudStorage` or `test-lib` fake implementations rather than fragile mock objects.
* **Test Resource Isolation:** Integration tests must generate unique GCS object URIs (e.g., appending UUIDs or tags) to prevent resource collisions during parallel test executions.

---

## 📝 7. PR & Commit Hygiene
*Commit history is audited across open-source communities.*

* **Conventional Commits:** PR titles and commit messages MUST follow Conventional Commits (e.g., `feat: ...`, `fix: ...`, `chore: ...`, `docs: ...`).
* **Atomic Changes:** Keep PRs focused. Separate refactoring, style fixes, and dependency upgrades from core feature development.

---

## 🤖 Instructions for AI Coding Agents (Gemini Code Assist / Antigravity)
When writing, refactoring, or reviewing code for this repository, coding agents **MUST**:
1. Verify Apache 2.0 license headers are present on all new files.
2. Check for potential memory leaks or unclosed resources.
3. Ensure no proprietary libraries or unapproved dependencies are used.
4. Scrutinize I/O paths for performance bottlenecks (e.g., excessive seeking or blocking).
5. Enforce Google Java Style and Guava usage.
6. Ensure strict resource cleanup (`try-with-resources`) and idempotent `close()` behavior.
7. Check inner loops for unnecessary object allocations, lambdas, or string concatenations.
8. Enforce Guava `Preconditions` for argument checks and Google Truth for test assertions.
9. Translate low-level storage exceptions to standard Hadoop `IOException` types without duplicate logging.
