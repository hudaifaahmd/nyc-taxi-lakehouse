# Architecture

## Execution Boundaries

The system separates monthly processing, analytical reporting and public aggregate delivery.

1. Notebook 08 loads generated notebook 09 and invokes the Python controller.
2. The controller plans eligible source periods, checks availability/revisions and executes stages 01-06 sequentially within its month budget.
3. Each processed month gets an isolated Python namespace, a shared run context and one ingestion batch identifier.
4. Notebook 06 reads persisted layers and appends reconciliation results. Only PASS or WARN can produce a successful completion event.
5. The dependent public publisher loads validated monthly observations through notebooks 13 and 12, then stages and promotes the public summary.

The runner does not use cross-task Python variables, dynamic workspace notebook APIs or runtime package installation. Generated sources bundle the canonical stage code and local-testable planning/controller modules.

## Storage Responsibilities

| Layer | Purpose | Write boundary |
| --- | --- | --- |
| Landing Volume | Original monthly Parquet and zone-reference bytes | Run-specific path |
| Bronze | Source fields plus ingestion provenance | Replace selected source month |
| Quarantine | Rejected source records with all applicable reasons | Replace selected source month, including an empty result |
| Silver | Standardized accepted records | Replace selected source month |
| Gold fact | Accepted records with date and mapped zone keys | Replace selected source month |
| Date and zone dimensions | Shared analytical members | Key-based merge, retaining earlier keys |
| Audit/check/event tables | Stage observations, checks and controller state | Append-only history |

Monthly replacements use source-period metadata, not pickup-date partitions. Accepted boundary pickups therefore remain associated with their original source file.

## Quality And Reconciliation

Quality classification preserves Bronze. Quarantine retains rejected records rather than dropping them silently. Silver/Gold retain accepted multiplicity; there is no invented unique ride key.

Reconciliation covers row counts, rounded amount/null/invalid accounting, per-batch provenance, dimension uniqueness, valid references, mapping correctness and pickup-date policy. FAIL blocks successful completion. WARN retains observability for accepted boundary pickups and unknown-zone mapping.

Dimensions use a reserved unknown member `-1` when a source location does not match the lookup. Original location IDs remain in the fact for diagnosis.

## Orchestration

Eligibility follows a two-calendar-month lag using Asia/Kolkata. A 404 means the source is not available for that attempt, not a valid empty input. Other exhausted transport failures stop the run.

The processing budget limits completed ETL months, not concurrency or every metadata request. Recent completed months are probed independently of that budget. Discovered revisions are recorded durably so they remain pending after leaving the three-month probe window.

Completed history is selected from the latest event per source period and must match the current completion-policy identity. Old successes cannot hide a newer failed or incomplete attempt.

## Public Delivery

The public contract contains monthly counts, rounded accepted total-amount cents and retained boundary counts. It excludes trip rows, batch IDs, run IDs, detailed errors and credentials.

The publisher validates exact fields, contiguous coverage, check/event agreement and repository identity/visibility. It reads back staged and promoted content. An ambiguous report write is inspected before success or uncertainty is reported.

The portfolio reads public aggregates when opened or refreshed. Its refresh time, the summary's evidence time and the Job's most recent scheduled check are separate concepts.

## Consistency Limits

Layer writes and shared-dimension merges commit separately. A failed stage can leave mixed layer versions until repaired. The workflow's completion gate is not a database-wide transaction or a concurrent-reader snapshot guarantee.

The current design requires a single ETL writer and no concurrent writes during reconciliation/summary preparation. Job concurrency settings do not prevent an independently launched manual notebook from writing the same tables.
