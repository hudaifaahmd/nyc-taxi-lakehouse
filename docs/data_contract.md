# Data Contract

This contract describes the canonical stage notebooks, monthly controller and aggregate publisher. It defines source semantics and persisted interfaces; it is not a live schema inventory.

## Source And Grain

The source is one complete monthly yellow-taxi Parquet file. `_process_year` and `_process_month` identify its source period; they are not inferred from pickup timestamps. Bronze preserves source rows with ingestion metadata. Silver and Gold preserve accepted row multiplicity. There is no established unique trip key or deduplication guarantee.

Required Bronze input columns: `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID`, `payment_type`, `fare_amount`, and `total_amount`. Notebook 02 permits additive columns with `mergeSchema=true`; numeric drift is aligned to existing Bronze types only after a losslessness check. Nonnumeric incompatible changes and lossy numeric changes fail. This is a bounded compatibility rule, not unrestricted schema evolution.

Metadata retained through Silver includes `_batch_id`, `_source_name`, `_ingested_at_utc`, `_process_year`, and `_process_month`. Gold retains `_batch_id` and the process period, plus original location IDs. The runtime creates a UUID `run_id` per processed period and derives one stable `_batch_id` shared by 01-06 through `_pipeline_context`; standalone notebook executions can have separate UUID batch IDs. Do not equate either identifier with a content hash.

## Quality Policy

Current canonical policy: `quality_v2_pickup_grace_numeric_contract`, default `boundary_grace_days=1`. Pickup dates must fall in `[source_month_start - grace days, next_month_start + grace days)`. January 2024 with default grace permits December 31, 2023 through February 1, 2024 inclusive. The earlier `pickup_month_grace_v1` document describes the preceding date-only change, not the full current policy.

Notebook 03 rejects missing vendor, pickup/dropoff timestamp, or pickup/dropoff location; negative distance, fare, or total amount; dropoff before pickup; and pickup beyond the allowed date window. `_dq_error` contains semicolon-space-separated reasons; one rejected row can have several reasons. `_rejected_at_utc` records quarantine processing time.

Notebook 03 additionally quarantines null, uncastable, NaN, or infinite `trip_distance`, `fare_amount`, and `total_amount` with `missing_or_nonfinite_<column>` reasons. Its write uses `mergeSchema=true` to preserve additive source fields, including 2025 fields already admitted by Bronze. This does not claim universal validation of every numeric field. Raw Bronze/quarantine invalid amounts are retained as evidence; 06 reconciles invalid-count splits and requires zero invalid amounts only in accepted Silver/Gold. Negative values are quarantined under project policy, not an approved refunds/adjustments accounting policy.

Boundary pickups inside the grace window but outside the source month remain accepted and visible as WARN. Beyond-grace accepted rows fail reconciliation. Notebook 01 sets `spark.sql.session.timeZone=UTC` for stable session projection. This does not establish the input timestamp's real-world timezone or prove NYC-to-UTC conversion. The scheduler's `Asia/Kolkata` calendar is separate from taxi event-time semantics.

## Persisted Objects

All table names below are qualified by `workspace.nyc_taxi_lakehouse` by default.

| Object | Grain / keys / write contract |
| --- | --- |
| `landing` Volume | Run-specific original trip files; zone reference CSV stored with checksum in notebook 05. |
| `bronze_taxi_trips` | Source row plus metadata; selected month replaced after additive/lossless schema alignment; original bytes and SHA-256 retained separately. |
| `quarantine_taxi_trips` | Rejected source row plus reasons; selected month replaced even when empty; additive fields merged with `mergeSchema=true`. |
| `silver_taxi_trips` | Accepted standardized row; selected month replaced; derived pickup date and duration. |
| `gold_fact_taxi_trips` | Accepted row with `date_key`, `pickup_zone_key`, `dropoff_zone_key`; selected month replaced, including valid fully quarantined empty output. |
| `gold_dim_date` | Unique non-null `date_key` as integer `yyyyMMdd`, with actual `pickup_date`; shared MERGE. |
| `gold_dim_zone` | Unique non-null `zone_id`; borough, zone name, service zone, source URL; shared MERGE with unknown `-1`. |
| `pipeline_audit` | Append-only stage execution summaries; not a monthly completion registry. |
| `pipeline_reconciliation_checks` | Append-only per-check execution results, overall status, and recorded UTC check time. |
| `pipeline_month_events` | Append-only controller events, created by `SparkEventStore` even in 08 plan mode. |

Notebook 01 defines `recon_table` as `pipeline_reconciliation`; notebook 06 actually writes `pipeline_reconciliation_checks`. Do not assume the unsuffixed table is populated.

Fact joins: `date_key = gold_dim_date.date_key`; `pickup_zone_key = gold_dim_zone.zone_id`; `dropoff_zone_key = gold_dim_zone.zone_id`. Validate dimensions before joining: duplicates multiply both trips and amounts. Original location IDs are retained to verify mapped keys.

## Metrics And Reconciliation

`trip_count` counts accepted source rows, not distinct vehicles, people, or deduplicated rides. Quarantine percentage is `100 * quarantine_count / bronze_count`, null if the denominator is zero. Reason occurrences are not distinct rejected rows.

`source_total_amount` and `source_fare_amount` sum source values after per-row conversion to `DECIMAL(20,2)`. Reconciliation compares rounded cents, null counts, and invalid counts using Bronze = Silver + quarantine and Silver = Gold. Only accepted Silver/Gold must have zero invalid amounts; raw/rejected invalid evidence is allowed and counted rather than erased. These measures are not validated net revenue, profit, or a recomputed fare-component accounting model.

Required persisted invariants: Bronze = Silver + quarantine; Silver = Gold; rounded amount/null accounting reconciles across splits and accepted layers; per-ingestion-batch metrics agree; dimensions have unique non-null keys; all Gold references exist and map correctly; date keys match pickup dates/timestamps; accepted beyond-grace counts are zero. Unknown zones and retained boundary trips can produce WARN. Overall status is FAIL before WARN before PASS.

Zero accepted rows are permitted only when Bronze is positive and quarantine equals Bronze. Notebook 05 can clear the month's Gold slice in this case while preserving shared dimensions; 06 uses `silver_rows_or_fully_quarantined` to enforce the same count contract. All other reconciliation checks still apply. Notebook 07's nonempty-report guard can reject an empty accepted period even when pipeline accounting is valid; use coverage 10 and checks to explain that period.

## Monthly Event Interface

`src/taxi_pipeline/execution.py` defines `EVENT_SCHEMA`: counts are Delta `LONG`; all other fields are `STRING`. `SparkEventStore.emit` generates UUID event IDs and UTC ISO timestamps with microsecond precision. Nullable measurements mean unavailable/not measured, never implicit zero.

| Field | Meaning |
| --- | --- |
| `event_id` | UUID identifier; tie-breaker after descending `event_time_utc` for latest-event selection. |
| `run_id` | UUID correlation identifier for one candidate source-period attempt. |
| `source_period` | Canonical `YYYY-MM` source period. |
| `status` | `STARTED`, `STAGE_STARTED`, `REVISION_PENDING`, `COMPLETED`, `COMPLETED_WARN`, `FAILED`, `WAITING_FOR_SOURCE`, or `RETRY_WAITING_FOR_SOURCE`. Progress and revision discovery are nonterminal. |
| `policy_version` | `pipeline_v2:<first 16 COMPLETION_POLICY_DIGEST hex characters>:grace=<days>`. Normally source-derived; explicitly reviewed budget/widget/documentation-only payloads retain compatibility through exact hash checks. Future executable changes are not covered by that exception. |
| `stage` | `availability`, `pipeline`, canonical stage basename (`01_setup_and_config` through `06_reconciliation_and_audit`), or terminal `reconciliation`. |
| `bronze_count`, `silver_count`, `gold_count`, `quarantine_count` | Nullable integer persisted row counts scoped to this source period. |
| `out_of_period_count` | Nullable integer accepted trip rows outside the nominal source month, including retained boundary rows. Not a count of distinct dates or beyond-grace violations. |
| `source_url` | Monthly trip-file URL. |
| `source_sha256` | Nullable SHA-256 of downloaded file bytes; cannot be inferred from HEAD metadata. |
| `source_etag`, `source_last_modified` | Nullable source HTTP metadata; neither is guaranteed to be a content hash. |
| `event_time_utc` | UTC ISO string with microseconds; ordered descending, then event ID descending. |
| `error_message` | Nullable bounded diagnostic text; exclude credentials and sensitive configuration. |

`COMPLETED` requires all mandatory stages and PASS reconciliation. `COMPLETED_WARN` requires WARN with no critical failure. `SparkEventStore.completed` selects the latest event per period before requiring matching policy and a completion status; STARTED, STAGE_STARTED, REVISION_PENDING, FAILED, WAITING, and stale-policy events do not qualify. `SparkEventStore.priority_pending` selects latest current-policy REVISION_PENDING/FAILED/STARTED/STAGE_STARTED periods, passed by 08 into the controller priority queue. Notebook 10 uses the same latest-event ordering and displays `NOT_PROCESSED_BY_CURRENT_VERSION` for missing/current-policy-ineligible coverage.

Run mode probes completed periods among the last three selected months before ETL, independently of its month-processing budget. Changed non-null ETag/Last-Modified produces a durable `REVISION_PENDING` event with the new headers before processing. Work order is newest selected eligible month, newly detected revisions/persisted retry priorities, then oldest pending history. The latest pending event keeps discovered revision work eligible after it ages out of the probe window. Missing/unchanged metadata cannot rule out revisions; older undiscovered revisions need explicit single-month force replay to download and rehash. Historical completion and HEAD metadata alone do not prove current data freshness.
