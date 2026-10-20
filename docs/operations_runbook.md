# Operations

## Deployment Model

Keep notebook base names and relative dependencies stable. The monthly runner uses notebook 09; summary preparation uses 09 and 12; public publication uses 13. Generated notebooks must be rebuilt from canonical inputs.

A Databricks Git-folder commit does not update a Job that points to a different workspace folder. Inspect the Job's actual notebook paths before replacing files. The checked-in Job template is paused and is not an inventory of the live workspace's configuration.

## Parameters

| Parameter | Default | Meaning |
| --- | --- | --- |
| `mode` | `plan` | Plan without source probes/ETL, or `run` |
| `start_period` | `2024-01` | First source month |
| `end_period` | Empty | Latest policy-eligible source month |
| `max_months_per_run` | `1` | Integer 1-30, sequential processed-month budget |
| `boundary_grace_days` | `1` | Nonnegative date grace; part of completion identity |
| `force_reprocess` | `false` | Replay only an explicitly selected single-month range |
| `catalog_name` | `workspace` | Validated simple SQL identifier |
| `schema_name` | `nyc_taxi_lakehouse` | Validated simple SQL identifier |

Plan mode can create the operational schema/event table. It performs no source probes, downloads or business-table writes.

## Routine Checks

1. Inspect the latest Job run and each task's result.
2. Use notebook 10 for current-policy source-month coverage and recent events.
3. Review FAIL checks and warning detail in `pipeline_reconciliation_checks`.
4. Compare source coverage and totals rather than treating a daily availability check as new data.
5. For public delivery, accept `SUMMARY_PUBLISHED` or `UNCHANGED`. Investigate other statuses; do not expose raw HTTP/secret errors.

“Unchanged source” means the checked monthly files did not present different usable revision metadata, so ETL was skipped. It is not proof that every source byte was reread. “Unchanged summary” means the validated public content is equivalent apart from its preparation timestamp, so no report commit is needed.

## Updating Code Safely

1. Wait until the Job and all manual writers are idle. Temporarily pause the schedule if needed; record its original state.
2. Export a backup of the actual Job-source notebooks and record existing task paths/parameters.
3. Replace related sources as one set; do not mix old stages with a new generated runtime.
4. Preserve notebook names, catalog/schema, grace days, source range, run budget and `force_reprocess=false`.
5. Inspect the runtime and completion-policy values in the supplied release manifest.
6. Use plan mode and notebook 10 to confirm the existing completions remain recognized. This check does not require historical ETL.
7. Restore the schedule's original state after verification.

A reviewed documentation-only release can retain the existing completion identity through an exact source-bundle hash mapping. Any unrelated source edit is outside that mapping. Do not pin future executable changes to an old identity merely to suppress reprocessing.

## Failure Handling

| Observation | Action |
| --- | --- |
| `WAITING_FOR_SOURCE` | Leave the period eligible for a later check; do not create an empty business slice |
| `REVISION_PENDING` | A revision was detected and remains queued; it is not completed work |
| `FAILED` at ingestion | Inspect transport, file validation, landing bytes and stored Bronze |
| `FAILED` at transformation | Inspect quality reasons, schema compatibility and row accounting |
| `FAILED` at Gold | Inspect reference lookup, key uniqueness, joins and amount checks |
| `FAILED` at reconciliation | Review persisted individual FAILs before repairing upstream stages |
| Latest `STARTED`/`STAGE_STARTED` | Investigate interruption and persisted table state; do not infer completion from row presence |
| `COMPLETED_WARN` | Required checks passed; review retained boundary/unknown-zone observations |
| Publisher `PUBLISH_STATE_UNKNOWN` | Inspect the fixed report destination before retry; do not assume the old report was retained |
| Publication credential failure | Repair the managed secret/permissions without copying credentials into notebooks |

Do not delete failed audit/events or insert a manual success event to make coverage appear complete.

## Recovery Boundaries

Writes are separate table transactions. A failed run does not restore all layers automatically. Confirm the affected source month, existing table versions and latest attempt before any replay. A controlled replay must compare neighboring months as well as the selected month.

Use the controller for operational completions. Standalone notebooks do not record controller completion events; rerunning them can invalidate older observations until reconciliation is repeated.

A forced replay, policy migration, table restore or schema change is an operational decision, not part of a documentation-only update. Review and test such changes separately.

## Security And Publication

Store publication credentials only in managed secret storage. The publisher's destination and file paths are allowlisted; redirects are disabled. Keep raw trip rows, request objects, tokens, detailed errors and private workspace screenshots out of source commits.

Daily Job success is operational health. Reconciliation timestamps describe validated observations. The public summary is a validated snapshot, not a real-time heartbeat. Unchanged checks do not advance its evidence timestamp.

## Validation Scope

Offline tests exercise planning, controller callbacks, source bundling and publication validation with test adapters. Spark execution, permissions, compute compatibility and recovery behavior must be verified in the target workspace. Never run a forced historical replay just to validate comment changes.
