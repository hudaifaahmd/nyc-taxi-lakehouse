# NYC Taxi Lakehouse

An independent, end-to-end Databricks data-engineering reference project using
Python, PySpark, SQL and Delta Lake. Monthly NYC TLC yellow-taxi source files
become traceable Bronze records, accepted Silver records, quarantined evidence,
a dimensional Gold model and reconciled reporting results.

This is a personal learning project, separate from employer production work.
It is not an official City report, a real-time trip stream or an SLA-backed service.

## Architecture

TLC monthly Parquet -> Landing Volume -> Bronze -> quality checks ->
Silver + quarantine -> Gold fact/dimensions -> reconciliation -> reporting.

The monthly runner orchestrates bounded sequential processing. A dependent task
exports validated aggregate evidence to a separate summary repository. Browser
visitors receive only that public JSON, never a Databricks or GitHub credential.

## Verified Baseline

January 2024 through August 2026, 32 source periods. Global persisted-table SQL
results were supplied and reviewed October 18, 2026; monthly reconciliation
observations were exported October 17.

| Metric | Result |
| --- | ---: |
| Bronze source rows | 119,595,677 |
| Silver accepted rows | 115,842,411 |
| Gold fact rows | 115,842,411 |
| Quarantined rows | 3,753,266 |
| Weighted quarantine percentage | 3.13829570946783% |
| Gold source total amount (USD) | 3,388,158,595.79 |
| Gold null total amounts | 0 |

Bronze = Silver + quarantine; Silver = Gold. These counts are source-period
records, not proven globally unique trips. Source `total_amount` is not
validated net revenue. Negative amounts are among the policy exclusions.

The baseline check export contains 1,536 PASS, 32 boundary WARN and 0 FAIL.
All 32 monthly completion statuses were `COMPLETED_WARN`, not warning-free
success. The 661 accepted boundary observations are not unique trip identities.

## Notebook Map

| Notebook | Purpose |
| --- | --- |
| [00](00_project_overview.ipynb) | Project overview |
| [01](01_setup_and_config.ipynb) | Configuration, namespace, widgets and helpers |
| [02](02_ingest_to_bronze.ipynb) | Landing and Bronze ingestion |
| [03](03_quality_checks.ipynb) | Quality policy and quarantine |
| [04](04_transform_to_silver.ipynb) | Accepted-record transformation |
| [05](05_build_gold_model.ipynb) | Gold fact and shared dimensions |
| [06](06_reconciliation_and_audit.ipynb) | Row, amount and metadata checks |
| [07](07_reporting_queries.ipynb) | Analytics and operational SQL |
| [08](08_monthly_pipeline_runner.ipynb) | Bounded source-month orchestration |
| [09](09_runtime_sources.ipynb) | Bundled runtime/stage sources used by the runner |
| [10](10_backfill_status.ipynb) | Read-only coverage and completion inspection |
| [11](11_publication_readiness.ipynb) | Optional historical connectivity prerequisite |
| [12](12_public_summary_sources.ipynb) | Summary validation/export definitions |
| [13](13_prepare_public_summary.ipynb) | Read-only aggregate preparation |
| [16](16_publish_public_summary.ipynb) | Validated public-summary promotion |

Notebooks 14 and 15 were private setup utilities and are intentionally omitted.
Notebook outputs are excluded from this source repository.

## Source Periods And Scheduling

The source period identifies the input file month, not necessarily every trip's
pickup month. The configured pickup-date grace window is one day; boundary
observations remain explicit evidence.

A source-availability check runs daily at 09:00 Asia/Calcutta (UTC+05:30).
ETL processes eligible new or changed monthly files; unchanged inputs skip ETL.
The runner calculates latest eligibility as the current India-time calendar month
minus two months. This is an eligibility policy, not a source-availability promise.
Compute startup still has a cost even when no monthly ETL is performed.

The Job allows one concurrent run with queueing. Publisher task
`publish_public_summary` depends on `08_monthly_pipeline_runner` with
Run if: All succeeded. No second daily trigger is required.

Evidence received October 19, 2026 shows the two-task Job manually launched and
successful: runner 53 seconds, publisher 14 seconds. Publisher returned
`UNCHANGED`, correctly skipping another report commit. This demonstrates the
dependent-task unchanged path, not a fresh changed-data publication or a new
scheduler-launched two-task run. A scheduler-launched runner was previously
observed successful October 18.

## Run In Your Own Databricks Workspace

1. Clone this repository as a Databricks Git folder. Keep the notebooks together
   so relative `%run` references resolve.
2. Use a supported Spark environment and a namespace/Volume where you have the
   required permissions. Defaults are catalog `workspace`, schema
   `nyc_taxi_lakehouse` and first source period `2024-01`.
3. Inspect notebook 08 parameters before execution. Default `mode=plan`
   initializes operational metadata and displays eligible periods; it is not
   a completely write-free operation. Use `mode=run` only when ready for ETL.
4. Keep `max_months_per_run=1` for bounded work. Blank `end_period` follows
   latest eligibility. Do not set `force_reprocess=true` for a routine check.
5. Inspect reconciliation and notebook 10 before consuming results. Failed,
   waiting and warning states must not be presented as all-green data.

Stage notebooks can create or replace data. Do not run every notebook blindly
against a shared namespace. Avoid overlapping manual writers.

Publication is optional and separate from ETL. Notebook 16 is deliberately
pinned to this owner's summary repository; fork owners must review and change
the destination and reader configuration before using their own publisher.
Provision publisher credentials through the supported secret mechanism, never
source cells or widgets. Do not reuse someone else's token or secret scope.

## Public Report

[Summary repository](https://github.com/hudaifaahmd/nyc-taxi-public-summary) |
[Aggregate JSON](https://raw.githubusercontent.com/hudaifaahmd/nyc-taxi-public-summary/main/reports/snapshot.json)

The report contains allowlisted monthly counts, source amounts as integer cents
strings, and evidence/export timestamps. Preparation checks matching completed
reconciliation evidence; it does not rescan all business tables.

`evidenceThroughUtc` is reconciliation observation time. `generatedAtUtc` is
export time. Neither is a daily Job heartbeat. Unchanged evidence skips commits;
an older export date alone does not prove the Job is broken. Conservative
`verified_snapshot`/`publicFeedConnected` labels are not a live connectivity API.

Validation blocks stale/reduced coverage and inconsistent evidence. Publishing
stages, reads back and promotes the summary using file SHA checks. A possible
applied write without confirmation is reported as uncertain, not a guaranteed
rollback. No raw rows or private operational logs belong in the report.

## Source Control And Deployment

Edit notebooks inside the Git folder, review the diff, then Commit & Push.
Edits to independent workspace copies outside the Git folder do not synchronize.
The existing running Job still references the original workspace notebooks;
committing Git-folder copies does not deploy those edits to that Job. Changing
Job source paths is a separate controlled step.

Notebooks 09 and 12 embed generated sources. Keep each generated bundle and its
underlying logic consistent; changing a standalone stage alone does not update
the runner's embedded implementation. This repository currently contains the
workspace notebook export, not the full local generator/test toolchain.

## Limits

Controlled forced replacement and interrupted-run recovery remain live
verification gaps. Source timezone/DST interpretation, signed amounts, global
trip identity and cross-table publication isolation have explicit limits.
Local checks, screenshots and one successful run are not production certification.
Databricks Free Edition quotas and platform restrictions still apply.

## Attribution

Source: [NYC TLC trip-record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
Independent learning project; not affiliated with or endorsed by NYC or
Databricks. These transformed, policy-filtered results are not official City
statistics. Source accuracy and completeness are not guaranteed.
