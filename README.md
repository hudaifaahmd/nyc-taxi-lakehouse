# NYC Taxi Lakehouse

[Project overview and results](https://hudaifa-ahamed.netlify.app/projects/nyc-taxi-lakehouse/) · [Interactive dashboards](https://hudaifa-ahamed.netlify.app/projects/nyc-taxi-lakehouse/dashboards/)

A monthly data-engineering pipeline for NYC yellow-taxi trip records, built with Databricks, PySpark, SQL and Delta Lake. It turns public Parquet files into reconciled analytical tables while retaining source provenance, rejected records and operational history.

## Pipeline

```text
TLC monthly Parquet -> Landing Volume -> Bronze
                                          |
                                    Quality rules
                                     /         \
                               Quarantine     Silver
                                                |
                                      Gold fact + dimensions
                                                |
                                    Persisted reconciliation
                                                |
                                SQL analytics / public aggregates
```

- **Monthly replacement:** source-period predicates replace the selected month's Bronze, quarantine, Silver and Gold facts without appending rerun duplicates.
- **Explicit quality handling:** rejected rows retain their source fields and validation reasons; accepted boundary pickups remain visible as warnings.
- **Independent reconciliation:** persisted counts, rounded monetary metrics, nulls, ingestion batches and dimension references are checked before completion.
- **Bounded orchestration:** the controller prioritizes the latest eligible month, detected revisions and unfinished history within a configured processing budget.
- **Controlled publication:** a dependent task validates, stages and verifies allowlisted monthly aggregates before publishing them to the public summary repository.

The [portfolio case study](https://hudaifa-ahamed.netlify.app/projects/nyc-taxi-lakehouse/) explains the results. Its dashboards support source-period filtering and consume public aggregate data, without exposing the Databricks workspace.

## Notebook Map

| Notebook | Responsibility |
| --- | --- |
| [00 Project overview](00_project_overview.ipynb) | Architecture and interpretation |
| [01 Setup and configuration](01_setup_and_config.ipynb) | Parameters, table names, date policy and shared helpers |
| [02 Bronze ingestion](02_ingest_to_bronze.ipynb) | Download, validate and checksum the input; replace Bronze |
| [03 Quality checks](03_quality_checks.ipynb) | Classify records and replace quarantine |
| [04 Silver transformation](04_transform_to_silver.ipynb) | Standardize accepted records and derive analytical fields |
| [05 Gold model](05_build_gold_model.ipynb) | Merge shared dimensions and replace monthly facts |
| [06 Reconciliation](06_reconciliation_and_audit.ipynb) | Persist layer-accounting and integrity checks |
| [07 Reporting queries](07_reporting_queries.ipynb) | Monthly trends, zone/route queries and quality summaries |
| [08 Monthly runner](08_monthly_pipeline_runner.ipynb) | Plan or execute bounded sequential monthly work |
| [09 Runtime sources](09_runtime_sources.ipynb) | Generated, checksummed stages and controller |
| [10 Backfill status](10_backfill_status.ipynb) | Latest current-policy coverage and run history |
| [11 Publication readiness](11_publication_readiness.ipynb) | Optional connectivity and secret-metadata diagnostic |
| [12 Summary sources](12_public_summary_sources.ipynb) | Generated aggregate-validation helpers |
| [13 Prepare summary](13_prepare_public_summary.ipynb) | Validate monthly aggregate observations |
| [16 Publish summary](16_publish_public_summary.ipynb) | Publish validated aggregates after successful processing |

Numbering preserves existing notebook dependencies. The gaps correspond to earlier private connection/publishing utilities, which are not required by the public workflow.

## Running In Databricks

Prerequisites: a workspace with compatible PySpark/Delta compute, Unity Catalog schema and Volume permissions, and HTTPS access to the official source host. Publication additionally requires a managed secret and permission to update the configured summary repository.

1. Import the notebooks into one folder, preserving their base names and relative dependencies.
2. Open notebook 08 with `mode=plan`, the intended source range, and `force_reprocess=false`.
3. Confirm the catalog, schema, due source month, processing budget and completion policy. Plan mode may create the schema and event table; it does not probe source files or run ETL.
4. Use `mode=run` for the intended bounded execution. Inspect notebook 10 and persisted reconciliation outcomes.
5. Configure notebook 16 only after publication permissions and destination are reviewed, as a dependent task that runs after notebook 08 succeeds.

Default namespace: `workspace.nyc_taxi_lakehouse`. The default run budget is one month; the supported range is 1-30. The latest eligible source month is the current Asia/Kolkata calendar month minus two months. Eligibility is a project scheduling rule, not a guarantee that TLC has published a file.

See [Operations](docs/operations_runbook.md) before updating an existing deployment. Do not run every notebook sequentially with “Run all”: standalone notebook dependencies and the monthly controller are different execution paths.

## Data And Metrics

- A **source period** identifies the input file, not every pickup's calendar month.
- **Trip counts** count accepted source rows; no globally unique trip key or cross-file deduplication is assumed.
- **Quarantine rate** is rejected rows divided by Bronze rows. A row can have multiple rejection reasons.
- **Source amounts** retain the input's fare and total-amount meaning. They are not net revenue or profit.
- **Reconciled cents** sum per-row `DECIMAL(20,2)` conversions. Stored fare and total fields remain doubles.
- The default one-day boundary grace retains adjacent-month pickups as warnings; accepted pickups outside that window fail reconciliation.
- A successful warning completion means required checks passed with recorded warnings, not that every quality check was green.

## Operational Boundaries

Individual Delta writes are atomic at table level; the full multi-table workflow is not one transaction. Completion events and the dependent publisher provide the release gate. Recent revision checks use HTTP metadata, which cannot detect every possible source change. Input timestamp timezone interpretation, source deduplication and financial accounting definitions require separate domain decisions.

The repository demonstrates an implemented batch pipeline. It does not assert an enterprise SLA, audited production certification or exhaustive failure-recovery validation.

## Further Reading

- [Architecture](docs/architecture.md)
- [Data contract and table responsibilities](docs/data_contract.md)
- [Operations and recovery](docs/operations_runbook.md)

## Source Attribution

Trip files and the zone lookup are published by the [NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The source publisher's data dictionary and usage terms govern interpretation and reuse.
