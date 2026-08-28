# Migration Notes: Shared Files — TMD_processing/ALL_TYPES/run

## 1. Summary
The legacy KornShell script `all_types_graph.ksh`, which executed an Ab Initio data integration graph, has been migrated to **Google Cloud BigQuery**. 

The business purpose of this pipeline is to filter, partition, and mask marketing and offer execution measures (`x_tos_measures`) into three distinct functional categories: Cancellations, Products, and Quotes/Contracts. For each category, the pipeline produces:
1. A **standard consolidated dataset** where team identifiers are masked (set to `NULL`) if they do not pass a visibility lookup check.
2. A **weekly dataset** containing only records prior to Tuesday of the current workweek.

The legacy file-based and Oracle-dependent ETL process has been fully consolidated into a single, native BigQuery SQL scripting pipeline.

---

## 2. Generated Artifacts
The migration process has produced the following target artifact:

* **`TMD_processing/ALL_TYPES/run/all_types_graph.sql`**
  * **Role**: A BigQuery Standard SQL scripting file containing the complete data transformation, filtering, lookup, and table-writing logic. This replaces the legacy shell wrapper, the compiled Ab Initio graph (`all_types_graph.mp`), and the temporary catalog management utilities.

---

## 3. Key Design Decisions

### SQL-First Architecture (BQSQL)
The legacy Ab Initio graph was translated into a single BigQuery SQL script. This eliminates the need for external ETL engines or containerized Spark jobs, leveraging BigQuery's serverless execution engine for optimal performance and cost-efficiency.

### Temporary Tables for Lookups
In the legacy graph, Phase 0 extracted visible teams from Oracle into a local lookup file (`lkp_team_virt_ccos.dat`). In BigQuery, this is implemented using a session-scoped `TEMP TABLE` (`lkp_teamvirt_ccos`). This avoids persisting intermediate lookup tables in the production dataset while maintaining high-performance join capabilities.

### Date Boundary Translation
The legacy script calculated the weekly cutoff dynamically using Ab Initio date math:
`datetime_add(now(), ((datetime_day_of_week(now()) - 2) * -1))`
This has been translated into standard BigQuery syntax to dynamically resolve to the Tuesday of the current ISO week:
`DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 DAY)`

### Table Name Normalization
Legacy Oracle tables containing special characters (such as the dollar sign `$` in `CCR$TA_F_TEAMSICHTBARKEIT`) have been normalized to standard BigQuery snake_case naming conventions (e.g., `ccr_ta_f_teamsichtbarkeit`).

### Data Type Preservation & Formatting Trade-offs
The legacy script performed string manipulation on numeric fields (e.g., replacing decimal dots with commas in `subventionen` via `string_replace(in.mea_2, '.', ',')`). In BigQuery, these metrics are stored natively as `NUMERIC` types to preserve mathematical integrity. Any localized formatting (such as German decimal commas) is deferred to the downstream presentation or file-export layer.

---

## 4. Manual Steps Before Go-Live

### 1. Schema and Dataset Creation
Ensure that the target BigQuery dataset (referenced as `{{project_id}}.dataset` in the script) is created in your designated GCP region.

### 2. Source Table Migration
Verify that the following legacy Oracle tables have been successfully migrated to BigQuery and are populated:
* `ccr_ta_f_teamsichtbarkeit`
* `ccr_ta_s_sdm_team`
* `ccr_ta_s_sdm_abteilung`

### 3. Input Data Ingestion
The legacy source `x_tos_measures.dat` was a multi-file system (MFS) dataset. An ingestion pipeline (e.g., Cloud Storage to BigQuery load job) must be established to load this raw data into the BigQuery table `x_tos_measures` before running this script.

### 4. IAM & Permissions
The service account executing the BigQuery SQL script must be granted the following IAM roles:
* `roles/bigquery.jobUser` (to run the query jobs)
* `roles/bigquery.dataEditor` (on the target dataset to create/replace tables)

### 5. Scheduling & Orchestration
Configure your orchestrator (e.g., Cloud Composer / Apache Airflow) to run the SQL script. 
* Replace the legacy UC4 job trigger with an Airflow `BigQueryInsertJobOperator` pointing to `all_types_graph.sql`.
* Ensure that parameter placeholders like `{{project_id}}` and `dataset` are dynamically substituted by the orchestrator at runtime.

---

## 5. Known Gaps & Unresolved References

### Downstream Dependencies (Wiring Not Finalized)
* **`DW.DWH_ALL_TYPES_MASTER`**: This downstream consumer job has not yet been migrated. The orchestration workflow must not be finalized until this consumer is ready to receive the output tables.
* **`TMD_processing/ALL_TYPES/run/all_types_graph.ksh`**: The legacy scheduling loop/trigger must be verified and mapped to Airflow DAG dependencies during the final orchestration migration phase.

### Retired Components
* **`AB_CATALOG_FUNCTIONS.KSH`**: This legacy helper script managed Ab Initio temporary catalogs. It has been retired as BigQuery handles lookups natively via SQL joins.

### Physical File Exports
If downstream legacy systems still require physical `.dat` files, you must append a BigQuery `EXPORT DATA` statement to the end of the SQL script (or add an Airflow export task) to unload the target tables to a Google Cloud Storage bucket.

---

## 6. Validation

### Dry Run Validation
Before executing with data, perform a BigQuery dry run of `all_types_graph.sql` to validate syntax, permissions, and schema references.

### Functional Testing
1. Populate the source tables with a controlled set of test records.
2. Execute `all_types_graph.sql`.
3. Verify the following "passing" criteria:
   * **Partitioning**: Records in `x_tos_measures` are correctly split into their respective target tables based on `tos_mea_group_name`.
   * **Weekly Cutoff**: Tables suffixed with `_wk` contain only records where `stichtag` is strictly less than Tuesday of the current week.
   * **Masking**: For standard tables (`tos_cancellations`, `tos_products`, `tos_quotes_contracts`), verify that `sdm_team_id` is set to `NULL` if the team/date combination does not exist in the active/visible teams lookup.
   * **Concatenation**: `tos_products` and `tos_products_wk` correctly generate `tcn_offer_product_id` as `tos_offer_id ~ tcn_product_id`.

---

## 7. Rollback Procedure

In the event of a deployment failure or data corruption:

1. **Revert Orchestration**: In Cloud Composer / Airflow, pause the new DAG and re-enable the legacy UC4 schedule or KornShell execution path.
2. **Restore Target Tables**: If target tables were corrupted, restore them to their pre-execution state using BigQuery's table snapshot/time-travel feature:
   ```sql
   CREATE OR REPLACE TABLE `{{project_id}}.dataset.tos_cancellations`
   AS SELECT * FROM `{{project_id}}.dataset.tos_cancellations`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
   *(Repeat this restore statement for all 6 target tables as necessary).*