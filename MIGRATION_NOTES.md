# MIGRATION_NOTES.md: r_ausd_rechempf.ksh

## 1. Summary

The KornShell job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh`, along with its dependencies (`k_ausd_rechempf.ksh` and `d_ausd_rechempf.sql`), has been migrated. This job is responsible for provisioning the contract cache for demand scoring (FOS) by extracting and transforming data from various Oracle source tables into a set of intermediate and final snapshot tables (`sof$ta_means_of_pay`, `sof$ta_bank`, `sof$ta_bank_verb`, `sof$ta_bank_zuord`, `sof$ta_p_rech_empf`, `sof$ta_p_d1_vpn`).

The migration target platform is Google Cloud Platform, leveraging **BigQuery** for data storage and transformation, and **Cloud Composer (Apache Airflow)** for orchestration.

## 2. Generated Artifacts

The following files were generated as part of this migration:

*   **`ddl/isbert_schema.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `isbert_schema` dataset and the `dwtk_meldungen` table within BigQuery. This table is a source for deriving the `v_datum` parameter.
*   **`ddl/carmen_bpd.sql`**
    *   **Role:** BigQuery DDL script to create the `carmen_bpd` dataset and its associated source tables (`ta_means_of_payment`, `ta_bank`, `ta_bank_international`) in BigQuery. These tables replace the Oracle tables previously accessed via a database link (`@pcrs1`).
*   **`ddl/fos_source.sql`**
    *   **Role:** BigQuery DDL script to create the `fos_source` dataset and its associated source tables (`sof_ta_e_reach_re`, `sof_ta_e_business_re`, `sof_ta_e_regulierer`) in BigQuery.
*   **`ddl/dwh_view.sql`**
    *   **Role:** BigQuery DDL script to create the `dwh_view` dataset and the `vi_s_ibasisprodukt` table in BigQuery.
*   **`ddl/fos_snapshots.sql`**
    *   **Role:** BigQuery DDL script to create the `fos_snapshots` dataset and all target snapshot tables (`sof_ta_means_of_pay`, `sof_ta_bank`, `sof_ta_bank_verb`, `sof_ta_bank_zuord`, `sof_ta_p_rech_empf`, `sof_ta_p_d1_vpn`) in BigQuery. These tables receive the transformed data.
*   **`sql/d_ausd_rechempf_bq.sql`**
    *   **Role:** BigQuery SQL script containing the complete data extraction and transformation logic. This script is the direct translation of the original `d_ausd_rechempf.sql` to BigQuery SQL syntax, including variable declarations, `TRUNCATE` statements, and all `INSERT...SELECT` operations.
*   **`dags/r_ausd_rechempf_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG orchestrates the execution of the `d_ausd_rechempf_bq.sql` script within BigQuery, handles parameter passing (`stichtag`, `wiederanlaufwert`), and manages the overall job flow, logging, and error handling. It replaces the functionality of `r_ausd_rechempf.ksh` and `k_ausd_rechempf.ksh`.

## 3. Key Design Decisions

*   **Cloud-Native Architecture:** The decision to migrate to BigQuery and Cloud Composer leverages Google Cloud's managed services, offering scalability, high availability, reduced operational overhead, and cost-efficiency compared to the on-premises Oracle and KornShell environment.
*   **Direct SQL Translation:** The core data transformation logic from `d_ausd_rechempf.sql` was directly translated into BigQuery SQL. This approach minimizes re-engineering of complex business logic, reducing the risk of introducing new bugs and accelerating the migration process. The trade-off is that some Oracle-specific constructs (e.g., `/*+ parallel */` hints) were removed as BigQuery handles parallelism automatically, and others (e.g., `NVL`, `TO_DATE`) were replaced with BigQuery equivalents.
*   **Consolidated BigQuery SQL Script:** The entire transformation logic (including variable declaration, truncations, and multiple inserts) is encapsulated within a single BigQuery SQL script (`d_ausd_rechempf_bq.sql`). This simplifies the Airflow DAG by allowing a single `BigQueryExecuteQueryOperator` to execute the entire workflow, rather than chaining multiple operators for each individual SQL step. This improves atomicity and reduces Airflow task overhead.
*   **Airflow for Orchestration:** Cloud Composer (Airflow) was chosen to replace the KornShell orchestration and UC4 scheduling due to its robust capabilities for scheduling, dependency management, parameterization, logging, and monitoring, which are superior to custom shell scripting.
*   **Pre-migration of External Sources:** Source tables accessed via Oracle DB links (e.g., `bpd$ta_means_of_payment@pcrs1`) are assumed to be pre-migrated and synchronized into dedicated BigQuery datasets (e.g., `carmen_bpd`). This design decision decouples the core transformation job from direct Oracle connectivity, simplifying the BigQuery SQL and focusing the migration effort on the transformation logic itself. The complexity of Oracle-to-BigQuery data ingestion is handled by separate pipelines.
*   **Parameter Handling:** Airflow's native `params` mechanism is utilized for passing runtime parameters like `stichtag` and `wiederanlaufwert`, providing a clean and standardized way to configure job runs.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps and prerequisites must be completed:

1.  **BigQuery Dataset Creation:**
    *   Manually create the following BigQuery datasets in your GCP project:
        *   `isbert_schema`
        *   `carmen_bpd`
        *   `fos_source`
        *   `dwh_view`
        *   `fos_snapshots`
    *   Ensure appropriate data location (e.g., `EU`, `US`) is selected for each dataset.

2.  **BigQuery Table Creation (DDL Execution):**
    *   Execute the generated DDL scripts (`ddl/*.sql`) in the BigQuery console or via `bq` command-line tool to create all necessary source and target tables within their respective datasets.
    *   Example: `bq query --use_legacy_sql=false < ddl/isbert_schema.sql`

3.  **IAM Permissions Configuration:**
    *   **Cloud Composer Service Account:** Grant the service account associated with your Cloud Composer environment the `BigQuery Data Editor` role (or more granular permissions like `BigQuery Data Viewer` and `BigQuery Data Editor` on specific datasets) for the GCP project containing the BigQuery datasets. This allows the Airflow DAG to read from source tables and write/truncate target tables.
    *   **Data Ingestion Service Accounts:** Ensure any service accounts used for ingesting data from Oracle to BigQuery (e.g., Dataflow, Cloud Storage Transfer) have the necessary permissions to read from the Oracle source and write to the BigQuery source datasets (`isbert_schema`, `carmen_bpd`, `fos_source`, `dwh_view`).

4.  **BigQuery Source Data Ingestion:**
    *   **Crucial Step:** Establish and verify data ingestion pipelines to populate the BigQuery source tables (`isbert_schema.dwtk_meldungen`, `carmen_bpd.*`, `fos_source.*`, `dwh_view.vi_s_ibasisprodukt`) with current and historical data from the legacy Oracle system. These pipelines must be operational and synchronized *before* the Airflow DAG runs.
    *   The frequency and method of ingestion (e.g., daily batch, CDC) should align with the business requirements for data freshness.

5.  **Airflow Connection Configuration:**
    *   Ensure the `google_cloud_default` connection is properly configured in your Airflow environment. This connection is used by the `BigQueryExecuteQueryOperator` to authenticate with BigQuery.

6.  **Airflow DAG Deployment:**
    *   Upload the `dags/r_ausd_rechempf_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   Upload the `sql/d_ausd_rechempf_bq.sql` file to a location accessible by the Airflow worker, typically a `dags/sql` subfolder or a Cloud Storage bucket referenced by the DAG.

7.  **Scheduling Configuration:**
    *   Once deployed, configure the desired schedule for the `r_ausd_rechempf_dag` within the Airflow UI (e.g., daily at a specific time, or triggered by an external event).

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up, potential redesign, or require further investigation:

*   **`v_datum` Derivation Accuracy:** The `v_datum` is derived from `isbert_schema.dwtk_meldungen`. The accuracy of this derivation depends entirely on the correct and timely ingestion of data into `isbert_schema.dwtk_meldungen` in BigQuery, ensuring it precisely reflects the `timecreated` of `BERT_DROP_TEMP_TABLE` events from the legacy system.
*   **Custom Shell Function Behavior:** The original KornShell scripts utilized several custom helper functions (e.g., `starteSQLSkript`, `DWMSG_MeldeFehler`, `h_alis_date.ksh`). While Airflow's native features cover most of this, subtle behaviors, specific error codes, or unique logging formats from these legacy functions might not be perfectly replicated. This requires careful validation during testing.
*   **Detailed Data Type Mapping:** While general data type conversions have been applied, a comprehensive, column-by-column data type mapping document for all source tables (especially those from Oracle DB links) is recommended to ensure precise BigQuery data types (e.g., `NUMERIC` for financial values, `TIMESTAMP` vs `DATETIME` vs `DATE` for date/time fields) and prevent potential data loss or truncation.
*   **Performance Validation:** The original Oracle SQL used `/*+ parallel(4) */` hints, indicating performance tuning was applied. While BigQuery handles parallelism automatically, post-migration performance validation is crucial to ensure the BigQuery queries meet or exceed the performance of the legacy system. This may involve BigQuery query optimization if necessary.
*   **Missing Table Descriptions (`DESC`):** The original SQL script included `DESC` statements for existence checks. In BigQuery, table existence is typically managed at the schema deployment level. If runtime checks for table existence are still required, they would need to be implemented using BigQuery's `INFORMATION_SCHEMA` or by ensuring robust table creation/management within the deployment pipeline.
*   **Character Encoding:** Potential issues with non-ASCII characters (e.g., German umlauts) in data, especially if the source Oracle database used a different character set than BigQuery's default UTF-8. This should be verified with sample data.
*   **`v_carmen` Placeholder:** The `DECLARE v_carmen STRING DEFAULT '@pcrs1';` in `d_ausd_rechempf_bq.sql` is currently a placeholder. If this value needs to be dynamic or represent a different external reference in the BigQuery environment, it needs to be addressed. Currently, it's not actively used in the BigQuery SQL logic as the `carmen_bpd` tables are assumed to be directly available.

## 6. Validation

Validation of the migrated job involves ensuring functional equivalence and data integrity between the legacy and new systems.

**How to Run Tests:**

1.  **Manual Trigger (Airflow):** In the Cloud Composer Airflow UI, manually trigger the `r_ausd_rechempf_dag` for a specific `stichtag` (e.g., a historical date for which legacy data is available).
2.  **BigQuery Console Execution:** For granular testing, individual `INSERT` statements or the entire `d_ausd_rechempf_bq.sql` script can be executed directly in the BigQuery console.
3.  **Automated Testing (Optional):** Develop unit or integration tests using Python and BigQuery client libraries to validate specific SQL logic or data transformations.

**What "Passing" Means:**

*   **DAG Success:** The `r_ausd_rechempf_dag` completes successfully in Airflow without any task failures or retries.
*   **Target Table Population:** All target tables in the `fos_snapshots` dataset (`sof_ta_means_of_pay`, `sof_ta_bank`, `sof_ta_bank_verb`, `sof_ta_bank_zuord`, `sof_ta_p_rech_empf`, `sof_ta_p_d1_vpn`) are populated with data.
*   **Row Count Verification:** For a given `stichtag`, the row counts in each target table in BigQuery should match the corresponding row counts in the legacy Oracle tables. A small, acceptable delta might be defined if minor data differences are expected due to system variations.
*   **Data Sample Comparison:** Perform spot checks on key columns and records across all target tables. Select random samples of data from BigQuery and compare them against the corresponding data in the legacy Oracle system to ensure data accuracy and integrity.
*   **`v_datum` Consistency:** Verify that the `v_datum` derived in BigQuery (from `isbert_schema.dwtk_meldungen`) matches the `Stichtag` value used by the legacy Oracle job for the same run.
*   **No Data Type Errors:** No data type conversion errors, truncation, or unexpected data format issues are observed in the BigQuery target tables.
*   **Performance Metrics:** The BigQuery job completes within an acceptable time frame, ideally matching or improving upon the legacy job's execution time.

## 7. Rollback Procedure

In case of critical issues or failures after go-live, the following rollback procedure can be initiated:

1.  **Immediate DAG Disablement:**
    *   If the Airflow DAG is failing repeatedly or producing incorrect data, immediately disable the `r_ausd_rechempf_dag` in the Cloud Composer Airflow UI to prevent further execution.

2.  **Data Rollback (if necessary):**
    *   Since this job truncates and re-inserts data, a direct "undo" of the data is not straightforward.
    *   **Option A (Preferred):** If a previous successful run's data is available (e.g., from the day before), restore the `fos_snapshots` tables from a BigQuery snapshot or a backup if such mechanisms are in place.
    *   **Option B:** If the legacy system is still operational, re-enable and re-run the original Oracle job for the affected `Stichtag` to overwrite the potentially corrupted data in the legacy environment (assuming the legacy system is the source of truth during transition).

3.  **Code Rollback (Airflow DAG):**
    *   If the issue is identified as a bug in the Airflow DAG or SQL script, revert the `dags/r_ausd_rechempf_dag.py` and `sql/d_ausd_rechempf_bq.sql` files in the Cloud Composer DAGs bucket to a previous, known-good version from your version control system.
    *   After reverting, re-enable the DAG and re-run it for the affected `Stichtag`.

4.  **Full System Rollback (to Legacy):**
    *   If the migration proves to be fundamentally flawed or unrecoverable within the GCP environment, the ultimate rollback is to fully revert to the legacy system.
    *   Ensure the original UC4 job `DW.BERT_P_RECH_EMPF.xml` is re-enabled and the Oracle environment is fully operational and processing data as before.
    *   This option implies that the legacy system remains available and maintained during the transition period.