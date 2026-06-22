# MIGRATION_NOTES.md for DW.BERT_AUSD_BP_TA_ICCID_VERTRAG

## 1. Summary

The `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` job, responsible for preparing and aggregating ICCID (SIM card ID) data for contract IDs, has been migrated. The original job involved a multi-layered orchestration using UC4 and KornShell scripts, executing core data transformation logic in Oracle SQL.

The job has been migrated to Google Cloud Platform (GCP), leveraging:
*   **Google Cloud Composer (Apache Airflow)** for orchestration, replacing the UC4 job definition and KornShell scripts.
*   **Google BigQuery** for data storage and transformation, replacing Oracle tables and SQL.

The migration involved translating the complex Oracle SQL aggregation and pivoting logic into BigQuery SQL and reimplementing the parameter parsing, date validation, and error handling within an Airflow DAG.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`sql/ddl/sof_ta_iccid_einzeln.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the target `sof_ta_iccid_einzeln` table in BigQuery. This table serves as the BigQuery equivalent of the legacy Oracle source table `sof$ta_iccid_einzeln`.
*   **`sql/ddl/sof_ta_iccid_vertrag.sql`**
    *   **Role:** BigQuery DDL script to create the target `sof_ta_iccid_vertrag` table in BigQuery. This table is the BigQuery equivalent of the legacy Oracle target table `sof$ta_iccid_vertrag`, designed to store the aggregated and pivoted ICCID data.
*   **`sql/ddl/dwtk_meldungen.sql`**
    *   **Role:** BigQuery DDL script to create the `dwtk_meldungen` table in BigQuery. This table is the BigQuery equivalent of the legacy Oracle `isbert_schema.dwtk_meldungen` table, used for metadata and dynamic variable derivation.
*   **`sql/ddl/pool_basisprodukt.sql`**
    *   **Role:** BigQuery DDL script to create the `pool_basisprodukt` table in BigQuery. This table is the BigQuery equivalent of the legacy Oracle `PoolBasisprodukt` table, used for tracking job status.
*   **`sql/transform/d_ausd_bp_ta_iccid_vertrag.sql`**
    *   **Role:** BigQuery SQL script containing the core data transformation logic. This script translates the Oracle `INSERT...SELECT` statement with `MAX()` aggregations for pivoting into BigQuery-compatible SQL, populating `sof_ta_iccid_vertrag` from `sof_ta_iccid_einzeln`.
*   **`dags/bert_ausd_bp_ta_iccid_vertrag_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG orchestrates the entire job, replacing the UC4 job and KornShell scripts. It handles parameter parsing, date validation, dynamic variable derivation, target table truncation, execution of the BigQuery transformation, and job status updates.

## 3. Key Design Decisions

*   **Orchestration Layer Migration (UC4/KornShell to Airflow):**
    *   **Decision:** Replaced the complex, multi-layered UC4 job and KornShell scripts (`r_ausd_bp_ta_iccid_vertrag.ksh`, `k_ausd_bp_ta_iccid_vertrag.ksh`) with a single, Python-based Airflow DAG.
    *   **Rationale:** Airflow provides a cloud-native, scalable, and observable orchestration platform. Python allows for more robust parameter handling, date validation, and dynamic logic compared to shell scripting, improving maintainability and error handling.
*   **Data Storage and Transformation Migration (Oracle to BigQuery):**
    *   **Decision:** Migrated source and target Oracle tables (`sof$ta_iccid_einzeln`, `sof$ta_iccid_vertrag`, `isbert_schema.dwtk_meldungen`, `PoolBasisprodukt`) to Google BigQuery. The core Oracle SQL transformation (`d_ausd_bp_ta_iccid_vertrag.sql`) was rewritten for BigQuery.
    *   **Rationale:** BigQuery offers a fully managed, highly scalable, and cost-effective data warehouse solution with superior performance for analytical queries. It eliminates the operational overhead of managing an Oracle database.
*   **SQL Transformation Logic (Pivoting):**
    *   **Decision:** The Oracle `MAX()` aggregation with `CASE WHEN` statements for pivoting ICCID attributes was directly translated to BigQuery SQL using the same `MAX(CASE WHEN ...)` pattern.
    *   **Rationale:** This approach directly mirrors the original logic, ensuring functional equivalence. While BigQuery has a `PIVOT` clause, `MAX(CASE WHEN ...)` is often more flexible for complex pivoting scenarios and directly maps to the existing Oracle logic.
*   **Parameter Handling and Date Validation:**
    *   **Decision:** Legacy shell script parameter parsing (`Stichtag`, `Wiederanlaufwert`) and date validation (`DWDate_Datum_Check`) were reimplemented as Python functions within Airflow tasks.
    *   **Rationale:** Integrates seamlessly with Airflow's parameter passing mechanisms and allows for robust, testable validation logic using Python's datetime capabilities.
*   **Dynamic Variable Derivation (`s_datum`):**
    *   **Decision:** The Oracle SQL*Plus `DEFINE` logic to derive `v_datum` from `isbert_schema.dwtk_meldungen` was replaced by a dedicated Python task in Airflow. This task queries the BigQuery `dwtk_meldungen` table and pushes the derived `s_datum` value to XComs for use by subsequent tasks.
    *   **Rationale:** Airflow's Python tasks and XComs provide a clean and efficient way to handle dynamic variable generation and sharing between tasks.
*   **Target Table Truncation:**
    *   **Decision:** The Oracle stored procedure call (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`) for truncating the target table was replaced by a direct BigQuery `TRUNCATE TABLE` DDL statement executed via an Airflow BigQuery operator.
    *   **Rationale:** Simplifies the operation, leveraging BigQuery's native DDL capabilities directly within the Airflow workflow.
*   **Logging and Status Updates:**
    *   **Decision:** Custom shell logging mechanisms (`DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK`) were replaced by Airflow's native logging and a BigQuery `MERGE` statement to update the `pool_basisprodukt` status table.
    *   **Rationale:** Leverages Airflow's built-in observability features and BigQuery's DML for consistent and centralized status tracking.
*   **Oracle Hints Removal:**
    *   **Decision:** Oracle-specific hints (e.g., `/*+ full(rp) parallel(rp,4) */`) were removed from the BigQuery SQL.
    *   **Rationale:** BigQuery's columnar storage and automatic query optimizer handle performance and parallelism inherently, rendering Oracle-specific hints unnecessary and potentially counterproductive.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset `gcp_project.dataset` exists in your GCP project. If not, create it.
2.  **BigQuery Table Creation (DDL):**
    *   Execute the generated DDL scripts (`sql/ddl/*.sql`) in BigQuery to create the following tables:
        *   `gcp_project.dataset.sof_ta_iccid_einzeln`
        *   `gcp_project.dataset.sof_ta_iccid_vertrag`
        *   `gcp_project.dataset.dwtk_meldungen`
        *   `gcp_project.dataset.pool_basisprodukt`
    *   Consider applying appropriate partitioning and clustering keys to `sof_ta_iccid_einzeln` and `sof_ta_iccid_vertrag` for optimal query performance, based on access patterns.
3.  **Initial Data Ingestion:**
    *   Establish a data pipeline (e.g., using Cloud Data Fusion, Database Migration Service, or custom scripts) to ingest historical data from the legacy Oracle tables (`sof$ta_iccid_einzeln`, `isbert_schema.dwtk_meldungen`, `PoolBasisprodukt`) into their respective BigQuery counterparts.
    *   Ensure ongoing data synchronization for `sof_ta_iccid_einzeln` and `dwtk_meldungen` if they are continuously updated in the legacy system.
4.  **IAM Permissions Configuration:**
    *   Grant the necessary BigQuery roles (e.g., `BigQuery Data Editor` for DML/DDL operations, `BigQuery Data Viewer` for SELECT) to the Google service account associated with your Cloud Composer environment's Airflow workers.
    *   Ensure the Composer environment's service account has sufficient permissions to interact with other GCP services if any are added in the future.
5.  **Airflow Connection Setup:**
    *   Verify that a `google_cloud_default` connection (or a custom BigQuery connection) is properly configured in your Airflow environment, allowing the DAG to connect to BigQuery.
6.  **Airflow DAG Deployment:**
    *   Upload the `dags/bert_ausd_bp_ta_iccid_vertrag_dag.py` file to the DAGs folder of your Cloud Composer environment.
7.  **Scheduling Configuration:**
    *   Configure the schedule of the `bert_ausd_bp_ta_iccid_vertrag_dag` in the Airflow UI to match the original execution frequency and timing of the legacy UC4 job.

## 5. Known Gaps & Unresolved References

The following items have been identified as potential gaps or require further follow-up:

*   **"Retire" Migration Bucket for SQL:** The original `d_ausd_bp_ta_iccid_vertrag.sql` was categorized as "Retire" in the migration design document. While a direct translation to BigQuery SQL has been provided, this "Retire" flag suggests that the underlying business logic or data model might warrant a more significant redesign or re-evaluation. This should be a point of discussion for future optimization or refactoring efforts (B4 item).
*   **Commented-out Shell Scripting:** The `k_ausd_bp_ta_iccid_vertrag.ksh` script contained significant commented-out `sed`, `sort`, and `join` commands. It is assumed these were inactive and thus not migrated. If these operations were ever active or become active in the future, their functionality would need to be re-evaluated and implemented in a BigQuery-compatible manner, potentially using Dataflow or PySpark for complex text processing if BigQuery SQL is insufficient.
*   **Schema Evolution for `MSx_ICCID` Fields:** The target schema for `sof_ta_iccid_vertrag` explicitly defines fields up to `MS10_ICCID`. If the number of MultiSIM slave cards (`MSx`) can dynamically increase beyond 10, the current fixed schema will not accommodate new fields without a DDL change. A more flexible BigQuery schema, such as using `ARRAY<STRUCT>` or a different data modeling approach, might be necessary to handle dynamic schema evolution.
*   **Oracle Database Link `v_carmen = "@pcrs1"`:** The `DEFINE v_carmen = "@pcrs1"` in the original Oracle SQL suggests a database link or connection to another Oracle instance. The purpose and data source behind `pcrs1` were not fully resolved during this migration. It is crucial to identify this external dependency and ensure its data is either replicated to BigQuery or that the dependency is no longer required.
*   **`dwtk_meldungen` Data Population:** The mechanism by which the `isbert_schema.dwtk_meldungen` table is populated in the legacy Oracle environment needs to be fully understood. For the BigQuery `dwtk_meldungen` table to function correctly (especially for deriving `s_datum`), its data population process must be replicated or replaced in GCP.
*   **`PoolBasisprodukt` Update Logic:** The `update_job_status` task in the Airflow DAG provides a basic `MERGE` statement to update the status to 'SUCCESS'. The full logic for updating `PoolBasisprodukt` (e.g., specific status codes, error messages, additional metadata) needs to be confirmed and fully implemented to match the legacy system's behavior.

## 6. Validation

To ensure the successful migration and correct functioning of the `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` job on GCP, the following validation steps should be performed:

1.  **Unit Testing of BigQuery SQL:**
    *   **How to run:** Execute the `sql/transform/d_ausd_bp_ta_iccid_vertrag.sql` script directly in BigQuery with a small, representative dataset loaded into `gcp_project.dataset.sof_ta_iccid_einzeln`.
    *   **"Passing" means:** The query executes successfully, and the resulting data in `gcp_project.dataset.sof_ta_iccid_vertrag` matches the expected output based on the legacy Oracle job's logic for the given input.
2.  **Airflow DAG Integration Testing:**
    *   **How to run:** Trigger the `bert_ausd_bp_ta_iccid_vertrag_dag` manually in the Airflow UI with various parameters, including:
        *   Default `p_stichtag` and `p_wiederanlaufWert`.
        *   Specific historical `p_stichtag` values.
        *   Invalid `p_stichtag` formats to test error handling.
    *   **"Passing" means:**
        *   The DAG completes successfully without any failed tasks.
        *   All tasks (parameter parsing, `s_datum` derivation, truncate, transformation, status update) execute as expected.
        *   Airflow logs show no unexpected errors or warnings.
3.  **Data Correctness Validation:**
    *   **How to run:**
        *   Select a specific `p_stichtag` (e.g., for a recent daily run).
        *   Execute the legacy Oracle job for that `Stichtag`.
        *   Execute the migrated Airflow DAG for the same `p_stichtag`.
        *   Compare the data in `gcp_project.dataset.sof_ta_iccid_vertrag` with the data in `sof$ta_iccid_vertrag` (Oracle) using data comparison tools or SQL queries (e.g., `EXCEPT` or `MINUS` queries).
    *   **"Passing" means:** The data in the BigQuery target table is identical to the data produced by the legacy Oracle job for the chosen test period. Any discrepancies must be investigated and resolved.
4.  **Performance Monitoring:**
    *   **How to run:** Monitor the execution time of the Airflow DAG and the BigQuery transformation query in the Airflow UI and BigQuery Jobs history.
    *   **"Passing" means:** The execution time of the migrated job is comparable to or better than the legacy job, and it meets defined Service Level Agreements (SLAs).
5.  **Logging and Status Table Verification:**
    *   **How to run:** Review Airflow task logs for detailed execution information. Query the `gcp_project.dataset.pool_basisprodukt` table to check the job's status and `last_update` timestamp.
    *   **"Passing" means:** Logs are comprehensive and correctly reflect job execution. The `pool_basisprodukt` table is updated with the correct status ('SUCCESS') and timestamp after a successful run.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop Airflow DAG:**
    *   Immediately pause or delete the `bert_ausd_bp_ta_iccid_vertrag_dag` in the Cloud Composer Airflow UI to prevent further execution.
2.  **Re-enable Legacy Scheduling:**
    *   Re-enable the original UC4 job scheduling for `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` to ensure business continuity.
3.  **Data Reversion (if necessary):**
    *   If the BigQuery target table (`gcp_project.dataset.sof_ta_iccid_vertrag`) was corrupted or incorrectly updated by the migrated job, and the legacy system relies on its state, consider reverting the table.
        *   **Option A (BigQuery Time Travel):** If the data was only recently updated, BigQuery's time travel feature can be used to restore the table to a state before the problematic run.
        *   **Option B (Backup Restore):** If a backup of the BigQuery table exists, restore it to the last known good state.
        *   **Note:** If the legacy system continues to run in parallel during the cutover period, data reversion might not be strictly necessary for the BigQuery target, as the legacy system would continue to populate its own target.
4.  **Monitor Legacy System:**
    *   Closely monitor the legacy UC4 job to ensure it executes successfully and produces correct output after the rollback.
5.  **Root Cause Analysis:**
    *   Conduct a thorough root cause analysis of the issue that necessitated the rollback. Address the identified problems in the Airflow DAG, BigQuery SQL, or data ingestion processes before attempting another go-live.