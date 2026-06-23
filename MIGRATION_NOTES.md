# MIGRATION_NOTES.md: DW.BERT_AUSD_V_TA_P_VERTRAG

## 1. Summary

The `DW.BERT_AUSD_V_TA_P_VERTRAG` job, originally orchestrated by UC4 and executing KornShell scripts that in turn ran an Oracle SQL*Plus script, has been migrated to Google Cloud Platform (GCP). The new architecture leverages Apache Airflow for workflow orchestration and Google BigQuery for data warehousing and SQL transformations. The primary function of processing and synchronizing contract-related data, specifically "twin-bill" contracts, and populating the `sof$ta_p_vertrag` table, remains the same.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`sql/ddl/sof_ta_p_vertrag.sql`**:
    *   **Role**: BigQuery Data Definition Language (DDL) script to create the target table `sof_ta_p_vertrag`. This table replaces the legacy Oracle `sof$ta_p_vertrag` table and will store the final processed contract data.
*   **`sql/ddl/sof_ta_vertrag_tmp.sql`**:
    *   **Role**: BigQuery DDL script to create the staging table `sof_ta_vertrag_tmp`. This table replaces the legacy Oracle `sof$ta_vertrag_tmp` and serves as an intermediate store for contract data before the main transformation.
*   **`sql/ddl/isbert_schema_dwtk_meldungen.sql`**:
    *   **Role**: BigQuery DDL script to create the `dwtk_meldungen` table within the `isbert_schema` dataset. This table replaces the legacy Oracle `isbert_schema.dwtk_meldungen` and is used to determine the processing date.
*   **`sql/ddl/sof_ta_disc_zusgf.sql`**, **`sql/ddl/sof_ta_discount.sql`**, ..., **`sql/ddl/sof_ta_action_assoc.sql`** (and other `sof_ta_` DDLs):
    *   **Role**: BigQuery DDL scripts for various temporary tables that are truncated as part of the job's cleanup phase. These replace their Oracle counterparts. Note that these DDLs currently use placeholder columns as their full schemas were not provided in the design document.
*   **`sql/d_ausd_v_ta_p_vertrag_bq.sql`**:
    *   **Role**: BigQuery SQL script containing the core data transformation logic. This script translates the original Oracle SQL*Plus script (`d_ausd_v_ta_p_vertrag.sql`), including the `DECLARE` statement for `v_datum`, the `TRUNCATE` of the target table, the main `INSERT INTO SELECT` statement, and the subsequent `TRUNCATE` operations for temporary tables.
*   **`dags/dw_bert_ausd_v_ta_p_vertrag_dag.py`**:
    *   **Role**: Apache Airflow DAG (Directed Acyclic Graph) definition file. This Python script orchestrates the entire job, replacing the UC4 job and KornShell wrappers. It defines the sequence of tasks, including table creation (for initial setup), data ingestion placeholders, the main BigQuery transformation, and temporary table cleanup.

## 3. Key Design Decisions

*   **Orchestration Shift to Airflow**: The legacy UC4 job and KornShell wrapper scripts (`r_ausd_v_ta_p_vertrag.ksh`, `k_ausd_v_ta_p_vertrag.ksh`) are replaced by a single Airflow DAG (`dw_bert_ausd_v_ta_p_vertrag_dag.py`). This centralizes scheduling, monitoring, and error handling within a modern, cloud-native environment.
*   **Data Processing with BigQuery**: The core Oracle SQL*Plus logic (`d_ausd_v_ta_p_vertrag.sql`) is translated into BigQuery SQL (`d_ausd_v_ta_p_vertrag_bq.sql`). BigQuery's serverless architecture and automatic parallelism eliminate the need for Oracle-specific hints (`/*+ parallel */`) and provide scalable, high-performance data processing.
*   **Direct SQL Translation**: The `INSERT INTO SELECT` statement, including the `LEFT JOIN` (originally indicated by Oracle's `(+)` syntax), and `TRUNCATE TABLE` commands are directly translated to their BigQuery equivalents. This minimizes logical changes and preserves the original transformation intent.
*   **Variable Handling**: The Oracle `v_datum` variable, derived from `isbert_schema.dwtk_meldungen`, is translated to a BigQuery `DECLARE` and `SET` statement within the main SQL script, ensuring the processing date logic is maintained.
*   **External Dependency Ingestion**: Data from the CARMEN DB (referenced via DB-Link `@pcrs1`) and `isbert_schema.dwtk_meldungen` is assumed to be ingested into BigQuery staging tables (`sof_ta_vertrag_tmp`, `isbert_schema.dwtk_meldungen`) by an upstream process. This decouples ingestion from the transformation DAG, promoting modularity.
*   **Airflow Operators for Tasks**: `BigQueryExecuteQueryOperator` is used for all BigQuery SQL operations (DDL, DML, DCL), providing native integration and robust error handling. `DummyOperator` is used as a placeholder for upstream data ingestion, indicating a dependency without performing an action itself.
*   **Placeholder DDLs for Temporary Tables**: For `sof_ta_` temporary tables where the full schema was not provided, placeholder DDLs were generated. This highlights the need to define the actual schemas during implementation.
*   **Removal of Oracle-Specific Client Features**: SQL*Plus client-side commands (e.g., `WHENEVER SQLERROR`, `SET TIMING ON`) are not directly translated as Airflow's native logging, monitoring, and error handling mechanisms provide equivalent or superior functionality.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps and configurations are required:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery datasets `project_id.dataset_id` and `project_id.isbert_schema` exist. Replace `project_id` and `dataset_id` with your actual GCP project and dataset identifiers.
2.  **BigQuery Table Schema Definition**:
    *   **Critical**: Review and update the generated DDLs for all `sof_ta_` temporary tables (e.g., `sof_ta_disc_zusgf`, `sof_ta_discount`, etc.) to include their actual column definitions (names, data types, nullability) based on the source Oracle schemas. The generated DDLs currently use `placeholder_col STRING`.
    *   Execute all BigQuery DDL scripts (`sql/ddl/*.sql`) to create the necessary tables and schemas in BigQuery. These DDLs are also included in the Airflow DAG for idempotency, but initial creation is recommended.
3.  **IAM Permissions**:
    *   Grant the Airflow service account (used by Cloud Composer) the necessary BigQuery roles:
        *   `BigQuery Data Editor` on `project_id.dataset_id` and `project_id.isbert_schema` to allow `TRUNCATE`, `INSERT`, and `CREATE TABLE` operations.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   Potentially `BigQuery Data Viewer` if any read-only access to other datasets is required.
    *   Ensure the service account used by the upstream data ingestion pipeline (e.g., Data Fusion, DMS) has appropriate permissions to read from the source Oracle database and write to the BigQuery staging tables (`sof_ta_vertrag_tmp`, `isbert_schema.dwtk_meldungen`).
4.  **Data Ingestion Pipeline Setup**:
    *   **Crucial**: Implement and configure the data ingestion pipelines responsible for populating `project_id.dataset_id.sof_ta_vertrag_tmp` and `project_id.isbert_schema.dwtk_meldungen` from the source Oracle CARMEN DB. This is an upstream dependency and must be operational before the Airflow DAG can run successfully.
    *   Ensure these pipelines run on a schedule that guarantees data readiness before `dw_bert_ausd_v_ta_p_vertrag` DAG execution.
5.  **Airflow DAG Deployment**:
    *   Deploy the `dags/dw_bert_ausd_v_ta_p_vertrag_dag.py` file to your Cloud Composer environment's DAGs folder.
6.  **Airflow DAG Configuration**:
    *   Update the `PROJECT_ID` and `DATASET_ID` variables within `dw_bert_ausd_v_ta_p_vertrag_dag.py` to match your GCP environment.
    *   Define the appropriate `schedule` for the DAG in `dw_bert_ausd_v_ta_p_vertrag_dag.py` (e.g., daily, hourly, etc.) to match the original UC4 schedule.
7.  **Secrets Management**:
    *   If the data ingestion pipeline requires direct database credentials for Oracle, ensure these are securely stored in Secret Manager and accessed appropriately by the ingestion tools.

## 5. Known Gaps & Unresolved References

The following items have been identified as known gaps or require further follow-up:

*   **Complexity of `sof$ta_vertrag_tmp` Population**: The exact process by which the `sof$ta_vertrag_tmp` table is populated in the legacy Oracle environment is not fully detailed. A thorough understanding of this upstream process is critical to ensure the BigQuery `sof_ta_vertrag_tmp` staging table is populated with identical data and timing.
*   **KornShell Utility Scripts**: The content and full functionality of the various KornShell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) were not provided. While Airflow's native features cover general orchestration, specific environmental setups, parameter handling, or custom logging/error reporting from these scripts might need explicit replication or careful consideration.
*   **Full Schemas for Temporary Tables (B4 Item)**: The DDLs for the numerous `sof_ta_` temporary tables (e.g., `sof_ta_disc_zusgf`, `sof_ta_discount`) currently use placeholder columns. The complete and accurate schemas for these tables must be identified and implemented in BigQuery. This is flagged as a "Build Phase 4 (B4)" item, meaning it requires further investigation and design.
*   **Performance Tuning**: While BigQuery handles parallelism automatically, the original Oracle script used `/*+ parallel */` hints. Post-migration, performance of the BigQuery SQL should be thoroughly tested with production-scale data to ensure it meets or exceeds performance expectations.
*   **Error Handling and Restartability**: The legacy KornShell scripts likely had specific error handling and restartability logic. While Airflow provides robust retry mechanisms and `on_failure_callback` options, a detailed comparison and mapping of the original error handling to Airflow's capabilities is recommended to ensure equivalent resilience.
*   **Missing `file_complexity` Data**: The absence of complexity metrics for the source files means that potential hidden complexities or unique migration challenges might not have been fully identified during the design phase.

## 6. Validation

Validation of the migrated job involves several stages:

1.  **Unit Testing (BigQuery SQL)**:
    *   **How to run**: Execute the `sql/d_ausd_v_ta_p_vertrag_bq.sql` script directly in BigQuery using a representative sample of data in the staging tables (`sof_ta_vertrag_tmp`, `isbert_schema.dwtk_meldungen`).
    *   **Passing criteria**:
        *   The script completes without syntax errors.
        *   The `sof_ta_p_vertrag` table is populated.
        *   The `v_datum` variable is correctly determined.
        *   Row counts in `sof_ta_p_vertrag` match those from the source Oracle system for the same input data.
        *   A sample of transformed data in `sof_ta_p_vertrag` matches expected output based on the original Oracle logic.
        *   All temporary tables listed for truncation are indeed truncated.

2.  **Integration Testing (Airflow DAG)**:
    *   **How to run**: Trigger the `dw_bert_ausd_v_ta_p_vertrag` DAG manually in the Cloud Composer UI. Ensure upstream data ingestion pipelines have successfully populated the staging tables.
    *   **Passing criteria**:
        *   The Airflow DAG completes successfully with all tasks marked as "success".
        *   No task failures or retries occur (unless explicitly configured and expected for transient issues).
        *   Logs for each `BigQueryExecuteQueryOperator` task show successful BigQuery job completion.
        *   The `sof_ta_p_vertrag` table in BigQuery contains the expected data.

3.  **Data Validation**:
    *   **How to run**: After a successful DAG run, compare the data in the BigQuery `sof_ta_p_vertrag` table with the corresponding data in the source Oracle `sof$ta_p_vertrag` table.
        *   **Row Count Comparison**: Verify that the total number of rows in the target table matches the source.
        *   **Checksum/Hash Comparison**: For critical columns or entire rows, calculate checksums/hashes in both systems and compare them.
        *   **Data Sample Comparison**: Select random samples of data and manually verify column values, data types, and NULL handling.
        *   **Key Metric Comparison**: Compare aggregate metrics (e.g., SUM, AVG, COUNT DISTINCT) for key columns.
    *   **Passing criteria**:
        *   All data validation checks (row counts, checksums, samples, key metrics) show an exact match between source and target.
        *   Data types are correctly mapped and preserved.
        *   The `LEFT JOIN` logic (Oracle `(+)`) correctly handles non-matching records, resulting in `NULL` values where expected.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Disable New Airflow DAG**: Immediately pause or un-schedule the `dw_bert_ausd_v_ta_p_vertrag` DAG in the Cloud Composer UI to prevent further execution.
2.  **Re-enable Legacy UC4 Job**: Re-activate the original `DW.BERT_AUSD_V_TA_P_VERTRAG` job in the UC4/Automic system.
3.  **Data State Assessment (Optional but Recommended)**:
    *   Assess the state of the `sof_ta_p_vertrag` table in BigQuery. Since the job performs a `TRUNCATE` followed by an `INSERT`, the table is completely overwritten each run. If the last run of the Airflow DAG introduced incorrect data, the table would contain only that incorrect data.
    *   If necessary, and if a backup strategy is in place (e.g., BigQuery time travel, table snapshots), restore `sof_ta_p_vertrag` to a known good state from before the problematic Airflow DAG run.
4.  **Investigate and Rectify**: Analyze the root cause of the issue with the migrated job, make necessary corrections to the Airflow DAG or BigQuery SQL, and re-test thoroughly in a non-production environment before attempting another go-live.