```markdown
# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `r_ausd_bp_ta_bcp_msisdn.ksh` job, known as "Bereitstellung Basisprodukte BERT". This job is responsible for the initial provisioning and ongoing extraction of selected base product contract cache data from the Data Warehouse (DWH) for "Forderungsscoring" (demand scoring).

The original job, composed of KornShell scripts (`r_ausd_bp_ta_bcp_msisdn.ksh`, `k_ausd_bp_ta_bcp_msisdn.ksh`) and an Oracle SQL script (`d_ausd_bp_ta_bcp_msisdn.sql`), executed on an Oracle database environment.

The job has been migrated to Google Cloud Platform (GCP), leveraging:
*   **Google Cloud Composer (Airflow)** for orchestration.
*   **Google BigQuery** for data storage and transformation.

## 2. Generated Artifacts

The migration produced the following primary artifact:

*   **`dags/bert_bp_ta_bcp_msisdn_dag.py`**
    *   **Role**: This Python file defines an Airflow DAG that orchestrates the entire data pipeline. It replaces the functionality of the original KornShell orchestrator and executor scripts. It handles parameter parsing (`stichtag`, `wiederanlaufwert`), date logic, retrieves metadata from BigQuery, truncates the target table, and executes the core data transformation logic using BigQuery operators.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Orchestration from KornShell to Airflow**: The original KornShell scripts (`r_ausd_bp_ta_bcp_msisdn.ksh`, `k_ausd_bp_ta_bcp_msisdn.ksh`) were replaced by a Python-based Airflow DAG.
    *   **Rationale**: Airflow provides a robust, scalable, and observable platform for ETL orchestration, offering advanced scheduling, monitoring, logging, dependency management, and retry mechanisms that are superior to custom shell scripting. This aligns with GCP's recommended practices for data pipeline management.
*   **Data Storage and Transformation from Oracle to BigQuery**: All source and target Oracle tables were migrated to BigQuery datasets and tables, and the core SQL transformation logic was translated to BigQuery Standard SQL.
    *   **Rationale**: BigQuery is a fully managed, serverless, highly scalable, and cost-effective data warehouse solution. It simplifies infrastructure management, provides high performance for analytical queries, and its Standard SQL dialect facilitates direct translation of Oracle SQL.
*   **Replacement of Helper Scripts**: Custom KornShell helper scripts (e.g., for date handling, error logging, parameter parsing) were replaced by native Python functions and Airflow's built-in capabilities.
    *   **Rationale**: This reduces dependencies on legacy shell scripts, leverages Airflow's ecosystem, and promotes a more unified Python-based development environment within the DAG.
*   **`TRUNCATE TABLE` Implementation**: The Oracle stored procedure call (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`) used for truncating the target table was replaced by a direct `TRUNCATE TABLE` DDL statement executed via `BigQueryExecuteQueryOperator`.
    *   **Rationale**: BigQuery directly supports `TRUNCATE TABLE`, simplifying the DDL operation and removing the need for a custom wrapper.
*   **Core Transformation with `BigQueryInsertJobOperator`**: The main `INSERT INTO ... SELECT ...` statement was implemented using `BigQueryInsertJobOperator`.
    *   **Rationale**: This operator is well-suited for executing DML statements. The `WRITE_APPEND` disposition, combined with a preceding `TRUNCATE` task, effectively achieves the desired overwrite behavior for the target table.
*   **Parameter Handling via Airflow DAG Parameters**: The `stichtag` and `wiederanlaufwert` inputs are now defined as Airflow DAG parameters.
    *   **Rationale**: This allows for flexible execution and re-runs with different input values directly from the Airflow UI or CLI, mirroring the original script's dynamic input capabilities.
*   **Logging and Error Handling**: Custom `DWMSG_*` functions were replaced by Airflow's native logging mechanisms.
    *   **Rationale**: Airflow provides centralized logging, integrates seamlessly with GCP Cloud Logging, and offers better visibility into job execution status and errors.

**Notable Trade-offs**:
*   **Loss of Original Execution Environment**: The direct shell script execution environment and its specific utilities are no longer present, requiring careful re-implementation of all implicit behaviors in Python.
*   **SQL Dialect Translation Nuances**: While BigQuery Standard SQL is powerful, subtle differences from Oracle SQL (e.g., date functions, `NVL` vs `COALESCE`) necessitated careful translation and thorough testing.
*   **Inferred Helper Script Logic**: The exact behavior of some original helper scripts was inferred due to lack of direct access to their code. This introduces a minor risk of subtle behavioral discrepancies if not fully validated.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated DAG in a production environment, the following manual steps must be completed:

1.  **GCP Project ID Configuration**:
    *   Update the `PROJECT_ID` placeholder in `dags/bert_bp_ta_bcp_msisdn_dag.py` from `"gcp-project-id"` to your actual GCP Project ID.
2.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset `isbert_schema` exists within your GCP project.
    *   Ensure the BigQuery dataset `sof_schema` exists within your GCP project.
3.  **BigQuery Source Table Migration**:
    *   Verify that the following source tables have been successfully migrated from Oracle to BigQuery and contain the necessary data:
        *   `project_id.isbert_schema.dwtk_meldungen_bq` (migrated from `isbert_schema.dwtk_meldungen`)
        *   `project_id.sof_schema.ta_bpr_bcp_bq` (migrated from `sof$ta_bpr_bcp`)
        *   `project_id.sof_schema.ta_rn_vertrag_bq` (migrated from `sof$ta_rn_vertrag`)
4.  **BigQuery Target Table Creation**:
    *   Create the empty target table `project_id.sof_schema.ta_bcp_msisdn_bq` in BigQuery with the correct schema. The expected columns are `CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, and `TN_TEL_MSISDN`, with appropriate data types (e.g., `STRING` or `INTEGER` as per source definition).
5.  **IAM Permissions Configuration**:
    *   The Airflow Service Account (or the user running the DAG) must be granted the following IAM roles on the GCP project or specific BigQuery resources:
        *   `BigQuery Data Editor` on the `isbert_schema` and `sof_schema` datasets to read from source tables and write to the target table.
        *   `BigQuery Job User` to allow the service account to run BigQuery jobs.
6.  **Airflow Connection Setup**:
    *   Ensure a `google_cloud_default` connection is properly configured in your Airflow environment, pointing to the correct GCP project.
7.  **DAG Scheduling**:
    *   Update the `schedule` parameter in the DAG definition within `dags/bert_bp_ta_bcp_msisdn_dag.py` from `None` to the desired production schedule (e.g., `"@daily"`, a specific cron expression).
8.  **Secrets Management (if applicable)**:
    *   If the original job relied on any external secrets (e.g., database credentials for other systems), ensure these are securely managed in GCP (e.g., Secret Manager) and accessed appropriately by the Airflow DAG. No explicit secrets were identified in the provided code for this specific job.

## 5. Known Gaps & Unresolved References

The following items were identified as potential gaps or unresolved references during the migration and require further investigation or follow-up:

*   **Original Helper Scripts (`k_ausd_bp_ta_bcp_msisdn.ksh` and sourced utilities)**: The full functionality of the original helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) was not fully available for detailed analysis. While common functionalities (parameter parsing, date handling) have been re-implemented in Python, there is a risk of missing subtle logic or side effects.
    *   **Follow-up**: A thorough review of the original helper script code is recommended to ensure all critical functionalities are accurately replicated or confirmed as unnecessary for the migrated solution.
*   **`v_carmen = "@pcrs1"` Reference**: The exact purpose and target of the Oracle database link/service name `v_carmen = "@pcrs1"` remain unclear. If this reference points to an external data source not yet migrated, this source needs to be identified, and its migration strategy or integration with BigQuery must be defined.
    *   **Follow-up**: Investigate the `v_carmen` reference in the original Oracle environment. Determine if it represents an active dependency and, if so, identify the source system and plan its migration or integration with BigQuery.
*   **Commented-out `sed`, `sort`, `join` Operations**: The original `k_ausd_bp_ta_bcp_msisdn.ksh` contained commented-out sections for file-based post-processing. These were ignored during migration based on the assumption that they are inactive or obsolete.
    *   **Follow-up**: Confirm with business stakeholders or source system owners that these steps are indeed obsolete and not required for the migrated solution.
*   **`FOSJobErzeugeEintrag` / `FOSJobDeaktivate` Calls**: Commented-out calls in `k_ausd_bp_ta_bcp_msisdn.ksh` suggest interaction with a "FOS Job Management" system. This interaction has not been replicated in the Airflow DAG.
    *   **Follow-up**: Identify the "FOS Job Management" system. Determine if these interactions are critical for the job's lifecycle or external system integration. If so, define a strategy for integrating Airflow with this system or replacing its functionality.
*   **Record Count Logging**: The original script likely logged the number of records processed or inserted. The current DAG does not explicitly log the number of rows inserted into the target table.
    *   **Follow-up**: Implement a mechanism to log the number of rows inserted into `sof_schema.ta_bcp_msisdn_bq`. This can be achieved by parsing the BigQuery job results or adding a separate `COUNT(*)` task after the insertion.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to Run Tests**:

1.  **Unit Tests**:
    *   Develop and execute Python unit tests for the `get_stichtag_and_wiederanlaufwert` and `retrieve_s_datum` tasks to verify correct parameter parsing, default date logic, and metadata retrieval.
2.  **BigQuery SQL Validation**:
    *   Manually execute the BigQuery SQL queries (for `retrieve_s_datum`, `TRUNCATE TABLE`, and the main `INSERT INTO SELECT`) in the BigQuery console using sample data. This verifies the correctness and performance of the SQL logic independently of Airflow.
3.  **Airflow DAG Execution**:
    *   **Deployment**: Upload the `dags/bert_bp_ta_bcp_msisdn_dag.py` file to your Airflow DAGs folder.
    *   **Activation**: Unpause the DAG in the Airflow UI.
    *   **Triggering**: Trigger the DAG manually from the Airflow UI.
        *   Perform a run without providing `stichtag` to test the default "yesterday" logic.
        *   Perform runs with specific `stichtag` values (e.g., current date, historical date).
        *   Perform runs with and without `wiederanlaufwert` to test parameter passing.
    *   **Monitoring**: Monitor the DAG run in the Airflow UI for task success/failure and review task logs for any errors or unexpected behavior.

**What "Passing" Means**:

1.  **DAG Success**: The Airflow DAG completes successfully without any task failures.
2.  **Data Integrity**:
    *   The `sof_schema.ta_bcp_msisdn_bq` table is truncated at the beginning of each successful run.
    *   The data in `sof_schema.ta_bcp_msisdn_bq` precisely matches the expected output based on the transformation logic applied to the source tables (`sof_schema.ta_bpr_bcp_bq`, `sof_schema.ta_rn_vertrag_bq`).
    *   **Comparison**: Perform a row count comparison between the original Oracle target table (`sof$ta_bcp_msisdn`) and the BigQuery target table (`sof_schema.ta_bcp_msisdn_bq`) after a full run with identical source data.
    *   **Sampling**: Conduct data sampling and comparison of key columns (e.g., `CNTRCT_ID`, `TN_TEL_MSISDN`) to ensure data types, values, and distinctness are preserved.
3.  **Parameter Handling**: The `stichtag` and `wiederanlaufwert` parameters are correctly parsed and utilized by the DAG, including the default `stichtag` behavior.
4.  **Metadata Retrieval**: The `s_datum` value retrieved from `isbert_schema.dwtk_meldungen_bq` matches the expected value from the original Oracle `isbert_schema.dwtk_meldungen` table.
5.  **Performance**: The DAG completes within acceptable timeframes, ideally comparable to or better than the original job's execution time.
6.  **Logging**: All relevant execution steps, warnings, and errors are logged appropriately in Airflow and are visible in GCP Cloud Logging.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Deactivate Airflow DAG**:
    *   Immediately pause or delete the `bert_bp_ta_bcp_msisdn_dag` in the Airflow UI to prevent any further executions of the migrated job.
2.  **Re-enable Original Scheduling**:
    *   Re-enable the original scheduling mechanism for the `r_ausd_bp_ta_bcp_msisdn.ksh` job (e.g., cron job, enterprise scheduler entry) to resume its operation in the Oracle environment.
3.  **Data Restoration (if necessary)**:
    *   If the BigQuery target table (`sof_schema.ta_bcp_msisdn_bq`) was corrupted or incorrectly populated by the migrated job, it can be restored using BigQuery's time travel feature to revert to a previous state.
    *   For the original Oracle target table (`sof$ta_bcp_msisdn`), if it was affected (which is unlikely as the migrated job writes to BigQuery), restore it from the most recent backup or re-run the original `r_ausd_bp_ta_bcp_msisdn.ksh` job to repopulate it.
4.  **Monitor Original Job**:
    *   Verify that the original `r_ausd_bp_ta_bcp_msisdn.ksh` job is running as expected in the Oracle environment and producing correct output.
5.  **Investigation**:
    *   Analyze the root cause of the issues that necessitated the rollback using Airflow logs, BigQuery job history, and other monitoring tools. Address the identified problems before attempting re-migration or re-deployment.
```