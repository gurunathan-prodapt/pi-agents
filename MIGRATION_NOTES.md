# MIGRATION_NOTES.md: DW.BERT_AUSD_BP_TA_ICCID_VERTRAG

## 1. Summary

The ETL job `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG`, originally orchestrated by Automic (UC4) and executing KornShell scripts and Oracle SQL, has been migrated. Its core function is to aggregate ICCID (SIM card ID) data from `SOF$TA_ICCID_EINZELN` and load it into `SOF$TA_ICCID_VERTRAG` for reporting.

The job has been re-platformed to Google Cloud Platform (GCP).
*   **Orchestration:** Apache Airflow on Cloud Composer.
*   **Data Storage & Transformation:** BigQuery.
*   **Logging & Monitoring:** Cloud Logging and Cloud Monitoring (via BigQuery audit tables).

The original shell script logic has been translated into BigQuery Stored Procedures, and the Oracle SQL transformation has been converted to BigQuery SQL.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`bigquery/ddl/audit/job_registry.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_registry` table within the `project.audit_dataset`. This table is used to store high-level metadata for each job execution, including its ID, name, start/end times, status, and parameters.
*   **`bigquery/ddl/audit/job_log.sql`**
    *   **Role:** Defines the DDL for the `job_log` table within the `project.audit_dataset`. This table captures detailed log messages (INFO, WARNING, ERROR) generated during job execution, replacing the custom shell script logging.
*   **`bigquery/ddl/target/sof_ta_iccid_vertrag.sql`**
    *   **Role:** Defines the DDL for the target table `sof_ta_iccid_vertrag` within the `project.target_dataset`. This table is the BigQuery equivalent of the original Oracle `SOF$TA_ICCID_VERTRAG` table, where the aggregated ICCID data is stored.
*   **`bigquery/sql/d_ausd_bp_ta_iccid_vertrag_insert.sql`**
    *   **Role:** Contains the core BigQuery SQL `INSERT` statement that performs the data aggregation and pivoting logic. This SQL is derived directly from the original Oracle `d_ausd_bp_ta_iccid_vertrag.sql` and is embedded within the `k_ausd_bp_ta_iccid_vertrag_sp` stored procedure.
*   **`bigquery/stored_procedures/k_ausd_bp_ta_iccid_vertrag_sp.sql`**
    *   **Role:** A BigQuery Stored Procedure that encapsulates the control logic previously found in `k_ausd_bp_ta_iccid_vertrag.ksh`. It handles parameter validation, date calculations, truncates the target table, executes the core SQL transformation (`d_ausd_bp_ta_iccid_vertrag_insert.sql`), and updates the audit tables.
*   **`bigquery/stored_procedures/r_ausd_bp_ta_iccid_vertrag_sp.sql`**
    *   **Role:** A BigQuery Stored Procedure that serves as the top-level orchestrator for the BigQuery transformation, replacing `r_ausd_bp_ta_iccid_vertrag.ksh`. It initializes the job, performs initial parameter validation, logs job start, calls `k_ausd_bp_ta_iccid_vertrag_sp`, and handles overall error reporting to the audit tables.
*   **`dags/dw_bert_ausd_bp_ta_iccid_vertrag.py`**
    *   **Role:** The Apache Airflow DAG definition file. This Python script defines the `dw_bert_ausd_bp_ta_iccid_vertrag` DAG, which contains a single task to trigger the `r_ausd_bp_ta_iccid_vertrag_sp` BigQuery Stored Procedure. It acts as the new top-level orchestrator, replacing the UC4 job.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Shell Script Logic:** The procedural logic from the KornShell scripts (`r_ausd_bp_ta_iccid_vertrag.ksh` and `k_ausd_bp_ta_iccid_vertrag.ksh`) was migrated into BigQuery Stored Procedures (`r_ausd_bp_ta_iccid_vertrag_sp` and `k_ausd_bp_ta_iccid_vertrag_sp`).
    *   **Rationale:** This approach keeps the transformation logic close to the data within BigQuery, leveraging its native scripting capabilities. It simplifies the migration by directly translating procedural steps and parameter handling without introducing an intermediate language (like Python for Dataflow) for the core logic. It also benefits from BigQuery's performance for data-intensive operations.
    *   **Trade-offs:** While efficient for SQL-centric logic, this approach offers less flexibility for complex external system interactions or non-SQL operations compared to a Python-based Dataflow or custom Airflow operator solution.
*   **Airflow on Cloud Composer for Orchestration:** Apache Airflow was chosen to replace the Automic (UC4) scheduler.
    *   **Rationale:** Cloud Composer provides a fully managed Airflow environment, aligning with GCP best practices for ETL orchestration. It offers robust scheduling, monitoring, and dependency management capabilities.
*   **Direct Oracle SQL to BigQuery SQL Translation:** The core data transformation logic from `d_ausd_bp_ta_iccid_vertrag.sql` was directly translated into BigQuery SQL.
    *   **Rationale:** This minimizes redesign effort and ensures functional equivalence. BigQuery's powerful SQL engine can handle complex aggregations and pivoting operations efficiently.
    *   **Trade-offs:** The original SQL was flagged as "Complex" and in the "Retire" bucket. A direct translation might carry over any inefficiencies or outdated logic from the source system, rather than optimizing for BigQuery's unique architecture.
*   **Centralized Audit Logging in BigQuery:** Dedicated `job_registry` and `job_log` tables were created in BigQuery to capture job execution metadata and detailed logs.
    *   **Rationale:** This provides a standardized, queryable, and centralized mechanism for tracking job status, parameters, and errors, replacing disparate shell script logging and custom `DWMSG_*` functions.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the following BigQuery datasets exist in your GCP project:
        *   `project.audit_dataset` (e.g., `your-gcp-project-id.audit_dataset`)
        *   `project.target_dataset` (e.g., `your-gcp-project-id.target_dataset`)
        *   `project.source_dataset` (e.g., `your-gcp-project-id.source_dataset`)
2.  **BigQuery Source Table Population:**
    *   The source table `project.source_dataset.sof_ta_iccid_einzeln` must be present and populated with data, mirroring the structure and content of the original Oracle `SOF$TA_ICCID_EINZELN`.
    *   The reference table `project.source_dataset.dwtk_meldungen` must also be present.
3.  **Deploy BigQuery DDLs:**
    *   Execute the DDL scripts to create the audit and target tables:
        *   `bigquery/ddl/audit/job_registry.sql`
        *   `bigquery/ddl/audit/job_log.sql`
        *   `bigquery/ddl/target/sof_ta_iccid_vertrag.sql`
4.  **Deploy BigQuery Stored Procedures:**
    *   Execute the stored procedure creation scripts in BigQuery:
        *   `bigquery/stored_procedures/k_ausd_bp_ta_iccid_vertrag_sp.sql`
        *   `bigquery/stored_procedures/r_ausd_bp_ta_iccid_vertrag_sp.sql`
5.  **IAM Permissions Configuration:**
    *   The service account associated with your Cloud Composer environment (Airflow worker) must have the following BigQuery permissions:
        *   `BigQuery Data Editor` on `project.audit_dataset` (to write logs and job status).
        *   `BigQuery Data Editor` on `project.target_dataset` (to truncate and insert into `sof_ta_iccid_vertrag`).
        *   `BigQuery Data Viewer` on `project.source_dataset` (to read from `sof_ta_iccid_einzeln` and `dwtk_meldungen`).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
6.  **Airflow Connection Configuration:**
    *   Ensure an Airflow connection named `google_cloud_default` exists and is correctly configured to connect to your GCP project.
7.  **Airflow DAG Deployment:**
    *   Upload the `dags/dw_bert_ausd_bp_ta_iccid_vertrag.py` file to your Cloud Composer environment's DAGs folder.
8.  **Airflow DAG Scheduling:**
    *   The `schedule` parameter in the Airflow DAG is currently set to `None`. This must be updated in the DAG file to reflect the required execution frequency (e.g., `@daily`, `0 0 * * *` for daily at midnight UTC) based on business requirements.
9.  **Parameter Configuration:**
    *   Review and update the default parameters (`p_stichtag`, `p_wiederanlaufWert`) passed to the `r_ausd_bp_ta_iccid_vertrag_sp` in the Airflow DAG (`dags/dw_bert_ausd_bp_ta_iccid_vertrag.py`) to match the desired runtime behavior. Consider using Airflow Variables or XComs for dynamic parameter passing.

## 5. Known Gaps & Unresolved References

The following items were identified during the migration design and require further attention or are known limitations:

*   **UC4 Schedule:** The original UC4 job's schedule was not available. The Airflow DAG's schedule (`schedule=None`) must be manually configured based on business requirements before go-live.
*   **External Utility Scripts:** The exact functionality of the original shell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) was assumed to be standard environment setup, logging, date utilities, and parameter parsing. Any complex or unique logic within these scripts that was not directly translated to BigQuery SQL or Python may require separate analysis and migration (e.g., custom UDFs, Cloud Functions) if their functionality is critical.
*   **`DWMSG_*` Functions:** The custom `DWMSG_*` error and logging functions from the original environment have been replaced by generic audit log table inserts and BigQuery `RAISE` statements. A full review of the original `DWMSG_*` functionality is recommended to ensure no critical features (e.g., specific alerting mechanisms, detailed error codes) are lost.
*   **Oracle `isbert_schema.dwtk_meldungen`:** This table is referenced in the original design for variable definition. Its schema and data content must be available in BigQuery as `project.source_dataset.dwtk_meldungen` for the job to function correctly.
*   **"Retire" Bucket for SQL:** The original Oracle SQL script (`d_ausd_bp_ta_iccid_vertrag.sql`) was flagged as "Retire" in the source inventory. While a direct BigQuery SQL equivalent has been implemented, this flag suggests potential underlying issues (e.g., outdated logic, performance bottlenecks, excessive complexity) that might warrant a functional redesign rather than a direct lift-and-shift. This design assumes a like-for-like translation, but the "retire" status indicates a potential for future optimization or refactoring.
*   **`p_wiederanlaufWert` Logic:** The original restart logic, involving deletion of entries `>= p_wiederanlaufWert` and processing `DWH_VERTRAG_ID > p_wiederanlaufWert`, needs careful validation in the BigQuery implementation to ensure data integrity and atomicity, especially if the `p_wiederanlaufWert` is used for incremental processing rather than full truncates. The current implementation performs a full truncate and reload.
*   **Table Naming:** The use of `SOF$` prefix in Oracle table names is non-standard. While translated to `sof_ta` in BigQuery, any potential conflicts or non-standard characters in other table names across the ecosystem should be reviewed.

## 6. Validation

To ensure the migrated job functions correctly, follow these validation steps:

1.  **Deploy All Components:**
    *   Ensure all BigQuery DDLs (audit tables, target table) and Stored Procedures are deployed to the respective BigQuery datasets.
    *   Ensure the Airflow DAG (`dags/dw_bert_ausd_bp_ta_iccid_vertrag.py`) is deployed to Cloud Composer.
    *   Verify that `project.source_dataset.sof_ta_iccid_einzeln` and `project.source_dataset.dwtk_meldungen` contain representative test data.
2.  **Trigger the Airflow DAG:**
    *   Manually trigger the `dw_bert_ausd_bp_ta_iccid_vertrag` DAG from the Airflow UI.
    *   Provide appropriate test values for `p_stichtag` (e.g., a recent date in `YYYYMMDD` format) and `p_wiederanlaufWert` (e.g., `0` for a full run).
3.  **Monitor Airflow Execution:**
    *   Observe the DAG run in the Airflow UI. The `start_r_ausd_bp_ta_iccid_vertrag_sp` task should transition to a "success" state.
4.  **Check BigQuery Audit Tables:**
    *   Query `project.audit_dataset.job_registry` for the `job_id` corresponding to your test run.
    *   **Passing Criteria:** The `status` column should be `'SUCCEEDED'`, `end_time` should be populated, and `error_message` should be `NULL`.
    *   Query `project.audit_dataset.job_log` using the `job_id`.
    *   **Passing Criteria:** The log should contain `INFO` messages indicating successful steps (e.g., "k_ausd_bp_ta_iccid_vertrag_sp started", "Truncated...", "Successfully inserted X records"). There should be no `ERROR` level messages.
5.  **Validate Target Data:**
    *   Query `project.target_dataset.sof_ta_iccid_vertrag`.
    *   **Passing Criteria (Data Volume):** The table should be populated with data, and the `processed_records` count in `job_registry` should match the `COUNT(*)` from `sof_ta_iccid_vertrag`.
    *   **Passing Criteria (Data Content):** Perform a sample-based comparison of the data in `project.target_dataset.sof_ta_iccid_vertrag` against the expected output from the original Oracle job for the same input parameters. Focus on `CNTRCT_ID` and a few key ICCID-related columns (e.g., `TN_ICCID`, `MS1_ICCID`, `MS1_VALID_TO`).
6.  **Error Handling Test:**
    *   Introduce a controlled error (e.g., temporarily revoke BigQuery write permissions for the service account, or provide an invalid `p_stichtag` format) and re-run the DAG.
    *   **Passing Criteria:** The Airflow task should fail, `job_registry` should show `status = 'FAILED'` with an `error_message`, and `job_log` should contain relevant `ERROR` messages.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Disable New Airflow DAG:**
    *   In the Cloud Composer Airflow UI, set the `dw_bert_ausd_bp_ta_iccid_vertrag` DAG to "Off" (unpause). This will prevent any further executions of the migrated job.
2.  **Re-enable Original UC4 Job:**
    *   Re-enable the original `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` job in the Automic (UC4) scheduler.
3.  **Data Recovery (if necessary):**
    *   If the new BigQuery job has written incorrect or corrupted data to `project.target_dataset.sof_ta_iccid_vertrag`, a data recovery strategy will be required.
        *   **BigQuery Time Travel:** Utilize BigQuery's time travel feature to restore the table to a state before the problematic run (e.g., `SELECT * FROM project.target_dataset.sof_ta_iccid_vertrag FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)`).
        *   **Backup/Snapshot Restore:** If BigQuery snapshots or external backups were taken, restore the table from the most recent valid backup.
        *   **Allow Old Job to Overwrite:** If the original UC4 job performs a full truncate and reload, simply re-enabling it will overwrite any data written by the new job.
4.  **Clean Up BigQuery Artifacts (Optional):**
    *   If the rollback is permanent, the newly created BigQuery Stored Procedures (`r_ausd_bp_ta_iccid_vertrag_sp`, `k_ausd_bp_ta_iccid_vertrag_sp`) can be dropped.
    *   The `project.target_dataset.sof_ta_iccid_vertrag` table can be dropped or retained depending on whether it will be used by other processes.
    *   The audit tables (`job_registry`, `job_log`) can be retained for historical analysis or archived.