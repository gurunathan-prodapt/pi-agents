# MIGRATION_NOTES.md

## 1. Summary

The `DW.BERT_AUSD_BP_TA_APN_VERTRAG` job, responsible for preparing instantiated base products (APN and contract reference data) for the BERT process, has been migrated. This job previously involved extracting and aggregating contract cache data from a legacy DWH, handling date parameters, and orchestrating a core SQL script.

The migration re-platforms the existing UC4-orchestrated KornShell and Oracle PL/SQL solution to Google Cloud Platform (GCP).
*   **Original Platform**: UC4 (orchestration), KornShell (scripting), Oracle Database (PL/SQL and data storage).
*   **Target Platform**:
    *   **Orchestration**: Airflow (managed by Cloud Composer).
    *   **Data Processing**: BigQuery (using BigQuery Stored Procedures).
    *   **Data Storage**: BigQuery tables.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`sql/ddl/create_tables.sql`**
    *   **Role**: Contains Data Definition Language (DDL) statements for creating the necessary BigQuery datasets (`project.isbert_schema`, `project.sof`) and tables (`project.isbert_schema.dwtk_meldungen`, `project.sof.ta_bpr_apn`, `project.sof.ta_apn_vertrag`) that serve as source and target for the migrated job. These tables mirror the schema of their Oracle counterparts.

*   **`sql/sp/sp_d_ausd_bp_ta_apn_vertrag.sql`**
    *   **Role**: A BigQuery Stored Procedure that encapsulates the core data processing logic. This procedure is a direct translation of the original `d_ausd_bp_ta_apn_vertrag.sql` Oracle PL/SQL script, converting procedural loops for string aggregation into an efficient `INSERT INTO ... SELECT` statement utilizing BigQuery's `STRING_AGG` function.

*   **`sql/sp/sp_r_k_ausd_bp_ta_apn_vertrag.sql`**
    *   **Role**: A BigQuery Stored Procedure that combines the orchestration logic previously handled by the `r_ausd_bp_ta_apn_vertrag.ksh` and `k_ausd_bp_ta_apn_vertrag.ksh` KornShell scripts. It manages parameter handling, validation, date calculations, and calls the core data processing procedure (`sp_d_ausd_bp_ta_apn_vertrag`). It also includes placeholders for job logging and error handling.

*   **`dags/dw_bert_ausd_bp_ta_apn_vertrag.py`**
    *   **Role**: An Airflow Directed Acyclic Graph (DAG) written in Python. This DAG is responsible for scheduling and orchestrating the execution of the `sp_r_k_ausd_bp_ta_apn_vertrag` BigQuery Stored Procedure. It defines the job's schedule, default arguments, and uses a `BigQueryExecuteQueryOperator` to invoke the main orchestration procedure.

## 3. Key Design Decisions

*   **Airflow for Orchestration**: UC4 was replaced by Airflow (Cloud Composer) to leverage GCP's managed orchestration service, providing scalability, reliability, and native integration with other GCP services.
*   **BigQuery for All Data Processing**: The entire data processing pipeline, including the logic from both KornShell scripts and the Oracle PL/SQL, was consolidated into BigQuery Stored Procedures. This decision centralizes data transformation within the data warehouse, minimizing data movement and leveraging BigQuery's performance for large-scale data operations.
*   **Consolidation of KornShell Logic into BigQuery SP**: The two KornShell scripts (`r_ausd_bp_ta_apn_vertrag.ksh` and `k_ausd_bp_ta_apn_vertrag.ksh`) were combined into a single BigQuery Stored Procedure (`sp_r_k_ausd_bp_ta_apn_vertrag`). This simplifies the Airflow DAG, which now only needs to call one BigQuery procedure, and keeps all control flow logic within BigQuery's procedural SQL.
*   **`STRING_AGG` for Efficient Aggregation**: The procedural `FOR` loop in the original Oracle PL/SQL script, used for aggregating and concatenating strings, was replaced with BigQuery's `STRING_AGG` function. This significantly improves performance and scalability for string concatenation operations by leveraging BigQuery's vectorized execution capabilities.
*   **BigQuery for Data Storage**: All source and target tables were migrated to BigQuery, providing a scalable, cost-effective, and fully managed data warehousing solution.

**Notable Trade-offs**:
*   **Procedural Logic in BigQuery**: While the core data transformation moved from a procedural Oracle loop to a more declarative `INSERT INTO ... SELECT` with `STRING_AGG`, the orchestration logic (parameter handling, error checks) is re-implemented using BigQuery's procedural language features. This is a trade-off to keep the entire data pipeline logic within BigQuery, minimizing Python code in the DAG to just calling the main SP.
*   **Loss of OS-level Control**: The fine-grained environment setup and external script calls from KornShell are replaced by BigQuery SP logic and Airflow's environment. This reduces flexibility for OS-level operations but increases consistency and cloud-native integration.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project Setup**: Ensure the target GCP project (`project`) is correctly configured and accessible.
2.  **BigQuery Dataset and Table Creation**:
    *   Execute the `sql/ddl/create_tables.sql` script in BigQuery to create the `project.isbert_schema` and `project.sof` datasets, along with the `dwtk_meldungen`, `ta_bpr_apn`, and `ta_apn_vertrag` tables within them.
3.  **Initial Data Loading**:
    *   Perform a one-time historical data load from the legacy Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_apn`, `sof$ta_apn_vertrag`) into their respective BigQuery counterparts. This is critical for initial validation and ensuring data continuity.
4.  **IAM/Permissions Configuration**:
    *   The Cloud Composer service account (which runs the Airflow DAG) must be granted appropriate IAM roles, such as `BigQuery Data Editor` or a custom role with permissions to execute BigQuery jobs (`bigquery.jobs.create`) and read/write data to the relevant datasets (`bigquery.dataEditor` on `project.isbert_schema` and `project.sof`).
5.  **BigQuery Stored Procedure Deployment**:
    *   Execute the `sql/sp/sp_d_ausd_bp_ta_apn_vertrag.sql` and `sql/sp/sp_r_k_ausd_bp_ta_apn_vertrag.sql` scripts in BigQuery to create or replace the stored procedures.
6.  **Airflow DAG Deployment**:
    *   Deploy the `dags/dw_bert_ausd_bp_ta_apn_vertrag.py` file to the Cloud Composer environment's DAGs folder.
    *   Verify that the `schedule_interval` in the DAG is set correctly for the production schedule.
    *   Adjust the `location` parameter in the `BigQueryExecuteQueryOperator` if your BigQuery dataset is not in the `US` multi-region.
7.  **Logging and Monitoring Setup**:
    *   Configure Cloud Logging sinks to capture BigQuery job logs and Airflow task logs.
    *   Set up Cloud Monitoring alerts for Airflow DAG failures and BigQuery job errors to ensure timely notification of issues.
8.  **Metadata/Job Tracking Tables (B4 Item)**:
    *   The `sp_r_k_ausd_bp_ta_apn_vertrag.sql` procedure contains commented-out `INSERT` and `UPDATE` statements for `project.isbert_schema.job_log` and `project.isbert_schema.error_log`. If detailed job tracking and error logging within BigQuery are required, these tables must be created (DDL not provided, needs to be defined) and the relevant lines in the stored procedure uncommented.

## 5. Known Gaps & Unresolved References

The following items have been flagged for follow-up or represent areas of potential risk:

*   **Parameter `p_wiederanlaufWert`**: The exact usage and implications of this parameter in the original KornShell scripts for restart/recovery logic are not fully clear. The current BigQuery SP defaults it to `0`. A thorough understanding and proper implementation of restart logic are needed if this parameter is critical for job recovery.
*   **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`**: The precise functionality of this Oracle utility call (beyond a simple `TRUNCATE TABLE`) needs to be confirmed. If it performs additional critical logic or side effects, these must be replicated in BigQuery.
*   **Unresolved Lineage References**: Several `UNRESOLVED` nodes were identified in the original lineage analysis (e.g., `CALENDAR:DW.KALENDER`, `COMMAND:SQLPLUS`, various environment variables and file paths). While many are implicitly handled by the migration to BigQuery/Airflow, a comprehensive review is recommended to ensure no critical dependencies are missed.
*   **Error Logging (`DWMSG_MeldeFehler`)**: The full functionality of `f_alis_msgerr.ksh` and its interaction with `DWMSG_MeldeFehler` for error reporting and alerting needs to be understood. The current BigQuery SP uses `SIGNAL SQLSTATE '45000'` for error propagation, which Airflow can catch, but specific alerting mechanisms (e.g., email, PagerDuty) need to be configured in GCP to match the legacy system's capabilities.
*   **Performance of `STRING_AGG`**: While `STRING_AGG` is generally efficient, its performance on extremely large datasets should be monitored during initial runs and optimized if necessary (e.g., by pre-filtering data or adjusting BigQuery slot allocation).
*   **Dynamic `p_eintragsnr` (B4 Item)**: The Airflow DAG currently uses a static placeholder `p_eintragsnr_value = 123456789`. In a production environment, this should be replaced with a dynamic, unique identifier for each job run (e.g., a timestamp, a sequence from a metadata table, or Airflow's `run_id`) to ensure proper job tracking and auditing.
*   **Job Logging Tables (B4 Item)**: The `project.isbert_schema.job_log` and `project.isbert_schema.error_log` tables are referenced in the BigQuery orchestration SP but are commented out. Their DDL needs to be defined and the `INSERT`/`UPDATE` statements uncommented if detailed job tracking and error logging within BigQuery are required.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results in the new environment.

**How to Run Tests**:

1.  **BigQuery Stored Procedure Execution (Unit Testing)**:
    *   Ensure the BigQuery datasets and tables are created (`sql/ddl/create_tables.sql`).
    *   Load a representative set of sample data into `project.isbert_schema.dwtk_meldungen` and `project.sof.ta_bpr_apn`.
    *   Execute `CALL project.sof.sp_d_ausd_bp_ta_apn_vertrag();` directly in the BigQuery console to test the core data transformation logic.
    *   Execute `CALL project.sof.sp_r_k_ausd_bp_ta_apn_vertrag('TEST_JOB', 1, CURRENT_DATE(), 0);` with various parameter combinations (including edge cases for validation) to test the orchestration, parameter handling, and error propagation.
2.  **Airflow DAG Execution (Integration Testing)**:
    *   Deploy the `dags/dw_bert_ausd_bp_ta_apn_vertrag.py` DAG to the Cloud Composer environment.
    *   Trigger the DAG manually from the Airflow UI or wait for its scheduled run.
    *   Monitor the DAG's progress in the Airflow UI, checking task logs for any errors or warnings.
    *   Review BigQuery job history for the executed stored procedures.

**What "Passing" Means**:

*   **Successful Execution**: The Airflow DAG completes successfully without any task failures. All BigQuery jobs initiated by the DAG complete with a `SUCCESS` status.
*   **Data Integrity**:
    *   The target table `project.sof.ta_apn_vertrag` is populated with data.
    *   The data in `project.sof.ta_apn_vertrag` exactly matches the expected output from the legacy Oracle job when run with the *same input data*. This includes:
        *   Correct `cntrct_id` values.
        *   Accurate aggregation and concatenation of `apn_list` and `cntrct_ref_list` values, respecting the `SUBSTR` length limits.
        *   No unexpected data loss, duplication, or transformation errors.
*   **Performance**: The job completes within acceptable timeframes, ideally comparable to or better than the legacy system's execution duration.
*   **Logging & Alerting**: Job start/end, warnings, and errors are correctly logged in Cloud Logging. Configured alerts (e.g., for DAG failures or BigQuery job errors) are triggered appropriately when failures occur and remain silent during successful runs.
*   **Idempotency**: Running the job multiple times with the same input parameters should consistently produce the same output in `project.sof.ta_apn_vertrag` (due to the `TRUNCATE TABLE` operation).

## 7. Rollback Procedure

In the event of critical failure or incorrect data generation by the migrated job in production, the following rollback procedure should be followed:

1.  **Immediate Action**:
    *   **Disable Airflow DAG**: Pause or delete the `dw_bert_ausd_bp_ta_apn_vertrag` DAG in the Cloud Composer environment to prevent any further execution of the migrated job.
2.  **Revert to Legacy System**:
    *   **Re-enable UC4 Job**: Re-enable and, if necessary, manually trigger the original UC4 job `DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml` in the legacy environment to ensure business continuity.
3.  **Data Cleanup (if necessary)**:
    *   If the migrated job wrote incorrect or incomplete data to `project.sof.ta_apn_vertrag`, truncate or delete the affected data in BigQuery to prevent data corruption.
4.  **Root Cause Analysis**:
    *   Thoroughly investigate the failure in the GCP environment using Airflow logs, BigQuery job history, and Cloud Logging to identify the root cause (e.g., bug in BigQuery SP, incorrect parameters, IAM issue).
5.  **Fix and Re-validate**:
    *   Apply necessary fixes to the BigQuery Stored Procedures or the Airflow DAG.
    *   Re-deploy the updated artifacts to a staging/development environment.
    *   Perform a full re-validation cycle as described in Section 6.
6.  **Re-deploy and Go-Live**:
    *   Once fixes are confirmed and validation passes, re-deploy the updated DAG to production and re-enable it.