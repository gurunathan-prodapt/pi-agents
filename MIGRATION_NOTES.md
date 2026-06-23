# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `r_ausd_v_ta_vvl_upgrade.ksh` job, which orchestrates the reconciliation of contract data for the `ta_vvl_upgrade` table. The original job consisted of a KornShell wrapper (`r_ausd_v_ta_vvl_upgrade.ksh`), a control script (`k_ausd_v_ta_vvl_upgrade.ksh`), and an Oracle SQL script (`d_ausd_v_ta_vvl_upgrade.sql`). Its business purpose is to prepare data for VVL (Vertragsverlängerung - contract extension) upgrades.

The job has been migrated from its legacy KornShell/Oracle environment to Google Cloud Platform (GCP). The target platform leverages:
*   **BigQuery**: For data storage, transformation, and procedural logic. All Oracle tables involved are assumed to be migrated to BigQuery.
*   **Cloud Composer (Apache Airflow)**: For job orchestration, scheduling, and monitoring.

## 2. Generated Artifacts

The migration process generated the following artifacts:

*   **`sql/bq/d_ausd_v_ta_vvl_upgrade_sp.sql`**
    *   **Role**: A BigQuery SQL stored procedure that encapsulates the core data transformation logic previously found in `d_ausd_v_ta_vvl_upgrade.sql`. It handles the determination of `v_datum`, truncates the target table, and inserts transformed data from source tables into `sof_ta_vvl_upgrade`.
*   **`sql/bq/create_sof_ta_vvl_upgrade_table.sql`**
    *   **Role**: A BigQuery DDL script to create the target table `sof_ta_vvl_upgrade`. This table will store the reconciled contract upgrade data.
*   **`sql/bq/create_job_log_table.sql`**
    *   **Role**: A BigQuery DDL script to create a centralized `job_log` table. This table replaces the custom shell-based logging framework (`DWMSG_*` functions) and will capture execution details, status, and errors for all migrated jobs.
*   **`sql/bq/vertragsdatenabgleich_wrapper_sp.sql`**
    *   **Role**: A BigQuery SQL stored procedure that combines the orchestration and control logic from the original `r_ausd_v_ta_vvl_upgrade.ksh` and `k_ausd_v_ta_vvl_upgrade.ksh` scripts. It handles parameter validation, logs job start/end/errors to `job_log`, and calls the `d_ausd_v_ta_vvl_upgrade_sp` for data transformation.
*   **`python/cloud_composer/vertragsdatenabgleich_dag.py`**
    *   **Role**: An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG is responsible for scheduling and triggering the `vertragsdatenabgleich_wrapper_sp` BigQuery stored procedure, replacing the top-level KornShell wrapper's orchestration role.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Consolidation of Shell Logic into BigQuery Stored Procedures**: The logic from the two KornShell scripts (`r_ausd_v_ta_vvl_upgrade.ksh` and `k_ausd_v_ta_vvl_upgrade.ksh`) was combined into a single BigQuery stored procedure (`vertragsdatenabgleich_wrapper_sp`). This simplifies deployment, centralizes logic within the data platform, and leverages BigQuery's native procedural capabilities for parameter handling, error management, and logging.
*   **BigQuery as the Primary Data Platform**: All data storage and transformation now occur within BigQuery. This aligns with modern cloud data warehousing principles, offering scalability, performance, and managed services.
*   **Cloud Composer for Orchestration**: Apache Airflow, via Cloud Composer, was chosen to replace the shell script's role as the primary orchestrator. This provides robust scheduling, dependency management, monitoring, and a standardized way to manage data pipelines in GCP.
*   **Centralized BigQuery Logging**: The custom shell-based logging framework (`DWMSG_*` functions) was replaced by `INSERT` statements into a dedicated BigQuery `job_log` table. This provides a queryable, centralized log repository for all job executions, improving observability and troubleshooting.
*   **Direct SQL Translation**: The Oracle SQL script (`d_ausd_v_ta_vvl_upgrade.sql`) was directly translated into a BigQuery SQL stored procedure (`d_ausd_v_ta_vvl_upgrade_sp`). Oracle-specific constructs (e.g., `NVL`, `TO_CHAR` for dates, parallel hints, `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`) were adapted to their BigQuery equivalents or removed where BigQuery handles functionality automatically (e.g., parallelism).

**Notable Trade-offs:**

*   **Loss of Shell Scripting Flexibility**: While consolidating logic into BigQuery stored procedures simplifies deployment, it removes the flexibility of shell scripting for certain system-level operations or interactions with external tools not directly integrated with BigQuery.
*   **Dependency on BigQuery Procedural SQL**: The migration relies heavily on BigQuery's procedural SQL capabilities. While powerful, this might require a learning curve for teams more familiar with traditional scripting languages.
*   **Airflow Learning Curve**: Introducing Cloud Composer (Airflow) adds a new technology stack (Python, Airflow concepts) to the environment, which might require training or expertise if not already present.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup**:
    *   Ensure a GCP Project (`your_gcp_project_id`) is established.
    *   Create a BigQuery Dataset (`your_bq_dataset_id`) within the project to house the tables and stored procedures.

2.  **Source Data Migration**:
    *   All Oracle source tables (`sof$ta_vvl_dwh`, `dwh$ta_l_bindefr_aendgr_carm`, `isbert_schema.dwtk_meldungen`) must be migrated from Oracle to BigQuery. Their schemas should be replicated, and historical data loaded.
    *   **Note**: The `isbert_schema.dwtk_meldungen` table is referenced with `your_gcp_project_id.isbert_schema.dwtk_meldungen` in the generated code, implying `isbert_schema` might be a separate dataset or project. Verify this path.

3.  **BigQuery Table and Logging Table Creation**:
    *   Execute `sql/bq/create_sof_ta_vvl_upgrade_table.sql` to create the target table `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade`.
    *   Execute `sql/bq/create_job_log_table.sql` to create the logging table `your_gcp_project_id.your_bq_dataset_id.job_log`.

4.  **BigQuery Stored Procedure Deployment**:
    *   Deploy `sql/bq/d_ausd_v_ta_vvl_upgrade_sp.sql` to create `your_gcp_project_id.your_bq_dataset_id.d_ausd_v_ta_vvl_upgrade_sp`.
    *   Deploy `sql/bq/vertragsdatenabgleich_wrapper_sp.sql` to create `your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_sp`.

5.  **IAM and Permissions Configuration**:
    *   **BigQuery Service Account**: Create or identify a GCP Service Account with the necessary BigQuery roles (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to read from source tables, write to target tables, and execute stored procedures within `your_bq_dataset_id` and `isbert_schema`.
    *   **Cloud Composer Service Account**: Ensure the Cloud Composer environment's service account has permissions to deploy DAGs, trigger BigQuery jobs, and access BigQuery resources.

6.  **Cloud Composer Environment Setup**:
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   Upload `python/cloud_composer/vertragsdatenabgleich_dag.py` to the DAGs folder of your Cloud Composer environment.

7.  **Configuration and Parameterization**:
    *   Update all placeholders (`your_gcp_project_id`, `your_bq_dataset_id`) in the generated SQL and Python files with actual project and dataset IDs.
    *   Review and set the `schedule_interval` in `vertragsdatenabgleich_dag.py` to match the desired execution frequency.

## 5. Known Gaps & Unresolved References

The following items were identified as potential gaps or require further investigation:

*   **Specifics of Sourced Shell Scripts**: The exact functionality of the original sourced shell scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) was not fully detailed. While assumptions were made based on their names (e.g., logging, parameter handling), any complex or unique logic within them might require specific re-implementation in BigQuery UDFs or helper procedures.
*   **Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`**: This Oracle package call was assumed to be a generic SQL execution utility. If it contains complex business logic beyond a simple DDL/DML wrapper, that logic would need to be explicitly migrated to BigQuery.
*   **Performance of Max Date Subquery**: The subquery for `MAX(aenderung_am) GROUP BY vertrags_id` was parallelized in Oracle. While BigQuery handles parallelism automatically, its performance should be closely monitored post-migration to ensure it meets expectations.
*   **Character Encoding**: Verification is needed to ensure that character encoding, especially for non-ASCII characters in `CASE` statements (e.g., `Endgerteupgrade` vs `Endgeräteupgrade`), is correctly handled and maintained during data migration and within BigQuery.
*   **Row Count Retrieval**: The `vertragsdatenabgleich_wrapper_sp` currently logs job completion but does not explicitly capture the number of rows inserted/updated by `d_ausd_v_ta_vvl_upgrade_sp`. If this metric is critical for validation or monitoring, `d_ausd_v_ta_vvl_upgrade_sp` should be modified to return the row count, and the wrapper procedure updated to capture and log it.

## 6. Validation

To ensure the successful migration and correct functioning of the job, perform the following validation steps:

1.  **BigQuery Stored Procedure Execution (Manual)**:
    *   Manually execute `your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_sp` in the BigQuery console with sample `p_job_kennung` and `p_eintrags_nr` parameters.
    *   Monitor the BigQuery job logs for any errors or warnings.

2.  **Cloud Composer DAG Execution**:
    *   Trigger the `vertragsdatenabgleich_vvl_upgrade_dag` in the Cloud Composer UI.
    *   Monitor the Airflow task logs for `execute_vertragsdatenabgleich_sp` for successful completion.

3.  **Log Verification**:
    *   Query the `your_gcp_project_id.your_bq_dataset_id.job_log` table.
    *   **Passing Criteria**: A successful run should show entries for `vertragsdatenabgleich_wrapper_sp` with `status = 'STARTED'` and `status = 'COMPLETED'`, and no entries with `log_level = 'ERROR'` for the specific job run.

4.  **Data Validation**:
    *   **Row Count Comparison**: Compare the number of rows in the target `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade` table with the expected row count from the original Oracle job's output.
    *   **Data Quality Checks**: Perform detailed data quality checks on a representative sample of data in `sof_ta_vvl_upgrade`. Verify that:
        *   `vertrags_id`, `upgradegrund`, and `upgradedatum` columns contain the correct values.
        *   The `CASE` statement logic for `upgradegrund` is correctly applied.
        *   The `upgradedatum` (derived from `MAX(aenderung_am)`) is accurate.
    *   **Functional Equivalence**: Compare the output of the BigQuery job with the output of the legacy Oracle job using identical input data. The results in `sof_ta_vvl_upgrade` should be identical to the `sof$ta_vvl_upgrade` table in Oracle.

**"Passing" means**:
*   The Cloud Composer DAG completes successfully without errors.
*   The `job_log` table records a `COMPLETED` status for the `vertragsdatenabgleich_wrapper_sp` execution.
*   The data in `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade` is accurate, complete, and matches the expected output from the original Oracle system.
*   No unexpected errors or warnings are observed in BigQuery job logs or Cloud Composer task logs.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Executions**:
    *   In the Cloud Composer UI, pause or un-schedule the `vertragsdatenabgleich_vvl_upgrade_dag` to prevent further executions of the migrated job.

2.  **Revert to Legacy System**:
    *   Re-enable or restart the original `r_ausd_v_ta_vvl_upgrade.ksh` job in the legacy Oracle environment. Ensure it can run successfully and produce the expected output.

3.  **BigQuery Data Restoration (if necessary)**:
    *   If the `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade` table was corrupted or incorrectly updated, restore it to a known good state. This can be done by:
        *   Using BigQuery's time travel feature to restore the table to a point in time before the erroneous execution (if within the configured time window).
        *   Loading data from a backup of the `sof_ta_vvl_upgrade` table.
        *   Re-running the original Oracle job and then re-migrating the data to BigQuery (if the issue was with the migration process itself).

4.  **Undeploy GCP Artifacts (Optional, for clean rollback)**:
    *   Delete the `vertragsdatenabgleich_vvl_upgrade_dag` from the Cloud Composer DAGs folder.
    *   Drop the BigQuery stored procedures:
        *   `DROP PROCEDURE IF EXISTS your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_sp;`
        *   `DROP PROCEDURE IF EXISTS your_gcp_project_id.your_bq_dataset_id.d_ausd_v_ta_vvl_upgrade_sp;`
    *   Consider dropping the `sof_ta_vvl_upgrade` table if it's no longer needed or if a clean re-deployment is planned. The `job_log` table can typically remain.

5.  **Root Cause Analysis**:
    *   Investigate the reason for the rollback using the `job_log` table, BigQuery job logs, and Cloud Composer task logs to identify and resolve the underlying issue before attempting re-deployment.