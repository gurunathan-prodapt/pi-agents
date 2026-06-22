# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh`. The original job was responsible for orchestrating the provisioning of a contract cache for "Forderungsscoring" (FOS) by extracting snapshot-based data from the Data Warehouse (DWH).

The job has been migrated to a cloud-native architecture:
*   **Source:** `r_ausd_rechempf.ksh` (KornShell orchestrator) and its invoked core logic (identified as `d_ausd_rechempf.sql` within the generated code, which was likely part of `k_ausd_rechempf.ksh` or a dependency).
*   **Target Platform:** Google Cloud Platform, utilizing **Apache Airflow** for orchestration and **Google BigQuery** for data processing and storage.

The migrated solution now runs as an Airflow DAG, executing BigQuery SQL to perform the data extraction and transformation, populating FOS-related tables.

## 2. Generated Artifacts

The migration produced the following artifacts:

*   **`dags/isbert/r_ausd_rechempf_dag.py`**
    *   **Role:** This Python file defines the Airflow DAG `isbert_r_ausd_rechempf_dag`. It replaces the original `r_ausd_rechempf.ksh` script, handling parameter parsing, date calculations, orchestration of data processing, and logging.
    *   **Key Components:**
        *   `parse_params_and_setup` task: A PythonOperator responsible for parsing input parameters (`stichtag_ddmmyyyy`, `restart_value`), setting default values, and calculating necessary date formats. It pushes these values to Airflow XComs for use by downstream tasks.
        *   `execute_bq_load_main_script` task: A `BigQueryExecuteQueryOperator` that executes the core data processing logic. This task contains the BigQuery SQL derived from the legacy system's processing script (likely `d_ausd_rechempf.sql`).
        *   `log_status_task` task: A PythonOperator for logging job completion status, replacing legacy `DWMSG_SetzeStatusOK` calls.

*   **Embedded BigQuery SQL within `execute_bq_load_main_script`**
    *   **Role:** This SQL code, embedded directly in the Airflow DAG, performs the actual data extraction, transformation, and loading. It replaces the logic previously contained within `k_ausd_rechempf.ksh` (specifically, the `d_ausd_rechempf.sql` component).
    *   **Functionality:**
        *   Creates or replaces several intermediate and final target tables in BigQuery.
        *   **Intermediate Tables:**
            *   `sof_ta_means_of_pay`: Derived from `carmen_source.ta_means_of_payment`.
            *   `sof_ta_bank`: Derived from `carmen_source.ta_bank` and `carmen_source.ta_bank_international`.
            *   `sof_ta_bank_verb`: Joins `sof_ta_means_of_pay` and `sof_ta_bank`.
            *   `sof_ta_bank_zuord`: Joins `sof_ta_bank_verb` and `fos_source.ta_e_regulierer`.
        *   **Final Target Tables:**
            *   `sof_ta_p_rech_empf`: Populated by joining `sof_ta_bank_zuord`, `fos_source.ta_e_reach_re`, and `fos_source.ta_e_business_re`. This table contains the core "Rechnungsempfänger" (invoice recipient) data.
            *   `sof_ta_p_d1_vpn`: Populated from `dwh_source.vi_s_ibasisprodukt` based on `vpn_id` and `basisprodukt_id` filters.

## 3. Key Design Decisions

*   **Airflow for Orchestration:** The KornShell orchestrator (`r_ausd_rechempf.ksh`) was replaced by an Airflow DAG. This decision leverages Airflow's capabilities for scheduling, dependency management, robust logging, error handling, and cloud-native integration, providing a more scalable and maintainable solution than shell scripting.
*   **BigQuery for Data Processing:** The core data transformation logic, previously executed via an unknown mechanism (likely Oracle SQL within `k_ausd_rechempf.ksh`), was translated into BigQuery SQL. BigQuery offers serverless execution, high performance for analytical workloads, and cost-effectiveness, aligning with modern data warehousing practices.
*   **Embedded SQL in DAG:** For this specific job, the BigQuery SQL logic is embedded directly within the `BigQueryExecuteQueryOperator` in the Python DAG. This approach keeps the entire job definition (orchestration and logic) in a single file, simplifying deployment and version control for smaller, self-contained tasks.
*   **Parameter Handling via Airflow Params and XComs:** Command-line argument parsing (`getopts`) and shell variable passing were replaced by Airflow DAG parameters and XComs. This provides a structured, type-safe, and auditable way to pass configuration and runtime values (like `stichtag` and `restart_value`) between tasks.
*   **Python for Utility Functions:** Custom shell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) were replaced by standard Python libraries (e.g., `pendulum` for date handling) or Airflow's built-in features (e.g., logging). This reduces reliance on shell-specific utilities and promotes a unified Python-based development environment.
*   **`CREATE OR REPLACE TABLE` for Temporary/Target Tables:** BigQuery's `CREATE OR REPLACE TABLE` statement is used for all intermediate and final target tables. This implicitly handles the truncation and recreation of tables, simplifying the SQL logic compared to explicit `TRUNCATE` or `DROP` statements often found in legacy scripts. This also implies a full reload strategy for the target tables.

## 4. Manual Steps Before Go-Live

Before the migrated job can be deployed and run in production, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the following BigQuery datasets exist in the target GCP project:
        *   `isbert_dwh` (for ISBERT DWH source tables)
        *   `carmen_source` (for Carmen source tables)
        *   `fos_source` (for FOS source tables)
        *   `dwh_source` (for general DWH source tables)
        *   `fos_target` (for target tables created by this job)
    *   These datasets must be created with appropriate regional settings.

2.  **IAM/Permissions Configuration:**
    *   The Airflow service account (or the service account used by the Airflow worker executing the DAG) must have the necessary IAM roles to:
        *   Read data from the source BigQuery datasets (`isbert_dwh`, `carmen_source`, `fos_source`, `dwh_source`).
        *   Create, overwrite, and write data to tables in the `fos_target` BigQuery dataset.
        *   Execute BigQuery jobs.
        *   Access Airflow XComs.

3.  **Airflow Connection Setup:**
    *   Verify that an Airflow GCP connection named `google_cloud_default` is configured correctly in the Airflow environment. This connection is used by the `BigQueryExecuteQueryOperator` to authenticate with BigQuery.

4.  **Update GCP Project ID:**
    *   The `project_id` parameter in the DAG definition (`dags/isbert/r_ausd_rechempf_dag.py`) currently defaults to `"your-gcp-project-id"`. This **must be updated** to the actual GCP Project ID where BigQuery operations will be performed.

5.  **Source Data Migration:**
    *   All source tables referenced in the BigQuery SQL (e.g., `carmen_source.ta_means_of_payment`, `carmen_source.ta_bank`, `carmen_source.ta_bank_international`, `fos_source.ta_e_regulierer`, `fos_source.ta_e_reach_re`, `fos_source.ta_e_business_re`, `dwh_source.vi_s_ibasisprodukt`) must be migrated and populated in their respective BigQuery datasets before this DAG can run successfully.

6.  **Scheduling Configuration:**
    *   The DAG is currently defined with `schedule=None`. For production, configure the appropriate Airflow schedule (e.g., `@daily`, a cron expression) based on the original job's execution frequency.

## 5. Known Gaps & Unresolved References

Several discrepancies and unresolved items were identified during the migration process:

*   **Discrepancy in Core Logic (`k_ausd_rechempf.ksh` vs. `d_ausd_rechempf.sql`):**
    *   The migration design document focused on `k_ausd_rechempf.ksh` as the core logic, describing it as extracting "Vertrags-Cache" data from DWH tables (`TA_C_VERTRAG`) with specific date filtering (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM`).
    *   However, the generated BigQuery SQL explicitly references `d_ausd_rechempf.sql` in its comments and implements logic that processes data from `carmen_source`, `fos_source`, and `dwh_source` to build FOS-related tables (`sof_ta_p_rech_empf`, `sof_ta_p_d1_vpn`).
    *   **Gap:** The generated SQL **does not appear to implement the "Vertrags-Cache" extraction logic** from `TA_C_VERTRAG` as initially described for `k_ausd_rechempf.ksh`. This suggests either `k_ausd_rechempf.ksh` had multiple distinct functions, or `d_ausd_rechempf.sql` was a separate, but related, component that was prioritized for migration. A thorough review of `k_ausd_rechempf.ksh` and `d_ausd_rechempf.sql` is required to confirm the full scope and ensure all necessary logic has been migrated.

*   **Restart Logic (`p_wiederanlaufWert`) - B4 Item:**
    *   The original design document indicated that `p_wiederanlaufWert` was used for incremental processing, potentially involving deletion of existing records (`die Eintraege bzgl. Werten >= diesem Wert werden geloescht`) and selective insertion based on `DWH_VERTRAG_ID`.
    *   **Gap:** The generated BigQuery SQL **does not utilize the `restart_value` parameter** for incremental processing. Instead, it uses `CREATE OR REPLACE TABLE` for all target tables, implying a full reload on each run. This is a significant functional difference.
    *   **Action Required (B4):** The incremental/restart logic needs to be designed and implemented in BigQuery SQL if it is a required feature. This would involve conditional logic to either perform a full reload or an incremental update based on `restart_value`, potentially using `MERGE` statements or a combination of `DELETE` and `INSERT`.

*   **`DW_EintragsNr` Handling:**
    *   The original script used `DW_EintragsNr` for logging traceability.
    *   **Gap:** While Airflow provides `task_instance_id` and `run_id` for similar purposes, `DW_EintragsNr` is not explicitly mapped or passed through in the current DAG. If specific traceability to the legacy `DW_EintragsNr` format is required, this needs to be addressed.

*   **`MIN(sysdate,maxladedatum)` Date Logic:**
    *   The original script's usage text mentioned `MIN(sysdate,maxladedatum)` for `v_datum` for synchronization purposes. The DAG currently defaults `stichtag` to `today` if not provided.
    *   **Gap:** This might not fully replicate the `maxladedatum` dependency. If `maxladedatum` from a source table is critical for determining the `stichtag`, this logic needs to be explicitly implemented in the `parse_params_and_setup` task, potentially by querying a BigQuery metadata table or the source table itself.

## 6. Validation

To validate the successful migration and functionality of the `isbert_r_ausd_rechempf_dag` Airflow DAG, follow these steps:

1.  **Manual Trigger with Default Parameters:**
    *   In the Airflow UI, unpause the `isbert_r_ausd_rechempf_dag`.
    *   Trigger the DAG manually without providing any custom parameters. This will execute the job for the current date (`stichtag` defaults to today).
    *   Monitor the DAG run in the Airflow UI to ensure all tasks complete successfully without errors.

2.  **Manual Trigger with Specific `stichtag`:**
    *   Trigger the DAG manually, providing a specific `stichtag_ddmmyyyy` (e.g., `01012023`) in the DAG run configuration.
    *   Monitor the DAG run for successful completion.

3.  **Data Validation - "Passing" Criteria:**
    *   **Existence:** Verify that the target tables (`fos_target.sof_ta_p_rech_empf` and `fos_target.sof_ta_p_d1_vpn`) are created and populated in BigQuery.
    *   **Row Counts:** Compare the row counts of the newly created BigQuery target tables with the corresponding output from the legacy system for the same `stichtag`. The counts should match.
    *   **Data Integrity:**
        *   Perform spot checks on a sample of records to ensure column values are correctly transformed and loaded.
        *   Calculate aggregate values (e.g., `SUM`, `AVG`, `COUNT DISTINCT`) on key columns in both the BigQuery output and the legacy output. These aggregates should match.
        *   (Optional but Recommended) If possible, perform a full data comparison (e.g., using checksums or row-by-row comparison tools) between the BigQuery output and the legacy output for a given `stichtag`.
    *   **Schema Validation:** Ensure the schema (column names, data types) of the BigQuery target tables matches the expected schema from the legacy system.

4.  **Log Review:**
    *   After each DAG run, review the Airflow task logs for any warnings, errors, or unexpected messages.
    *   Verify that the `log_status_task` correctly indicates job completion.

5.  **Parameter Validation (if restart logic is implemented):**
    *   If the `restart_value` logic is implemented (as a B4 item), test the DAG with different `restart_value` inputs and verify that the incremental processing behaves as expected, only processing/updating records greater than the specified value.

## 7. Rollback Procedure

In case of issues or failure during the go-live or post-migration, the following rollback procedure can be executed:

1.  **Pause/Delete Airflow DAG:**
    *   In the Airflow UI, immediately **pause** the `isbert_r_ausd_rechempf_dag` to prevent further executions.
    *   If necessary, **delete** the DAG from the Airflow environment.

2.  **Revert BigQuery Target Tables (if necessary):**
    *   Since the current implementation uses `CREATE OR REPLACE TABLE`, the target tables (`fos_target.sof_ta_p_rech_empf`, `fos_target.sof_ta_p_d1_vpn`) are fully overwritten on each run.
    *   If the data in these tables needs to be reverted to a state prior to the failed migration run, you have a few options:
        *   **Time Travel:** BigQuery allows querying tables at a specific point in time (up to 7 days by default). You can use this feature to restore data if the corruption is recent.
        *   **Backup/Snapshot:** If a backup or snapshot strategy was in place for these tables, restore from the last known good state.
        *   **Re-run Legacy Job:** The most straightforward approach is to re-enable and re-run the original legacy job to repopulate the target tables with correct data.

3.  **Resume Legacy Job:**
    *   **Re-enable** the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh` job in its legacy scheduling system.
    *   **Verify** that the legacy job runs successfully and produces the expected output.

4.  **Troubleshoot and Redesign:**
    *   Analyze the root cause of the migration failure.
    *   Address any identified gaps or issues (e.g., implementing the `restart_value` logic, correcting SQL transformations).
    *   Re-test the migrated solution thoroughly in a non-production environment before attempting another go-live.