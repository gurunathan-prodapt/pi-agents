# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the data processing job originally orchestrated by the KornShell script `k_ausd_bp_ta_rn_da_vda_tk.ksh` and its associated Oracle SQL script `d_ausd_bp_ta_rn_da_vda_tk.sql`. The job's purpose is to process basis product data related to telephone numbers (DA-, VDA-, and TK-Rufnummern) and store the results in a temporary table.

The job has been migrated from a legacy Oracle/KornShell environment to Google Cloud Platform. The new target platform utilizes **BigQuery** for all data storage and transformation logic, and **Apache Airflow (via Cloud Composer)** for orchestration and scheduling.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`d_ausd_bp_ta_rn_da_vda_tk_bq.sql`**
    *   **Role:** This file contains the core BigQuery SQL logic. It performs the truncation of the target table `sof$ta_rn_da_vda_tk` and then inserts data from the source table `sof$ta_rn_einzeln`, applying the necessary filtering conditions. This SQL directly replaces the functionality of the original `d_ausd_bp_ta_rn_da_vda_tk.sql` script.
*   **`d_ausd_bp_ta_rn_da_vda_tk_dag.py`**
    *   **Role:** This is an Apache Airflow DAG (Directed Acyclic Graph) written in Python. It orchestrates the execution of the BigQuery SQL. It defines a single task using the `BigQueryExecuteQueryOperator` to run the SQL contained within `d_ausd_bp_ta_rn_da_vda_tk_bq.sql` against the specified BigQuery tables. This DAG replaces the orchestration logic previously handled by the `k_ausd_bp_ta_rn_da_vda_tk.ksh` KornShell script.

## 3. Key Design Decisions

Several key design decisions were made during this migration:

*   **Cloud-Native Orchestration with Airflow:** Apache Airflow on Cloud Composer was chosen for orchestration due to its robust scheduling capabilities, native integration with Google Cloud services, monitoring features, and ability to define complex data pipelines as code. This replaces the custom KornShell scripting for job control.
*   **BigQuery for Data Transformation and Storage:** BigQuery was selected as the target data warehouse for its scalability, performance, cost-effectiveness for large datasets, and serverless architecture. All Oracle SQL logic was translated to BigQuery SQL, eliminating the need for a separate database instance.
*   **Consolidation of Logic:** The orchestration logic from the KornShell script and the data transformation logic from the Oracle SQL script were consolidated into a single Airflow DAG executing BigQuery SQL. This simplifies the overall architecture, reduces operational overhead, and leverages Airflow's capabilities for managing the entire workflow.
*   **Removal of Oracle-Specific Hints:** Oracle-specific SQL hints like `/*+ full(rp) parallel(rp,4) */` were removed during the translation to BigQuery SQL. BigQuery's query optimizer automatically handles execution plans and parallelism, rendering such hints unnecessary and potentially counterproductive.
*   **In-SQL Truncation and Insertion:** The `TRUNCATE TABLE` and `INSERT INTO ... SELECT` operations are combined within a single BigQuery SQL statement. This ensures atomicity of the data manipulation within BigQuery and simplifies the Airflow task definition, as a single `BigQueryExecuteQueryOperator` can manage the entire data transformation.
*   **Airflow for Parameter Handling:** Parameters previously handled by the KornShell script (e.g., `p_Stichtag`, `p_JobKennung`) will be managed through Airflow's native mechanisms (e.g., DAG parameters, `dag_run.conf`) or Python logic within the DAG, aligning with cloud-native best practices and removing shell script dependencies.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset and Table Creation:**
    *   Ensure the BigQuery dataset where these tables reside is created.
    *   Create the target BigQuery table `sof$ta_rn_da_vda_tk` with the schema matching the `INSERT` statement (columns: `CNTRCT_ID`, `DA_RN_MSISDN`, `DA_RN_STATUS`, `DA_RN_VALID_TO`, `VDA_RN_MSISDN`, `VDA_RN_STATUS`, `VDA_RN_VALID_TO`, `TK_RN_MSISDN`, `TK_RN_STATUS`, `TK_RN_VALID_TO`).
    *   Verify that the source BigQuery table `sof$ta_rn_einzeln` exists in the same dataset and is populated with the necessary data.
    *   If the `v_datum` logic (from `isbert_schema.dwtk_meldungen`) becomes relevant for future enhancements or other dependencies, ensure its BigQuery equivalent table is also created and populated.
2.  **IAM Permissions:**
    *   The Google Cloud service account associated with the Cloud Composer environment must have appropriate IAM roles to interact with BigQuery. This typically includes `BigQuery Data Editor` or `BigQuery User` roles for the dataset containing `sof$ta_rn_da_vda_tk` and `sof$ta_rn_einzeln`.
    *   The service account also needs permissions to upload and manage DAGs in the Cloud Composer's GCS bucket.
3.  **Data Ingestion:**
    *   Ensure that the data from the original Oracle source for `sof$ta_rn_einzeln` is continuously and correctly ingested or replicated into its BigQuery counterpart. This is a prerequisite for the migrated job to process current data.
4.  **Airflow DAG Deployment:**
    *   Upload the `d_ausd_bp_ta_rn_da_vda_tk_dag.py` file to the DAGs folder of your Cloud Composer environment's GCS bucket.
5.  **Scheduling Configuration:**
    *   Update the `schedule_interval` parameter in `d_ausd_bp_ta_rn_da_vda_tk_dag.py` to match the original job's execution cadence. The current value `None` is a placeholder. For example, use `'0 0 * * *'` for daily execution at midnight UTC.
6.  **BigQuery Location:**
    *   Confirm and set the correct `location` parameter for the `BigQueryExecuteQueryOperator` (e.g., `'US'`, `'EU'`) to match your BigQuery dataset's region.

## 5. Known Gaps & Unresolved References

The following items have been identified as potential gaps or require further follow-up:

*   **Missing File Complexity Data:** The original `file_complexity` data for `k_ausd_bp_ta_rn_da_vda_tk.ksh` was not found. This could mean that a detailed complexity analysis was not performed, potentially leading to an underestimation of effort if hidden complexities exist within the original script beyond the core SQL execution.
*   **Commented-out Code in Original Script:** The KornShell script contains commented-out sections related to `sed`, `sort`, and `join` commands on files like `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`. It is crucial to **confirm if these sections are truly obsolete** or if they represent dormant requirements that might need to be reactivated. If they are still relevant, they would require migration to BigQuery-native transformations (e.g., SQL, Dataflow, or Dataproc).
*   **External Utility Script Logic:** The exact logic within the sourced KornShell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) was not fully analyzed. While assumptions have been made about their general function, a deeper dive might be required if their functionality is complex and needs precise replication or if they contain critical business logic not captured in the core SQL.
*   **Temporary File Usage for Record Counts:** The original ksh script used a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_da_vda_tk.tmp`) to capture processed record counts. If this record count is still required for logging, auditing, or downstream processes, it needs to be explicitly implemented in the Airflow DAG (e.g., by parsing BigQuery job statistics or using XComs).
*   **Relevance of `v_datum`:** The `d_ausd_bp_ta_rn_da_vda_tk.sql` script determines `v_datum` from `isbert_schema.dwtk_meldungen` but does not use it in the `INSERT` statement. It needs to be confirmed if `v_datum` is relevant for broader job context, logging, or other dependencies not immediately apparent. If not, its migration can be omitted.
*   **Dynamic Parameter Handling:** The original ksh script parsed parameters like `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`. The current Airflow DAG does not explicitly handle these. If these parameters are dynamic and critical for the job's execution (e.g., for filtering data based on a specific date or job run), a mechanism to pass and utilize them within the BigQuery SQL or Airflow context needs to be implemented (e.g., using Airflow macros or `dag_run.conf`).

## 6. Validation

To ensure the successful migration and correct functioning of the new BigQuery/Airflow job, the following validation steps should be performed:

1.  **Run the Airflow DAG:**
    *   **Manual Trigger:** Trigger the `d_ausd_bp_ta_rn_da_vda_tk` DAG manually from the Airflow UI.
    *   **CLI Test (Optional):** For local testing or debugging, use the `gcloud composer environments run <env-name> --location <location> dags test d_ausd_bp_ta_rn_da_vda_tk <execution_date>` command.
    *   **BigQuery Console Test (Optional):** Execute the SQL from `d_ausd_bp_ta_rn_da_vda_tk_bq.sql` directly in the BigQuery console for isolated testing of the transformation logic.

2.  **"Passing" Criteria:**
    *   **DAG Completion:** The Airflow DAG `d_ausd_bp_ta_rn_da_vda_tk` must complete successfully without any task failures or errors.
    *   **BigQuery Job Status:** The underlying BigQuery job initiated by the `BigQueryExecuteQueryOperator` should complete successfully.
    *   **Target Table State:**
        *   Verify that the `sof$ta_rn_da_vda_tk` table in BigQuery was truncated (if it contained data prior to the run).
        *   Verify that new data has been inserted into `sof$ta_rn_da_vda_tk`.
    *   **Row Count Validation:** Compare the number of rows inserted into `sof$ta_rn_da_vda_tk` with the expected row count from the source system or a previous run of the legacy job.
    *   **Data Accuracy (Sample Validation):**
        *   Select a representative sample of records from the `sof$ta_rn_einzeln` source table that meet the `WHERE` clause conditions.
        *   Verify that these records are correctly transformed and inserted into `sof$ta_rn_da_vda_tk`, checking values for `CNTRCT_ID`, `DA_RN_MSISDN`, `VDA_RN_MSISDN`, `TK_RN_MSISDN`, and associated status/validity dates.
        *   Compare the output with the results generated by the original Oracle job for the same input data.
    *   **Performance:** Monitor the execution time of the BigQuery job to ensure it meets performance expectations and is comparable to or better than the legacy system.

## 7. Rollback Procedure

In case of issues or critical failures after go-live, the following rollback procedure can be initiated:

1.  **Deactivate/Delete Airflow DAG:**
    *   From the Airflow UI, pause or delete the `d_ausd_bp_ta_rn_da_vda_tk` DAG. This will prevent any further executions of the migrated job.
    *   Alternatively, remove the `d_ausd_bp_ta_rn_da_vda_tk_dag.py` file from the Cloud Composer's DAGs folder in GCS.
2.  **Restore BigQuery Data (if necessary):**
    *   If the `sof$ta_rn_da_vda_tk` table was corrupted or incorrectly populated by the migrated job, restore it to a known good state. This can be done by:
        *   Using BigQuery's Time Travel feature to query data from a point before the erroneous run.
        *   Restoring from a BigQuery table snapshot or backup if such a strategy is in place.
        *   Re-running the original legacy job to repopulate the target table (if the source data is still available and the legacy job can be run against it).
3.  **Reactivate Original Job:**
    *   Re-enable the original KornShell script (`k_ausd_bp_ta_rn_da_vda_tk.ksh`) and its associated scheduler in the legacy environment.
    *   Verify that the legacy job runs successfully and produces the expected output.