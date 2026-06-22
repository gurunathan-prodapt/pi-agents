# MIGRATION_NOTES: BERT_V_TA_DISC_ZUSGF

## 1. Summary

The `BERT_V_TA_DISC_ZUSGF` job, originally orchestrated by UC4 executing KornShell scripts that invoked Oracle PL/SQL, has been migrated. Its purpose is to concatenate discount descriptions from a source table (`sof$ta_discount`) and store the aggregated information into a target table (`sof$ta_disc_zusgf`).

The job has been re-implemented on Google Cloud Platform (GCP).
*   **Target Orchestration Platform:** Apache Airflow (via Cloud Composer).
*   **Target Data Platform:** Google BigQuery for all data storage and transformation.

The original four-stage architecture (UC4 -> Wrapper KSH -> Control KSH -> Oracle SQL) has been streamlined into a single Airflow DAG that directly executes BigQuery SQL.

## 2. Generated artifacts

The migration produced the following files:

*   **`bigquery/sql/d_ausd_v_ta_disc_zusgf_bq.sql`**
    *   **Role:** This SQL script contains the core data transformation logic. It is a BigQuery-compatible translation of the original Oracle PL/SQL script (`d_ausd_v_ta_disc_zusgf.sql`). It handles the derivation of the processing date (`v_datum`), truncates the target table, and then inserts concatenated discount descriptions into `sof_ta_disc_zusgf` using BigQuery's `STRING_AGG` function and Common Table Expressions (CTEs). It also explicitly applies the 500-character length limit for the `rabatt_alle` field.
*   **`airflow/dags/dw_bert_ausd_v_ta_disc_zusgf.py`**
    *   **Role:** This Python file defines the Apache Airflow DAG responsible for orchestrating the job. It replaces the UC4 job and the KornShell scripts. The DAG contains a single `BigQueryExecuteQueryOperator` task that executes the `d_ausd_v_ta_disc_zusgf_bq.sql` script. It manages the workflow, dependencies, and execution on Cloud Composer.

## 3. Key design decisions

*   **GCP as Target Platform:** The entire workflow is moved to GCP, leveraging managed services for scalability, reliability, and reduced operational overhead.
*   **Airflow for Orchestration:** Apache Airflow on Cloud Composer replaces UC4 and the KornShell wrapper/control scripts. This provides a cloud-native, Python-based, and highly observable orchestration layer.
*   **BigQuery for Data Storage and Transformation:** Google BigQuery replaces the Oracle database for both source and target data storage, as well as for executing the transformation logic. This leverages BigQuery's serverless, petabyte-scale analytics capabilities.
*   **SQL-centric Transformation:** The complex Oracle PL/SQL, including object types and pipelined functions, was translated into standard BigQuery SQL using CTEs and `STRING_AGG`. This simplifies the logic, makes it more readable, and leverages BigQuery's optimized SQL engine.
*   **Explicit Length Handling for `rabatt_alle`:** Given the original Oracle `VARCHAR2(500)` constraint, `SUBSTR` functions were explicitly added to the BigQuery SQL to ensure the concatenated `rabatt_alle` field adheres to the 500-character limit, both for individual discount parts and the final aggregated string. This addresses a potential difference in `STRING_AGG` behavior compared to Oracle's implicit handling.
*   **Streamlined Architecture:** The original four-tier architecture (UC4 -> Wrapper KSH -> Control KSH -> Oracle SQL) was consolidated into a single Airflow DAG executing a BigQuery SQL script. This significantly reduces complexity and points of failure.
*   **`v_datum` Derivation in SQL:** The logic to derive the processing date (`v_datum`) was embedded directly into the BigQuery SQL script using a `DECLARE` statement and a `SELECT` query against the `dwtk_meldungen` table. This keeps the date derivation close to the data and avoids passing it as an external parameter unless dynamic override is required.
*   **Direct `TRUNCATE TABLE`:** The Oracle `DWPA_UTIL_SKRIPT.runstatement` call for truncating the target table was replaced with a direct `TRUNCATE TABLE` statement in BigQuery SQL, as BigQuery handles this operation natively and efficiently.

**Notable Trade-offs:**
*   **Loss of Oracle Procedural Features:** The migration involved re-architecting away from Oracle-specific PL/SQL features (object types, pipelined functions). While BigQuery SQL is powerful, it doesn't offer the same procedural capabilities, requiring a different approach to complex logic.
*   **Dependency on BigQuery Performance:** The job's performance is now entirely dependent on BigQuery's query engine. While generally excellent, complex queries might require optimization.
*   **Explicit Data Type and Length Management:** BigQuery's more flexible schema-on-read approach means that explicit casting and length constraints (like `SUBSTR`) are sometimes necessary to replicate strict Oracle behaviors.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `dwh_prod`) exists in your GCP project. If not, create it.
2.  **BigQuery Table Creation (DDL):**
    *   Create the necessary source and target tables in BigQuery with appropriate schemas. This includes:
        *   `gcp_project_id.dwh_prod.dwtk_meldungen`
        *   `gcp_project_id.dwh_prod.sof_ta_discount`
        *   `gcp_project_id.dwh_prod.sof_ta_disc_zusgf`
    *   The schemas should reflect the original Oracle table structures, with data types mapped to BigQuery equivalents (e.g., `VARCHAR2` to `STRING`, `NUMBER` to `INT64`/`NUMERIC`).
3.  **Initial Data Load:**
    *   Perform a one-time bulk load of historical data from the Oracle source tables (`dwtk_meldungen`, `sof$ta_discount`) into their respective BigQuery counterparts. This can be done using tools like Cloud Data Fusion, Dataflow, or BigQuery's native data loading capabilities.
    *   Establish an ongoing synchronization mechanism (e.g., CDC, scheduled batch loads) to keep the BigQuery source tables up-to-date until the Oracle source is fully decommissioned.
4.  **IAM Permissions:**
    *   Grant the Airflow service account (associated with your Cloud Composer environment) the necessary IAM roles to interact with BigQuery. This typically includes:
        *   `BigQuery Data Editor` on the `dwh_prod` dataset (for `TRUNCATE` and `INSERT` into `sof_ta_disc_zusgf`).
        *   `BigQuery Data Viewer` on the `dwh_prod` dataset (for `SELECT` from `dwtk_meldungen` and `sof_ta_discount`).
5.  **Airflow Connection:**
    *   Ensure the `google_cloud_default` Airflow connection is correctly configured in your Cloud Composer environment, providing the necessary credentials for BigQuery access.
6.  **Placeholder Replacement:**
    *   In both `bigquery/sql/d_ausd_v_ta_disc_zusgf_bq.sql` and `airflow/dags/dw_bert_ausd_v_ta_disc_zusgf.py`, replace `'gcp_project_id.dwh_prod'` and `'gcp_project_id'` placeholders with your actual GCP project ID and BigQuery dataset name.
7.  **Airflow DAG Deployment:**
    *   Upload the `airflow/dags/dw_bert_ausd_v_ta_disc_zusgf.py` file to the DAGs folder of your Cloud Composer environment.
    *   Upload the `bigquery/sql/d_ausd_v_ta_disc_zusgf_bq.sql` file to a location accessible by the Airflow DAG, typically within the DAGs folder or a subfolder (e.g., `dags/sql/`).
8.  **Scheduling Configuration:**
    *   Adjust the `schedule_interval` in `airflow/dags/dw_bert_ausd_v_ta_disc_zusgf.py` to match the original UC4 job's frequency (e.g., `@daily`, `0 5 * * *`).
9.  **Review `v_datum` Logic:**
    *   Confirm that the `v_datum` derivation logic from `dwtk_meldungen` in the BigQuery SQL accurately reflects the original Oracle behavior and that `dwtk_meldungen` is populated correctly in BigQuery.

## 5. Known gaps & unresolved references

*   **Shell Script Business Logic:** While the KornShell scripts were primarily for orchestration, a final review is recommended to ensure no critical business logic (e.g., complex error handling, specific environment variable manipulations) from `f_alis_msgerr.ksh`, `h_alis_job.ksh`, or other referenced scripts was missed and needs to be replicated in Airflow or Python.
*   **Dynamic Parameterization:** The mapping of original UC4 variables (e.g., `&DWH_JOB_KENNUNG`) and KornShell parameters (e.g., `$p_JobKennung`) to Airflow DAG parameters or variables has been simplified to direct SQL execution. If any of these parameters were truly dynamic and influenced the core SQL logic, this might need further refinement (e.g., using Airflow `params` or `macros` in the SQL).
*   **CARMEN DB Dependency:** The original design document noted a reference to `CARMEN DB` via a DB link, though its active usage in the provided SQL was not evident. If `CARMEN DB` data is implicitly used or required by other parts of the system, its migration or integration strategy (e.g., federated queries, replication) needs to be defined.
*   **Tracing and Spooling:** The original Oracle script used `START ../trace.sql.cfg` and `SPOOL` commands for tracing and output capture. These have been replaced by Airflow's native logging and Cloud Logging. Ensure that the level of detail and accessibility of logs meets operational requirements.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Trigger the Airflow DAG:**
    *   In the Airflow UI, navigate to the `dw_bert_ausd_v_ta_disc_zusgf` DAG.
    *   Manually trigger a run.
2.  **Monitor DAG Execution:**
    *   Observe the DAG run in the Airflow UI. Ensure all tasks complete successfully without errors.
    *   Check the task logs for any warnings or unexpected output.
3.  **Verify BigQuery Output:**
    *   Once the DAG completes, query the target table in BigQuery: `SELECT * FROM gcp_project_id.dwh_prod.sof_ta_disc_zusgf LIMIT 100;`
    *   **What "passing" means:**
        *   **Successful DAG Run:** The `execute_discount_concatenation` task in the Airflow DAG completes with a "success" status.
        *   **Target Table Population:** The `sof_ta_disc_zusgf` table in BigQuery is populated with data.
        *   **Data Consistency:**
            *   **Row Count:** Compare the row count of the `sof_ta_disc_zusgf` table in BigQuery with the corresponding table in the original Oracle environment for the same processing date/data set.
            *   **Data Content:** Sample a significant number of records and compare the `cntrct_id`, `cntrct_obj_version`, `disc_vector_ty`, and especially the `rabatt_alle` values against the original Oracle output. Pay close attention to the concatenation order, delimiters, and the 500-character length constraint.
            *   **`rabatt_alle` Length:** Verify that no `rabatt_alle` string in BigQuery exceeds 500 characters.
        *   **No Errors in Logs:** No critical errors or unexpected warnings are present in the Airflow task logs or BigQuery job logs.

## 7. Rollback procedure

In case of issues with the migrated job, the following rollback procedure can be executed:

1.  **Disable New Airflow DAG:**
    *   In the Airflow UI, toggle off the `dw_bert_ausd_v_ta_disc_zusgf` DAG to prevent further runs.
2.  **Re-enable Original UC4 Job:**
    *   Re-activate the original `DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml` job in UC4.
3.  **Data Reversion (if necessary):**
    *   If the migrated job has written incorrect or incomplete data to `sof_ta_disc_zusgf` in BigQuery, and this data needs to be reverted:
        *   **Option A (Time Travel):** If BigQuery's time travel feature is enabled and the window is sufficient, revert the `sof_ta_disc_zusgf` table to a state before the problematic Airflow DAG run.
        *   **Option B (Backup/Snapshot):** If backups or snapshots of the `sof_ta_disc_zusgf` table were taken before the migration or problematic run, restore the table from the most recent valid backup.
        *   **Option C (Truncate and Reload):** If data integrity allows, truncate the `sof_ta_disc_zusgf` table in BigQuery and allow the re-enabled Oracle job to populate its original target table. If the Oracle job also writes to BigQuery (e.g., via CDC), ensure the BigQuery table is cleared or correctly synchronized.
4.  **Investigate and Rectify:**
    *   Analyze the logs and data discrepancies to identify the root cause of the failure in the migrated job. Rectify the issues in the BigQuery SQL or Airflow DAG.
5.  **Re-test and Re-deploy:**
    *   Once issues are resolved, re-test the migrated job in a staging environment before attempting another go-live.