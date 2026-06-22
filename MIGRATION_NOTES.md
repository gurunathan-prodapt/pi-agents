# MIGRATION_NOTES.md

## 1. Summary

The ETL job `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` has been migrated. This job enriches discount data by adding contract number and standard contract template information.

**Original Platform:**
*   **Orchestration:** UC4 Unix job definition.
*   **Execution:** KornShell wrapper scripts invoking an Oracle SQL*Plus script.
*   **Database:** Oracle Database.

**Target Platform:**
*   **Orchestration:** Airflow DAG running on Google Cloud Composer.
*   **Execution:** PySpark script running on Google Cloud Dataproc, executing BigQuery SQL.
*   **Database:** Google BigQuery.

The migration involved re-engineering the orchestration, wrapper logic, and core transformation to leverage cloud-native GCP services, ensuring scalability, maintainability, and alignment with modern data warehousing practices.

## 2. Generated Artifacts

The following artifacts were generated as part of this migration:

*   **`dags/dw_bert_ausd_v_ta_p_discount_rr.py`**
    *   **Role:** Airflow DAG definition. This Python script orchestrates the entire job, defining the sequence of tasks, handling retries, and managing the overall workflow. It submits the PySpark job to a Dataproc cluster.
    *   **Replaces:** `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml` (UC4 job definition).

*   **`pyspark_scripts/r_ausd_v_ta_p_discount_rr.py`**
    *   **Role:** PySpark wrapper script. This Python script runs on Dataproc, replacing the functionality of the original KornShell scripts. It handles environment setup, parameter passing (though minimal for this job), error handling, and crucially, reads and executes the BigQuery SQL transformation.
    *   **Replaces:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh` and `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh`.

*   **`sql_scripts/d_ausd_v_ta_p_discount_rr.sql.bq`**
    *   **Role:** BigQuery SQL transformation script. This file contains the core data manipulation logic, translated from Oracle SQL*Plus to BigQuery SQL syntax. It performs the truncation of the target table and the subsequent insertion of enriched discount data.
    *   **Replaces:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_discount_rr.sql`.

*   **`ddl/ddl_tables.sql.bq`**
    *   **Role:** BigQuery Data Definition Language (DDL) script. This script contains `CREATE TABLE IF NOT EXISTS` statements for the target table (`dw.ta_p_discount_rr`) and the necessary source tables (`dw.dwtk_meldungen`, `dw.ta_discount_rr`, `dw.ta_cntrct_crs`, `dw.ta_cntrct_templ`) in BigQuery.
    *   **Replaces:** Implicit Oracle table definitions.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Cloud-Native Orchestration with Airflow:** Airflow on Cloud Composer was chosen to replace UC4 for job scheduling and workflow management. This provides a robust, scalable, and observable orchestration layer fully integrated with GCP services.
*   **PySpark for Wrapper Logic:** The KornShell wrapper scripts were re-implemented in PySpark and executed on Dataproc. This decision was made to:
    *   **Centralize Logic:** Consolidate environment setup, parameter handling, and error trapping into a modern, maintainable language.
    *   **Cloud Integration:** Leverage Dataproc's managed Spark environment for scalable execution and seamless integration with other GCP services like BigQuery and Cloud Logging.
    *   **Future Flexibility:** PySpark offers greater flexibility for complex data transformations or dynamic SQL generation if the job requirements evolve.
*   **BigQuery for Core Transformation:** The Oracle SQL*Plus script was translated to BigQuery SQL. BigQuery was selected as the target data warehouse for its serverless architecture, automatic scaling, high performance for analytical queries, and cost-effectiveness.
*   **Direct BigQuery SQL Execution via PySpark:** Instead of embedding the SQL directly into the PySpark script, the SQL is kept in a separate `.sql.bq` file. The PySpark script reads and executes this file using the BigQuery client library. This promotes separation of concerns, making the SQL easier to manage, version control, and test independently.
*   **BigQuery as Unified Data Storage:** All source and target tables were migrated to BigQuery, eliminating the dependency on the legacy Oracle database and simplifying the data landscape.

**Notable Trade-offs:**

*   **Dataproc Overhead:** While PySpark offers flexibility, using Dataproc for a relatively simple BigQuery SQL execution introduces the overhead of managing a Spark cluster (even if ephemeral). For simpler transformations, a direct `BigQueryOperator` in Airflow might have been considered, but the design prioritized a consistent pattern for migrating KornShell wrapper logic.
*   **Loss of Oracle-Specific Features:** Oracle-specific utilities (e.g., `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`, SQL*Plus tracing) were replaced with BigQuery equivalents or standard cloud logging, requiring a re-evaluation of their original purpose and impact.
*   **Initial Setup Complexity:** Setting up Cloud Composer, Dataproc, and BigQuery infrastructure requires initial configuration and IAM management, which is more involved than simply running a shell script on an existing Unix host.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **GCP Project Configuration:**
    *   Update the `PROJECT_ID` placeholder (`your-gcp-project-id`) in `dags/dw_bert_ausd_v_ta_p_discount_rr.py` and `pyspark_scripts/r_ausd_v_ta_p_discount_rr.py` with your actual GCP project ID.

2.  **Cloud Storage (GCS) Bucket:**
    *   Create a GCS bucket (e.g., `your-gcs-bucket`) to store the PySpark script and the BigQuery SQL file.
    *   Update the `main_python_file_uri` and `file_uris` in `dags/dw_bert_ausd_v_ta_p_discount_rr.py` with the correct GCS path to your bucket.
    *   Upload `pyspark_scripts/r_ausd_v_ta_p_discount_rr.py` to `gs://your-gcs-bucket/pyspark_scripts/`.
    *   Upload `sql_scripts/d_ausd_v_ta_p_discount_rr.sql.bq` to `gs://your-gcs-bucket/sql_scripts/`.

3.  **Dataproc Cluster:**
    *   Ensure a Dataproc cluster named `bert-dataproc-cluster` exists in the specified region (`us-central1`) or update the `cluster_name` in `dags/dw_bert_ausd_v_ta_p_discount_rr.py` to an existing cluster. If not, create one.
    *   The cluster should be configured with appropriate machine types and auto-scaling policies to handle the workload.

4.  **BigQuery Datasets and Tables:**
    *   Ensure the `dw` BigQuery dataset exists in your GCP project.
    *   Execute the DDL statements from `ddl/ddl_tables.sql.bq` to create the target table (`dw.ta_p_discount_rr`) and the necessary source tables (`dw.dwtk_meldungen`, `dw.ta_discount_rr`, `dw.ta_cntrct_crs`, `dw.ta_cntrct_templ`).
    *   **Crucially, ensure all source tables are populated with data from the legacy Oracle system.** This typically involves a separate data ingestion process (e.g., using Data Transfer Service, Striim, Fivetran, or custom ETL).

5.  **IAM Permissions:**
    *   The Service Account used by your Cloud Composer environment (Airflow) and the Dataproc cluster's worker nodes must have the following IAM roles:
        *   `BigQuery Data Editor` (for `dw` dataset)
        *   `BigQuery Job User`
        *   `Dataproc Worker`
        *   `Storage Object Viewer` (to read scripts from GCS)
        *   `Storage Object Creator` (if Dataproc writes logs/temp files to GCS)

6.  **Scheduling:**
    *   The Airflow DAG is currently set with `schedule=None`. Determine the appropriate business schedule for this job and update the `schedule` parameter in `dags/dw_bert_ausd_v_ta_p_discount_rr.py` accordingly (e.g., `schedule="@daily"` or a specific cron expression).

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or require further attention:

*   **Scheduling Information:** The original UC4 job lacked explicit scheduling information. The Airflow DAG's schedule is currently `None` and **must be determined based on business requirements** before production deployment.
*   **Workflow Context:** No `JOBP` or `JSCH` files were provided for the UC4 job, meaning its broader workflow context and dependencies within the legacy system are unknown. This migration assumes `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` is a standalone job. If it has upstream or downstream dependencies in UC4, these will need to be identified and integrated into the Airflow ecosystem.
*   **KornShell Utility Equivalents:** The original KornShell scripts sourced several utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, etc.). While the core wrapper logic has been re-implemented in PySpark, any highly specific or complex functionalities of these utilities (e.g., custom error reporting formats, unique date calculations) should be verified to ensure their PySpark equivalents behave identically or are no longer required.
*   **Oracle `DWPA_UTIL_SKRIPT.runstatement`:** The custom Oracle PL/SQL procedure call `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_p_discount_rr');` has been replaced by a direct `TRUNCATE TABLE` statement in BigQuery. This is functionally equivalent for the `TRUNCATE` operation, but any other side effects or logging performed by the original PL/SQL procedure are no longer present and should be considered if critical.
*   **Performance Optimization (Oracle Hints):** The original Oracle SQL used `/*+ parallel(da,4) parallel(c,4) parallel(ct,4) */` hints. BigQuery automatically handles parallelism, but the performance of the migrated BigQuery SQL should be monitored closely after deployment. If performance issues arise, BigQuery query optimization techniques (e.g., clustering, partitioning, materialised views) may be necessary.
*   **SQL*Plus Tracing and Spooling:** The `START ../trace.sql.cfg` and `SPOOL` commands in the Oracle SQL*Plus script are specific to the Oracle environment. In BigQuery, logging and tracing are handled by Cloud Logging, which captures BigQuery job details and PySpark script output. Users should rely on Cloud Logging for monitoring and debugging.

## 6. Validation

To validate the successful migration and correct functionality of the `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` job:

1.  **Trigger the Airflow DAG:**
    *   In the Cloud Composer UI, navigate to the `dw_bert_ausd_v_ta_p_discount_rr` DAG.
    *   Manually trigger a run of the DAG.

2.  **Monitor Airflow Task Execution:**
    *   Observe the DAG run in the Airflow UI. All tasks (`start_task`, `run_pyspark_transformation`, `end_task`) should complete successfully (green status).
    *   Check the logs for the `run_pyspark_transformation` task for any errors or warnings from the PySpark script or BigQuery job.

3.  **Verify BigQuery Job Completion:**
    *   In the GCP Console, navigate to BigQuery -> SQL Workspace -> Query History.
    *   Confirm that the BigQuery query submitted by the PySpark script completed successfully.
    *   Review the BigQuery job details for any errors, warnings, or performance metrics.

4.  **Data Validation:**
    *   **Row Count Comparison:**
        *   Before running the migrated job, record the row count of the legacy Oracle `sof$ta_p_discount_rr` table.
        *   After the migrated job completes, query the BigQuery target table `dw.ta_p_discount_rr` and compare its row count to the legacy system's output. They should match.
    *   **Sample Data Comparison:**
        *   Select a representative sample of data (e.g., 100-1000 rows) from the legacy Oracle `sof$ta_p_discount_rr` table.
        *   Perform the same query on the BigQuery `dw.ta_p_discount_rr` table.
        *   Compare the values for all columns, especially `contract_number` and `std_vertrag`, to ensure data integrity and correctness of the transformation logic.
    *   **`v_datum` Derivation:** If possible, verify the `v_datum` value derived in BigQuery matches what would have been derived in the Oracle environment for the same `timecreated` and `job_kennung` conditions.

**"Passing" Criteria:**

*   The Airflow DAG `dw_bert_ausd_v_ta_p_discount_rr` completes successfully without any task failures.
*   The underlying BigQuery job completes successfully without errors.
*   The row count in `dw.ta_p_discount_rr` matches the expected output from the legacy system.
*   A sample data comparison confirms that the data in `dw.ta_p_discount_rr` is identical or logically equivalent to the legacy output.
*   No critical errors or unexpected warnings are observed in Cloud Logging for the Dataproc job or BigQuery queries.

## 7. Rollback Procedure

In case of issues with the migrated job (e.g., data corruption, performance degradation, or critical failures), the following rollback procedure should be followed:

1.  **Disable New Job:**
    *   In the Cloud Composer Airflow UI, pause or delete the `dw_bert_ausd_v_ta_p_discount_rr` DAG to prevent further execution.

2.  **Re-enable Legacy Job:**
    *   Re-activate the original UC4 job `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` in the legacy environment.
    *   Verify that the legacy job runs successfully and produces the expected output in the Oracle database.

3.  **Data Rollback (if necessary):**
    *   If the BigQuery target table `dw.ta_p_discount_rr` was corrupted or incorrectly populated by the migrated job, restore it to a known good state.
    *   **Option A (Time Travel):** BigQuery supports time travel. You can query the table as it was at a specific timestamp before the problematic run and recreate it, or use `CREATE TABLE AS SELECT * FROM dw.ta_p_discount_rr FOR SYSTEM_TIME AS OF 'YYYY-MM-DD HH:MM:SS UTC'`.
    *   **Option B (Snapshot/Backup):** If BigQuery snapshots or backups were configured for `dw.ta_p_discount_rr`, restore from the most recent valid snapshot.
    *   **Option C (Re-ingest):** If the data is entirely derived and can be safely truncated and re-inserted, and the source data is correct, you might consider truncating `dw.ta_p_discount_rr` and re-running the legacy job (if it also populates BigQuery, or if there's a separate ingestion process).

4.  **Root Cause Analysis:**
    *   Investigate the cause of the failure using Airflow logs, Dataproc logs, and BigQuery job history in Cloud Logging.
    *   Address the identified issues in the Airflow DAG, PySpark script, or BigQuery SQL.
    *   Perform thorough testing in a non-production environment before attempting another deployment.