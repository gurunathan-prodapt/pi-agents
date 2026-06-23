# MIGRATION_NOTES.md

## 1. Summary

The UC4 job `DW.BERT_AUSD_BP_TA_BCP_MSISDN` has been migrated from its legacy environment to Google Cloud Platform (GCP).

*   **Original System:**
    *   **Orchestration:** UC4/Automic
    *   **Scripting:** KornShell (`r_ausd_bp_ta_bcp_msisdn.ksh`, `k_ausd_bp_ta_bcp_msisdn.ksh`)
    *   **Database/SQL:** Oracle (`d_ausd_bp_ta_bcp_msisdn.sql`)
    *   **Host:** Unix host (`DWHDWH2P`)
*   **Target Platform:** Google Cloud Platform (GCP)
    *   **Orchestration:** Apache Airflow on Cloud Composer
    *   **Scripting/Processing:** Python on Google Cloud Dataproc
    *   **Database/SQL:** Google BigQuery

This migration re-platforms the ETL workflow responsible for provisioning and preparing selected basic products related to MSISDN data for the BERT system.

## 2. Generated artifacts

The following files were generated as part of this migration:

*   **`sql/ddl/sof_ta_bpr_bcp.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `sof_ta_bpr_bcp` table. This table serves as a source for the data transformation, replacing the Oracle `sof$ta_bpr_bcp` table.
*   **`sql/ddl/sof_ta_rn_vertrag.sql`**
    *   **Role:** BigQuery DDL script to create the `sof_ta_rn_vertrag` table. This table also serves as a source for the data transformation, replacing the Oracle `sof$ta_rn_vertrag` table.
*   **`sql/ddl/sof_ta_bcp_msisdn.sql`**
    *   **Role:** BigQuery DDL script to create the `sof_ta_bcp_msisdn` table. This is the target table where the transformed data will be loaded, replacing the Oracle `sof$ta_bcp_msisdn` table.
*   **`sql/ddl/dwtk_meldungen.sql`**
    *   **Role:** BigQuery DDL script to create the `dwtk_meldungen` table within the `isbert_schema` dataset. This table is used to retrieve metadata, specifically the `timecreated` for a given `job_kennung`, replacing the Oracle `isbert_schema.dwtk_meldungen` table.
*   **`python/r_ausd_bp_ta_bcp_msisdn.py`**
    *   **Role:** Python script that encapsulates the entire data processing logic. It replaces the functionality of the original KornShell wrapper scripts (`r_ausd_bp_ta_bcp_msisdn.ksh`, `k_ausd_bp_ta_bcp_msisdn.ksh`) and the core Oracle SQL script (`d_ausd_bp_ta_bcp_msisdn.sql`). It handles parameter parsing, retrieves metadata, truncates the target BigQuery table, and executes the BigQuery `INSERT...SELECT` statement. This script is designed to run as a PySpark job on Dataproc.
*   **`airflow/dw_bert_ausd_bp_ta_bcp_msisdn.py`**
    *   **Role:** Apache Airflow DAG definition. This DAG orchestrates the execution of the `r_ausd_bp_ta_bcp_msisdn.py` Python script on a Google Cloud Dataproc cluster. It replaces the UC4 orchestration job (`DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml`).

## 3. Key design decisions

*   **Cloud-Native Orchestration with Airflow:** Apache Airflow on Cloud Composer was chosen to replace UC4 due to its cloud-native capabilities, scalability, robust scheduling, monitoring, and integration with other GCP services. This provides a modern, maintainable, and extensible orchestration layer.
*   **Python on Dataproc for Script Logic:** The KornShell scripts were translated into a single Python script executed on Google Cloud Dataproc. This decision leverages Python's versatility for scripting, data manipulation, and BigQuery client interactions, while Dataproc provides a scalable and managed environment for executing the processing logic, replacing the Unix host environment.
*   **BigQuery as the Data Warehouse:** Oracle tables were migrated to BigQuery. BigQuery offers a serverless, highly scalable, and cost-effective data warehousing solution, which is well-suited for analytical workloads and replaces the traditional Oracle database.
*   **Direct BigQuery Client Interaction:** Instead of using a generic SQL execution wrapper (like `SQL*Plus` in the original), the Python script directly uses the `google-cloud-bigquery` client library. This provides tighter integration, better error handling, and removes the need for external SQL client tools.
*   **Consolidation of Script Logic:** The two KornShell wrapper scripts (`r_ausd_bp_ta_bcp_msisdn.ksh`, `k_ausd_bp_ta_bcp_msisdn.ksh`) and the core Oracle SQL script (`d_ausd_bp_ta_bcp_msisdn.sql`) were consolidated into a single Python script. This reduces complexity, improves readability, and simplifies maintenance by having all related logic in one place.
*   **Removal of Oracle-Specific Constructs:** Oracle-specific hints (`/*+ full(...) parallel(...) */`) and PL/SQL wrappers (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`) were removed. BigQuery's query optimizer automatically handles execution plans and parallelism, and direct BigQuery DDL statements replace the PL/SQL wrapper for operations like `TRUNCATE`. This simplifies the SQL and leverages BigQuery's native capabilities.

**Notable Trade-offs:**

*   **Dataproc Cluster Management:** While Dataproc is managed, it still requires a running cluster (or the overhead of ephemeral cluster creation) for the PySpark job. This introduces a dependency on Dataproc cluster availability and configuration.
*   **Implicit Scheduling:** The original UC4 job XML did not specify a schedule. The Airflow DAG is initially configured with `schedule_interval=None`, meaning it will not run automatically. A specific schedule will need to be defined based on business requirements, or it will rely on external triggers.
*   **Parameter Handling Evolution:** The migration involved translating shell script parameter parsing (`getopts`) to Python's `argparse` and Airflow's `op_args`. This requires careful mapping to ensure all original parameters and their default behaviors are correctly replicated.
*   **`Wiederanlaufwert` Parameter:** The `wiederanlaufwert` parameter is passed to the Python script for compatibility but is currently unused in the generated code. If this parameter had functional significance in specific scenarios in the original job, its logic would need to be implemented in the Python script.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps and configurations are required:

1.  **BigQuery Dataset Creation:**
    *   Create the BigQuery datasets: `your_bigquery_dataset` (for data tables) and `isbert_schema` (for metadata tables) in your target GCP project.
2.  **BigQuery DDL Deployment:**
    *   Execute the generated DDL scripts (`sql/ddl/*.sql`) in BigQuery to create the necessary tables:
        *   `sof_ta_bpr_bcp`
        *   `sof_ta_rn_vertrag`
        *   `sof_ta_bcp_msisdn`
        *   `isbert_schema.dwtk_meldungen`
3.  **IAM Permissions:**
    *   Ensure the Cloud Composer service account (or the service account used by Dataproc) has the following minimum IAM roles:
        *   `BigQuery Data Editor` (to read from source tables and write/truncate the target table).
        *   `Dataproc Worker` (to run jobs on Dataproc).
        *   `Storage Object Viewer` and `Storage Object Creator` (to read the Python script from GCS and write logs/temporary files).
4.  **GCS Upload of Python Script:**
    *   Upload the `python/r_ausd_bp_ta_bcp_msisdn.py` script to the specified Google Cloud Storage (GCS) path, e.g., `gs://your-gcs-bucket/pyspark_scripts/r_ausd_bp_ta_bcp_msisdn.py`.
5.  **Dataproc Cluster Configuration:**
    *   Ensure a Dataproc cluster named `your-dataproc-cluster-name` is provisioned and running in `your-gcp-region`. The Airflow DAG is configured to submit jobs to this cluster.
6.  **Airflow DAG Configuration:**
    *   **Replace Placeholders:** Update the `airflow/dw_bert_ausd_bp_ta_bcp_msisdn.py` DAG file with your specific GCP project ID, region, Dataproc cluster name, GCS bucket path, and BigQuery dataset names.
        *   `GCP_PROJECT_ID = "your-gcp-project-id"`
        *   `GCP_REGION = "your-gcp-region"`
        *   `DATAPROC_CLUSTER_NAME = "your-dataproc-cluster-name"`
        *   `PYTHON_SCRIPT_GCS_PATH = "gs://your-gcs-bucket/pyspark_scripts/r_ausd_bp_ta_bcp_msisdn.py"`
        *   `BIGQUERY_DATASET_NAME = "your_bigquery_dataset"`
        *   `METADATA_DATASET_NAME = "isbert_schema"`
    *   **Scheduling:** The `schedule_interval` is currently `None`. If the job needs to run on a schedule (e.g., daily, weekly), update this parameter in the DAG file (e.g., `schedule_interval='@daily'`).
7.  **Initial Data Load (Historical Data):**
    *   Perform a one-time migration of historical data from the Oracle source tables (`sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, `sof$ta_bcp_msisdn`, `isbert_schema.dwtk_meldungen`) to their corresponding BigQuery tables. This is crucial for functional testing and ensuring data availability in the new environment.

## 5. Known gaps & unresolved references

The following items have been identified as known gaps or require further attention:

*   **Scheduler Configuration:** The original UC4 job XML did not specify a schedule. The Airflow DAG is currently configured with `schedule_interval=None`. A definitive schedule (e.g., daily, weekly, or event-driven) for the Airflow DAG needs to be established and configured based on business requirements.
*   **Environment Variables & Configuration:** The exact details of environment variables set by `. $HOME/.dw_init` in the original KornShell environment, and how `BERT_DIR_ROOT` was resolved, need to be fully understood. These configurations (if critical) will need to be managed through Airflow Variables, environment variables on Dataproc, or configuration files within the Python script.
*   **Parameter Default Values and Edge Cases:** While parameter parsing is implemented in Python, a thorough review of all default values, validation rules, and error conditions from the original KornShell scripts is required to ensure accurate replication in the Python implementation.
*   **Logging and Alerting Integration:** The legacy `DWMSG_...` logging and `trap` mechanisms have been replaced by Python's standard `logging` module, which integrates with Cloud Logging. However, specific alerting rules based on log patterns or job failures in Cloud Monitoring need to be configured to maintain or improve operational visibility.
*   **Completeness of Job Definition:** The migration focused solely on the provided UC4 job as a standalone entity. If this job was part of a larger UC4 workflow with upstream or downstream dependencies not captured in the provided XML, those dependencies would need to be identified and integrated into the Airflow DAG or a broader Airflow ecosystem.
*   **`Wiederanlaufwert` Parameter:** The `wiederanlaufwert` parameter is passed to the Python script but is currently unused. If this parameter had specific restart or recovery logic in the original job, that logic needs to be implemented in the Python script.
*   **Placeholder Replacement:** All `your-gcp-project-id`, `your-gcp-region`, `your-dataproc-cluster-name`, `your-gcs-bucket`, and `your_bigquery_dataset` placeholders in the generated code must be replaced with actual environment-specific values before deployment.

## 6. Validation

To validate the successful migration and functionality of the `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job:

1.  **Deployment:**
    *   Ensure all manual steps (Section 4) are completed.
    *   Deploy the BigQuery DDLs.
    *   Upload the `r_ausd_bp_ta_bcp_msisdn.py` Python script to the designated GCS bucket.
    *   Deploy the `dw_bert_ausd_bp_ta_bcp_msisdn.py` Airflow DAG to Cloud Composer.
2.  **Data Preparation:**
    *   Load a representative set of sample data or historical data into the BigQuery source tables (`sof_ta_bpr_bcp`, `sof_ta_rn_vertrag`, `isbert_schema.dwtk_meldungen`).
    *   Ensure the `isbert_schema.dwtk_meldungen` table contains an entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with a `timecreated` value if the `stichtag` parameter is not explicitly passed.
3.  **Execution:**
    *   Trigger the `dw_bert_ausd_bp_ta_bcp_msisdn` Airflow DAG manually from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI and the Dataproc job in the GCP Console.
4.  **Verification (What "passing" means):**
    *   **Airflow DAG Status:** The `dw_bert_ausd_bp_ta_bcp_msisdn` DAG run completes successfully with all tasks marked as "success".
    *   **Dataproc Job Status:** The Dataproc job submitted by the DAG completes successfully without errors.
    *   **Cloud Logging:** Review Cloud Logging for the Dataproc job and Airflow tasks. There should be no error messages or unexpected warnings. Log messages should indicate successful truncation and insertion of data.
    *   **BigQuery Data Validation:**
        *   Query the target table `your_gcp_project.your_bigquery_dataset.sof_ta_bcp_msisdn` to confirm it has been truncated and repopulated.
        *   Verify the row count in `sof_ta_bcp_msisdn` matches the expected output from the original Oracle job for the same input data.
        *   Perform data integrity checks:
            *   Spot-check a sample of `CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, and `TN_TEL_MSISDN` values against the original Oracle output.
            *   Ensure `DISTINCT` logic is correctly applied.
            *   Validate that `CNTRCT_ID_REF` in `sof_ta_bpr_bcp` correctly joins with `CNTRCT_ID` in `sof_ta_rn_vertrag`.
    *   **Performance:** Compare the execution time of the migrated job with the original UC4/Oracle job to ensure performance is acceptable or improved.

## 7. Rollback procedure

In case of critical issues or failure during or after go-live, the following rollback procedure can be followed:

1.  **Immediate Action (Stop New Runs):**
    *   **Airflow:** Pause or delete the `dw_bert_ausd_bp_ta_bcp_msisdn` Airflow DAG in the Cloud Composer UI to prevent further executions.
    *   **Dataproc:** If a Dataproc job is still running, attempt to cancel it from the Dataproc Jobs page in the GCP Console.
2.  **Re-enable Original System:**
    *   Re-enable the original UC4 job `DW.BERT_AUSD_BP_TA_BCP_MSISDN` in the UC4/Automic scheduler.
3.  **Data Recovery (if necessary):**
    *   **Target Table:** If the `sof_ta_bcp_msisdn` table in BigQuery was corrupted or incorrectly populated, and if a backup or snapshot mechanism is in place (e.g., BigQuery time travel or table snapshots), restore the table to its state before the problematic run.
    *   **Oracle Data:** If the original Oracle job needs to be re-run to correct data, ensure its source tables are in a consistent state.
4.  **Code Reversion (if necessary):**
    *   If the issue was due to a bug in the Airflow DAG or Python script, revert the deployed `airflow/dw_bert_ausd_bp_ta_bcp_msisdn.py` and/or `python/r_ausd_bp_ta_bcp_msisdn.py` files to a previous stable version or remove them from GCS/Composer.
5.  **Verification:**
    *   After re-enabling the original system, verify that it runs successfully and produces correct data in the Oracle environment.
    *   Confirm that no further runs of the migrated job are occurring in GCP.