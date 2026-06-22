# MIGRATION_NOTES: DW.BERT_AUSD_BP_TA_RN_VERTRAG

## 1. Summary

The ETL job `DW.BERT_AUSD_BP_TA_RN_VERTRAG`, responsible for the "preparation of instantiated basic products" by processing contract-related data to consolidate phone numbers and their statuses, has been migrated.

**Original Platform:** UC4/KornShell/Oracle
**Target Platform:** Google Cloud Platform (GCP) utilizing:
*   **Orchestration:** Apache Airflow on Cloud Composer
*   **Processing:** PySpark/Python scripts executed on Google Cloud Dataproc
*   **Data Warehousing:** Google BigQuery

The migration involved translating UC4 scheduling to an Airflow DAG, converting KornShell scripts to Python, and adapting Oracle SQL transformation logic to BigQuery SQL.

## 2. Generated Artifacts

The migration produced the following key artifacts:

*   **`src/dataproc/dataproc_job.py`**
    *   **Role:** This Python script serves as the core execution logic for the job. It replaces the functionality of the original `r_ausd_bp_ta_rn_vertrag.ksh`, `k_ausd_bp_ta_rn_vertrag.ksh`, and various utility KornShell scripts (`gestern.ksh`, `h_alis_date.ksh`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`). It handles parameter parsing (`Stichtag`, `Wiederanlaufwert`), date calculations, logging, and orchestrates the execution of the BigQuery SQL transformation. It is designed to be submitted as a PySpark job to a Dataproc cluster.
*   **`dags/dw_bert_ausd_bp_ta_rn_vertrag_dag.py`**
    *   **Role:** This is the Airflow DAG definition file. It replaces the UC4 job definition (`DW.BERT_AUSD_BP_TA_RN_VERTRAG.xml`) for scheduling and orchestration. It defines the workflow, including a `DataprocSubmitPySparkJobOperator` task to launch `dataproc_job.py` on a Dataproc cluster, passing necessary parameters.
*   **`sql/d_ausd_bp_ta_rn_vertrag_bq.sql` (Implicitly generated/translated)**
    *   **Role:** This file (not explicitly provided in the generated code but referenced by `dataproc_job.py`) contains the translated BigQuery SQL transformation logic. It is derived from the original `d_ausd_bp_ta_rn_vertrag.sql` and performs the truncation and population of the target `isbert_schema.SOF_TA_RN_VERTRAG` table using data from `isbert_schema.SOF_TA_RN_EINZELN`. This SQL script is read and executed by `dataproc_job.py`.

## 3. Key Design Decisions

*   **Airflow for Orchestration:** Cloud Composer (managed Airflow) was chosen to replace UC4 due to its native integration with GCP services, robust scheduling capabilities, and Python-based DAG definitions, offering greater flexibility and maintainability.
*   **Dataproc for Execution Environment:** Dataproc provides a managed Spark/Hadoop environment, suitable for executing Python/PySpark scripts. This allows for scalable processing and avoids managing underlying infrastructure, replacing the KornShell execution environment.
*   **Python/PySpark for KornShell Logic:** The complex orchestration, parameter handling, and utility functions from the KornShell scripts were translated into a single Python script (`dataproc_job.py`). This decision leverages Python's readability, extensive libraries (e.g., `datetime`, `logging`), and better integration with GCP client libraries, improving maintainability and testability compared to direct shell script migration.
*   **BigQuery for Data Transformation and Storage:** BigQuery was selected as the target data warehouse, replacing Oracle. Its serverless architecture, columnar storage, and high-performance SQL engine are well-suited for the analytical transformations performed by this job. Oracle-specific SQL hints (e.g., `FULL`, `PARALLEL`) were removed as BigQuery automatically optimizes query execution.
*   **Parameter Handling in Python:** Job parameters like `Stichtag` and `Wiederanlaufwert` are now parsed and validated within the Python script, providing more robust error handling and type checking than shell scripts. The `Stichtag` can be dynamically passed from Airflow using macros.
*   **External Data Ingestion Strategy:** A separate, continuous data ingestion pipeline (e.g., using DataStream for CDC or Cloud Data Fusion for batch loading) is assumed for bringing `SOF$TA_RN_EINZELN` and `DWTK_MELDUNGEN` from Oracle into BigQuery. This decouples the source data availability from the transformation job itself, aligning with modern data warehousing practices.
*   **Logging and Error Handling:** Python's standard `logging` module replaces the custom KornShell error handling (`f_alis_msgerr.ksh`), integrating seamlessly with Cloud Logging for centralized monitoring.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps and configurations are required:

1.  **BigQuery Dataset and Table Creation:**
    *   Create the `isbert_schema` BigQuery dataset in your target GCP project.
    *   Create the target table `isbert_schema.SOF_TA_RN_VERTRAG` in BigQuery. The schema should be derived from the Oracle `SOF$TA_RN_VERTRAG` table and the `INSERT` statement in `d_ausd_bp_ta_rn_vertrag_bq.sql`.
    *   Ensure the source tables `isbert_schema.SOF_TA_RN_EINZELN` and `isbert_schema.DWTK_MELDUNGEN` exist in BigQuery with schemas matching their Oracle counterparts.
2.  **Oracle Source Data Ingestion:**
    *   Implement and configure a data ingestion pipeline (e.g., using Cloud Data Fusion, DataStream, or a custom batch load) to continuously or regularly replicate data from the Oracle source tables (`SOF$TA_RN_EINZELN`, `DWTK_MELDUNGEN`) into their respective BigQuery tables (`isbert_schema.SOF_TA_RN_EINZELN`, `isbert_schema.DWTK_MELDUNGEN`). This pipeline must meet the required data freshness SLAs.
3.  **GCP Project and Resource Configuration:**
    *   **Update Placeholders:** In `dags/dw_bert_ausd_bp_ta_rn_vertrag_dag.py`, replace `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, and `YOUR_BUCKET_NAME` with actual values for your GCP environment.
    *   **Dataproc Cluster:** Ensure a Dataproc cluster (named `YOUR_DATAPROC_CLUSTER_NAME` in `YOUR_DATAPROC_REGION`) is provisioned and running, or configure the DAG to create an ephemeral cluster if preferred.
    *   **GCS Bucket:** Create the GCS bucket specified by `YOUR_BUCKET_NAME`.
4.  **Upload Artifacts to GCS:**
    *   Upload `src/dataproc/dataproc_job.py` to `gs://YOUR_BUCKET_NAME/dataproc/dataproc_job.py`.
    *   Upload the translated BigQuery SQL script (`d_ausd_bp_ta_rn_vertrag_bq.sql`) to `gs://YOUR_BUCKET_NAME/sql/d_ausd_bp_ta_rn_vertrag_bq.sql`.
5.  **IAM Permissions:**
    *   **Cloud Composer Service Account:** Grant the Composer service account (used by the Airflow DAG) the necessary permissions:
        *   `Dataproc Editor` or specific roles to submit jobs to Dataproc.
        *   `Storage Object Viewer` for reading scripts from GCS.
        *   `BigQuery Data Editor` for truncating and inserting data into `isbert_schema.SOF_TA_RN_VERTRAG`.
        *   `BigQuery Data Viewer` for reading from `isbert_schema.SOF_TA_RN_EINZELN` and `isbert_schema.DWTK_MELDUNGEN`.
    *   **Dataproc Worker Service Account:** Grant the service account used by the Dataproc cluster workers the necessary permissions:
        *   `Storage Object Viewer` for reading scripts from GCS.
        *   `BigQuery Data Editor` for truncating and inserting data into `isbert_schema.SOF_TA_RN_VERTRAG`.
        *   `BigQuery Data Viewer` for reading from `isbert_schema.SOF_TA_RN_EINZELN` and `isbert_schema.DWTK_MELDUNGEN`.
6.  **Airflow DAG Scheduling:**
    *   The DAG is currently configured with `schedule=None`. Based on business requirements, update the `schedule` parameter in `dags/dw_bert_ausd_bp_ta_rn_vertrag_dag.py` to define the desired execution frequency (e.g., `schedule="@daily"`).

## 5. Known Gaps & Unresolved References

*   **UC4 Schedule Definition:** The exact scheduling frequency and dependencies of the original UC4 job (`DW.BERT_AUSD_BP_TA_RN_VERTRAG.xml`) were not fully specified in the design document. The Airflow DAG's `schedule` parameter needs to be manually configured based on the business's operational requirements.
*   **UC4 Includes (`DW.BERT_LESE_LOG`, `DW.HOLE_PFAD`):**
    *   `DW.BERT_LESE_LOG` is currently represented by a `DummyOperator` (`log_completion`). Its specific logging, monitoring, or post-processing actions in the legacy system need further investigation to determine if a more robust GCP-native implementation (e.g., Cloud Monitoring metrics, BigQuery logging table) is required.
    *   `DW.HOLE_PFAD` is an `:inc` statement without an explicit file path in the provided UC4 XML. Its purpose and functionality remain unclear and should be verified if it impacts the job's execution or environment setup.
*   **Commented-Out KornShell Logic:** The `sed`, `sort`, and `join` commands commented out in `k_ausd_bp_ta_rn_vertrag.ksh` were not migrated. If these represent inactive but potentially reactivatable post-processing or data merging steps, their future requirement should be noted.
*   **Oracle `DWPA_UTIL_SKRIPT.runstatement`:** While assumed to be a wrapper for `TRUNCATE TABLE`, any additional logic within this Oracle procedure would need to be identified and migrated if it extends beyond simple table truncation.
*   **Data Type and Implicit Conversion Differences:** Oracle and BigQuery have distinct data type systems and implicit conversion rules. A thorough review and testing of the translated `d_ausd_bp_ta_rn_vertrag_bq.sql` is crucial to ensure data integrity and prevent unexpected behavior due to type mismatches or conversion errors.
*   **`v_datum` from `DWTK_MELDUNGEN`:** The legacy SQL script calculates `v_datum` from `DWTK_MELDUNGEN` but doesn't appear to use it in the main `INSERT` statement. The Python script includes a call to `_get_last_timecreated_from_dwtk_meldungen` for logging purposes, but if `v_datum` has a functional impact not immediately apparent, this needs further investigation.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to Run the Tests:**

1.  **Manual Trigger:** Access the Airflow UI (Cloud Composer) for the `dw_bert_ausd_bp_ta_rn_vertrag` DAG. Manually trigger a run.
2.  **Scheduled Run:** Once the `schedule` is configured, allow the DAG to run at its designated time.
3.  **Parameter Testing:** If `Stichtag` or `Wiederanlaufwert` are critical, test with various valid and invalid inputs (e.g., different dates, edge cases for `Wiederanlaufwert`) by modifying the `arguments` in the `DataprocSubmitPySparkJobOperator` or by using Airflow's "Trigger DAG with config" feature.

**What "Passing" Means:**

A successful validation run implies the following:

1.  **Airflow DAG Success:** The `dw_bert_ausd_bp_ta_rn_vertrag` DAG completes successfully in the Airflow UI, with all tasks (including `run_bert_aggregation_dataproc`) showing a "success" status.
2.  **Dataproc Job Success:** The underlying Dataproc job initiated by Airflow completes without errors (exit code 0). This can be verified in the Dataproc Jobs UI or by reviewing Cloud Logging.
3.  **BigQuery Table State:**
    *   The `isbert_schema.SOF_TA_RN_VERTRAG` table is successfully truncated before new data insertion.
    *   The `isbert_schema.SOF_TA_RN_VERTRAG` table is populated with data.
4.  **Data Accuracy (Critical):**
    *   **Row Count Comparison:** Compare the row count of the `isbert_schema.SOF_TA_RN_VERTRAG` table in BigQuery with the corresponding `SOF$TA_RN_VERTRAG` table in the legacy Oracle system for the same `Stichtag` and source data.
    *   **Data Content Comparison:** Perform a detailed data comparison (e.g., using checksums, hash values, or direct row-by-row comparison for a sample set) between the BigQuery output and the Oracle output. This is crucial to ensure the `MAX()` aggregations and other transformations yield identical results.
    *   **Schema Validation:** Verify that the schema of `isbert_schema.SOF_TA_RN_VERTRAG` matches the expected schema, including data types and nullability.
5.  **Logging and Monitoring:**
    *   Review Cloud Logging for the Dataproc job and Airflow tasks. Ensure there are no unexpected errors, warnings, or stack traces.
    *   Verify that relevant informational messages (e.g., `Stichtag` used, job start/end) are present.

## 7. Rollback Procedure

In case of issues during or after go-live, the following rollback procedure can be executed:

1.  **Disable Airflow DAG:** Immediately disable the `dw_bert_ausd_bp_ta_rn_vertrag` DAG in the Airflow UI to prevent further executions.
2.  **Delete Airflow DAG (Optional):** If the issues are severe or require significant rework, delete the DAG from the Composer environment.
3.  **Revert to Legacy UC4 Job:** Reactivate and re-enable the original `DW.BERT_AUSD_BP_TA_RN_VERTRAG` job in the UC4 system.
4.  **Verify Oracle Target Table:** Ensure the `SOF$TA_RN_VERTRAG` table in the Oracle environment is in a consistent and correct state. If the BigQuery migration testing inadvertently affected the Oracle table (unlikely if proper isolation was maintained), restore it from the latest backup.
5.  **BigQuery Data Restoration (If Necessary):** If the `isbert_schema.SOF_TA_RN_VERTRAG` table in BigQuery was corrupted or incorrectly populated during testing, it can be restored:
    *   **Time Travel:** Utilize BigQuery's time travel feature to query data as it was at a specific point in time before the erroneous run.
    *   **Table Snapshots/Copies:** If snapshots or copies were taken before the migration, restore the table from one of these.
    *   **Re-run Legacy Job (if applicable):** If the legacy job can write to BigQuery (e.g., during a parallel run phase), re-run the legacy job to repopulate the BigQuery table.
6.  **Post-Rollback Monitoring:** Monitor both the legacy UC4 job and the BigQuery environment to ensure stability and data integrity after the rollback.