# MIGRATION_NOTES.md: DW.BERT_AUSD_V_TA_P_VERTRAG

## 1. Summary

The `DW.BERT_AUSD_V_TA_P_VERTRAG` job, originally implemented using UC4 for orchestration, KornShell scripts for control, and Oracle SQL*Plus for data transformation against an Oracle database, has been migrated to Google Cloud Platform (GCP).

The core functionality of updating contract information, specifically "twin-bill" contracts, remains the same. Data is processed from a temporary staging table and used to populate a primary contract table.

The target platform utilizes:
*   **Google Cloud Composer (Apache Airflow)** for job orchestration.
*   **Google BigQuery** for data storage and transformation.
*   **Python** for scripting logic that previously resided in KornShell.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`sql/d_ausd_v_ta_p_vertrag_bq.sql`**
    *   **Role:** This file contains the core data transformation logic, translated from the original Oracle SQL*Plus script (`d_ausd_v_ta_p_vertrag.sql`) into BigQuery Standard SQL. It handles determining a key date, truncating the target contract table (`sof_ta_p_vertrag_bq`), inserting/updating "twin-bill" contract data, and truncating numerous temporary staging tables.
    *   **Location:** Intended to be accessible by the Airflow DAG, either embedded directly within the DAG file or stored in a location like a GCS bucket. For this migration, it's embedded in the DAG for self-containment.

*   **`python/p_vertrag_processor.py`**
    *   **Role:** This Python script encapsulates the wrapper and control logic previously found in `r_ausd_v_ta_p_vertrag.ksh` and `k_ausd_v_ta_p_vertrag.ksh`. It serves as a logical entry point for the Airflow DAG, handling environment setup, parameter parsing (if any were defined), and logging. In this specific migration, its primary role is to provide a Python-based task for Airflow and to log the start/end of the processing phase.
    *   **Location:** Should be deployed to the Cloud Composer environment, typically in a custom plugin folder or directly within the DAGs folder if it's a simple utility.

*   **`dags/dw_bert_ausd_v_ta_p_vertrag_dag.py`**
    *   **Role:** This is the Apache Airflow DAG definition file. It orchestrates the entire workflow, replacing the original UC4 job definition. It defines two main tasks: a Python task to execute `p_vertrag_processor.py` and a BigQuery task to execute the `d_ausd_v_ta_p_vertrag_bq.sql` content.
    *   **Location:** Must be deployed to the `dags/` folder of the Cloud Composer environment.

## 3. Key Design Decisions

*   **Cloud-Native Architecture:** The decision to migrate to BigQuery and Cloud Composer leverages GCP's managed, scalable, and cost-effective services, aligning with modern data warehousing practices.
*   **BigQuery for Data Storage and Transformation:** BigQuery was chosen to replace Oracle for all permanent and temporary tables. Its serverless nature, columnar storage, and powerful SQL engine are well-suited for analytical workloads and large datasets.
    *   **SQL Conversion:** Oracle-specific syntax (e.g., `NVL`, `(+)` for `LEFT JOIN`, `/*+ parallel */` hints, `WHENEVER SQLERROR`, `COMMIT`) was carefully translated to BigQuery Standard SQL. `TRUNCATE TABLE` statements were directly mapped.
*   **Python for Scripting Logic:** KornShell scripts were re-written in Python. This provides a more modern, maintainable, and widely supported language for control flow, parameter handling, and integration with GCP services.
*   **Cloud Composer (Airflow) for Orchestration:** Airflow was selected to replace UC4 due to its open-source nature, robust scheduling capabilities, rich set of operators for GCP services, and Python-based DAG definitions, offering greater flexibility and visibility into workflows.
*   **Embedded SQL in DAG:** For simplicity and self-containment, the BigQuery SQL content is directly embedded within the Airflow DAG file. This reduces external dependencies for deployment but might make SQL changes require a DAG redeployment.
*   **Handling Oracle Utilities:** Custom Oracle PL/SQL procedures (`DWPA_UTIL_SKRIPT.runstatement`) and KornShell utilities (`f_alis_msgerr.ksh`, etc.) were replaced by native BigQuery DDL commands (for truncations) and Python's standard logging module, integrated with Airflow's logging capabilities.

## 4. Manual Steps Before Go-Live

To ensure a successful go-live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Create the target BigQuery dataset, e.g., `your_bigquery_dataset`, in the specified GCP project (`your-gcp-project`).
    *   Ensure the dataset is created in the correct geographical `location` (e.g., `EU` as specified in the DAG).

2.  **BigQuery Table Schema Creation:**
    *   Create all necessary BigQuery tables with appropriate schemas, mapping Oracle data types to BigQuery data types. These include:
        *   `your-gcp-project.your_bigquery_dataset.dwtk_meldungen_bq`
        *   `your-gcp-project.your_bigquery_dataset.sof_ta_vertrag_tmp_bq`
        *   `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_bq` (the main target table)
        *   All other temporary `sof_ta_*_bq` tables listed in the `TRUNCATE` statements within `d_ausd_v_ta_p_vertrag_bq.sql`.
    *   **Note:** Careful consideration of data type mapping (e.g., Oracle `NUMBER` to BigQuery `INT64`, `NUMERIC`, or `FLOAT64`) is crucial.

3.  **IAM Permissions:**
    *   Grant the Cloud Composer service account (or the service account used by the Airflow worker) the necessary BigQuery permissions:
        *   `BigQuery Data Editor` on `your_bigquery_dataset` to allow data manipulation (INSERT, TRUNCATE).
        *   `BigQuery Data Viewer` on `your_bigquery_dataset` for reading data.
        *   `BigQuery Job User` to run BigQuery jobs.
    *   Ensure the service account has permissions to deploy and manage DAGs in the Composer environment.

4.  **Airflow Connections:**
    *   Verify that the `google_cloud_default` connection is correctly configured in your Airflow environment. This connection is used by the `BigQueryExecuteQueryOperator`.

5.  **Data Ingestion Pipelines:**
    *   **Crucial Prerequisite:** Establish and verify data ingestion pipelines to populate the BigQuery source tables from their original Oracle sources.
        *   `dwtk_meldungen_bq` (from `isbert_schema.dwtk_meldungen`)
        *   `sof_ta_vertrag_tmp_bq` (from `sof$ta_vertrag_tmp`)
    *   If the `PCRS1` Oracle DB Link (mentioned in the design document) is confirmed as the source for `sof$ta_vertrag_tmp` or other inputs, a dedicated ingestion pipeline from `PCRS1` to BigQuery must be implemented and operational.

6.  **Code Deployment:**
    *   Deploy `python/p_vertrag_processor.py` to a suitable location within the Cloud Composer environment (e.g., a custom plugin folder or a directory accessible by Airflow tasks).
    *   Upload `dags/dw_bert_ausd_v_ta_p_vertrag_dag.py` to the `dags/` folder of your Cloud Composer environment.

7.  **Scheduling Configuration:**
    *   Update the `schedule=None` parameter in `dags/dw_bert_ausd_v_ta_p_vertrag_dag.py` to the desired production schedule (e.g., `schedule="@daily"` or a specific cron expression).

## 5. Known Gaps & Unresolved References

*   **External System `PCRS1` Dependency (B4 Item):** The exact nature and criticality of the `PCRS1` Oracle DB link dependency require further investigation. While `sof$ta_vertrag_tmp` is the direct input to this job, its ultimate source might be `PCRS1`. A robust data ingestion pipeline from `PCRS1` to BigQuery must be designed and implemented if it's a source for any input tables. This is a significant follow-up item.
*   **Parameter Passing Refinement:** The original KornShell scripts used `getopts` for parameter parsing. While the Python wrapper `p_vertrag_processor.py` is prepared for arguments, no specific parameters were identified from the design document for direct translation. If the original UC4 job passed dynamic parameters, this aspect of the Python script and DAG will need further refinement to accept and utilize those parameters.
*   **Historical Data Migration (B4 Item):** The migration design focuses on the job's operational logic. The process for migrating existing historical data from the Oracle `sof$ta_p_vertrag` table (and other relevant tables) to their BigQuery counterparts (`sof_ta_p_vertrag_bq`, etc.) has not been explicitly defined. This is a critical B4 item to ensure data continuity.
*   **Oracle Utility Script Equivalents:** The original KornShell scripts relied on several utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). While basic logging and error handling are covered by Python's standard library and Airflow's features, any highly specific custom logic within these utilities might need to be re-implemented in Python or as Airflow components.
*   **Data Type Mapping Validation:** While schemas are created, subtle differences in data type behavior (e.g., precision, scale, null handling) between Oracle and BigQuery might exist. Thorough validation is required.

## 6. Validation

Validation of the migrated job involves several steps to ensure functional equivalence and data integrity:

1.  **Unit Testing (Python Script):**
    *   If `p_vertrag_processor.py` were to contain complex logic, unit tests would be developed to verify its individual functions. For this migration, its role is primarily a wrapper, so extensive unit testing might not be strictly necessary unless further logic is added.

2.  **Integration Testing (Airflow DAG & BigQuery SQL):**
    *   **Data Preparation:** Load a representative sample of source data into the BigQuery staging tables (`dwtk_meldungen_bq`, `sof_ta_vertrag_tmp_bq`) that mirrors the data used in a successful Oracle run.
    *   **DAG Execution:** Trigger the `dw_bert_ausd_v_ta_p_vertrag_dag` in the Cloud Composer environment.
    *   **Log Review:** Monitor the Airflow task logs and Cloud Logging for any errors, warnings, or unexpected behavior.
    *   **Data Verification:**
        *   Query the target table `sof_ta_p_vertrag_bq` after the DAG completes.
        *   Compare row counts in `sof_ta_p_vertrag_bq` with the corresponding Oracle `sof$ta_p_vertrag` table for the same input data.
        *   Perform aggregate checks (e.g., `SUM()`, `AVG()`, `COUNT(DISTINCT column)`) on key numeric or categorical columns in `sof_ta_p_vertrag_bq` and compare them against the Oracle output.
        *   Spot-check specific records to ensure data transformation logic is correctly applied.
        *   Verify that all temporary `sof_ta_*_bq` tables are truncated as expected.

3.  **"Passing" Criteria:**
    *   The `dw_bert_ausd_v_ta_p_vertrag_dag` completes successfully without any failed tasks.
    *   No errors or critical warnings are reported in Airflow logs or Cloud Logging.
    *   The data in `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_bq` is functionally identical to the data produced by the original Oracle job for the same input, as confirmed by row counts, aggregate checks, and sample record comparisons.
    *   All temporary tables listed in the `TRUNCATE` statements are empty after the job execution.

## 7. Rollback Procedure

In the event of critical issues or data corruption after go-live, the following rollback procedure can be initiated:

1.  **Disable New Job:**
    *   In Cloud Composer, disable the `dw_bert_ausd_v_ta_p_vertrag_dag` Airflow DAG.

2.  **Re-enable Original Job:**
    *   Re-enable the original `DW.BERT_AUSD_V_TA_P_VERTRAG` job in UC4.

3.  **Data Restoration (BigQuery):**
    *   If `sof_ta_p_vertrag_bq` (or any other critical BigQuery table) has been corrupted or contains incorrect data due to the new job, use BigQuery's time travel feature to restore the table to a state before the problematic run.
        *   Example: `CREATE OR REPLACE TABLE your_bigquery_dataset.sof_ta_p_vertrag_bq AS SELECT * FROM your_bigquery_dataset.sof_ta_p_vertrag_bq FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);` (Adjust interval as needed).
    *   Alternatively, if the original Oracle job is still running and producing correct data, re-run the Oracle job and then re-ingest the data from Oracle to BigQuery to repopulate the target tables.

4.  **Code Removal (Optional):**
    *   If the issue is severe and requires significant re-work, consider removing the `dw_bert_ausd_v_ta_p_vertrag_dag.py` and `p_vertrag_processor.py` files from the Cloud Composer environment to prevent accidental re-execution.

5.  **Investigate and Rectify:**
    *   Analyze logs, compare data, and identify the root cause of the issue. Apply necessary fixes to the BigQuery SQL, Python scripts, or DAG definition before attempting re-deployment.