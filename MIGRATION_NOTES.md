# MIGRATION_NOTES.md for DW.BERT_AUSD_BP_TA_TARIFOPTION

## 1. Summary

The legacy ETL job `DW.BERT_AUSD_BP_TA_TARIFOPTION`, responsible for preparing "instantiated basic products" for tariff options, has been migrated.

*   **Original Platform**: UC4/Automic for orchestration, KornShell scripts for control logic, and Oracle SQL for data transformation and storage.
*   **Target Platform**: Google Cloud Platform (GCP), utilizing Cloud Composer (Apache Airflow) for orchestration, PySpark on Dataproc for control and execution, and BigQuery for data storage and transformation.

The migration re-platforms the entire process, translating the orchestration, shell scripting, and Oracle SQL logic into their respective GCP equivalents.

## 2. Generated artifacts

The migration process generated the following files:

*   **`src/bigquery/udfs/isbert_schema.concat_placeholder_udf.sql`**
    *   **Role**: This file defines a placeholder BigQuery SQL User Defined Function (UDF) named `concat_placeholder_udf` within the `isbert_schema` dataset. It is intended to replace the custom Oracle `sof$ab_con.concatX` functions. **Note**: This is currently a placeholder and requires the actual logic from the original Oracle functions to be implemented.

*   **`src/bigquery/sql/d_ausd_bp_ta_tarifoption.sql`**
    *   **Role**: This BigQuery SQL script contains the core ETL transformation logic. It is a direct translation of the original `d_ausd_bp_ta_tarifoption.sql` Oracle script. It handles:
        *   Declaring and setting the `v_datum` variable based on `isbert_schema.dwtk_meldungen`.
        *   Dropping existing target tables (`sof_ta_bpr_opt_filter`, `sof_ta_tarifoption`).
        *   Creating and populating the intermediate table `sof_ta_bpr_opt_filter` using `EXECUTE IMMEDIATE FORMAT` for dynamic table naming.
        *   Creating and populating the final target table `sof_ta_tarifoption`, including the use of the placeholder UDF and BigQuery equivalents of Oracle functions (`LEAD`, `RTRIM`, `LTRIM`, `SUBSTR`, `CASE`).

*   **`src/pyspark/r_ausd_bp_ta_tarifoption_main.py`**
    *   **Role**: This PySpark script serves as the main control and execution component, replacing the `r_ausd_bp_ta_tarifoption.ksh` and `k_ausd_bp_ta_tarifoption.ksh` KornShell scripts. Its responsibilities include:
        *   Parsing command-line arguments for `stichtag` and `wiederanlaufwert`.
        *   Configuring logging.
        *   Providing a function to determine `v_datum` (though the SQL script now handles this internally).
        *   Executing the `d_ausd_bp_ta_tarifoption.sql` BigQuery script using the `google-cloud-bigquery` client library.
        *   Handling error trapping and logging for the overall job execution.

*   **`src/dags/dw_bert_ausd_bp_ta_tarifoption.py`**
    *   **Role**: This file defines the Apache Airflow DAG (`dw_bert_ausd_bp_ta_tarifoption`) for Cloud Composer. It orchestrates the entire job by:
        *   Defining the DAG's metadata (schedule, start date, tags, default arguments).
        *   Using a `DataprocSubmitJobOperator` to launch the `r_ausd_bp_ta_tarifoption_main.py` PySpark script on a Dataproc cluster.
        *   Passing Airflow macros (`{{ ds }}`, `{{ dag_run.run_id }}`) as arguments to the PySpark script for `stichtag` and `wiederanlaufwert`.
        *   Specifying the GCS paths for the PySpark script and the BigQuery SQL template.

## 3. Key design decisions

*   **Orchestration Re-platforming (UC4 to Cloud Composer)**: Cloud Composer (managed Airflow) was chosen for its robust scheduling capabilities, Python-based extensibility, and native integration with other GCP services, providing a modern and scalable orchestration layer compared to UC4.
*   **Control Logic Re-platforming (KornShell to PySpark on Dataproc)**: The KornShell scripts' responsibilities (parameter parsing, environment setup, logging, SQL execution) were migrated to a PySpark script. This leverages Dataproc for managed Spark execution, allowing for scalable and Python-native handling of control flow, and direct interaction with BigQuery via its client library.
*   **Data Transformation Re-platforming (Oracle SQL to BigQuery SQL)**: BigQuery was selected as the target data warehouse due to its serverless architecture, columnar storage, and high-performance analytics capabilities, making it a suitable and scalable replacement for Oracle for DWH workloads. The Oracle SQL was translated to BigQuery SQL, adapting syntax and leveraging BigQuery's features.
*   **Handling Custom Oracle Functions (e.g., `sof$ab_con.concatX`)**: These critical business logic components were identified for re-implementation as BigQuery SQL UDFs. This approach ensures that the specific logic is preserved and executable within the BigQuery environment.
*   **Dynamic Table Naming (`sof$ta_bpr_opt_text_&v_datum`)**: BigQuery's `EXECUTE IMMEDIATE FORMAT` statement was chosen to handle the dynamic construction of table names, mirroring the SQL*Plus substitution variable functionality in Oracle.
*   **`LEAD` Analytic Function with `ORDER BY NULL`**: The Oracle-specific `ORDER BY NULL` within the `LEAD` function was translated to `OVER ()` in BigQuery, or an explicit `ORDER BY cntrct_id, pds_description` was introduced based on the context, assuming a specific ordering is intended for the `LEAD` function's behavior. This requires validation of the business logic.
*   **Parameter Passing Strategy**: Airflow macros are used to pass dynamic values (like execution date and run ID) to the PySpark job. The PySpark job then internally manages the execution of the BigQuery SQL, which itself calculates `v_datum` using BigQuery's `DECLARE/SET` and `SELECT` statements.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **GCP Project and Resource Setup**:
    *   Ensure a GCP Project is available.
    *   Create or identify a Cloud Composer environment.
    *   Create or identify a Dataproc cluster (or configure ephemeral clusters via workflow templates) in the specified `GCP_REGION`.
    *   Create a GCS bucket (`GCS_BUCKET`) to store the PySpark script and BigQuery SQL template.

2.  **BigQuery Dataset Creation**:
    *   Create the BigQuery dataset `isbert_schema` if it does not already exist.

3.  **Data Migration**:
    *   Migrate all source Oracle tables (`isbert_schema.dwtk_meldungen`, `isbert_schema.sof$ta_l_bpr_optionen_filter`, `DWH$TA_C_VERTRAG`, `FOS-Tabelle`, and any instances of `sof$ta_bpr_opt_text_<dynamic_date>`) to corresponding BigQuery tables within the `isbert_schema` dataset. Ensure schemas are correctly defined.

4.  **IAM and Permissions**:
    *   Grant the Cloud Composer service account and the Dataproc cluster service account (or the service account used by the PySpark job) necessary permissions:
        *   BigQuery Data Editor (`roles/bigquery.dataEditor`) on the `isbert_schema` dataset.
        *   BigQuery Job User (`roles/bigquery.jobUser`) for running queries.
        *   Storage Object Viewer (`roles/storage.objectViewer`) for reading scripts from `GCS_BUCKET`.
        *   Storage Object Creator (`roles/storage.objectCreator`) if the PySpark job writes logs or temporary files to GCS.

5.  **UDF Implementation (Critical)**:
    *   **Replace the placeholder logic** in `src/bigquery/udfs/isbert_schema.concat_placeholder_udf.sql` with the actual business logic extracted from the original Oracle `sof$ab_con.concatX` functions.
    *   Deploy this UDF to BigQuery using `bq query --use_legacy_sql=false --file=src/bigquery/udfs/isbert_schema.concat_placeholder_udf.sql`.

6.  **Upload Scripts to GCS**:
    *   Upload `src/pyspark/r_ausd_bp_ta_tarifoption_main.py` to `gs://your-gcs-bucket/pyspark_jobs/`.
    *   Upload `src/bigquery/sql/d_ausd_bp_ta_tarifoption.sql` to `gs://your-gcs-bucket/bigquery_sql/`.

7.  **Airflow DAG Configuration**:
    *   Update the placeholder values in `src/dags/dw_bert_ausd_bp_ta_tarifoption.py`:
        *   `GCP_PROJECT_ID`
        *   `GCP_REGION`
        *   `DATAPROC_CLUSTER_NAME`
        *   `GCS_BUCKET`
    *   Confirm the `schedule_interval` (`0 0 * * *` - daily at midnight) matches the original UC4 schedule.
    *   Deploy the DAG file (`src/dags/dw_bert_ausd_bp_ta_tarifoption.py`) to the Cloud Composer DAGs folder.

## 5. Known gaps & unresolved references

*   **Custom Oracle `concatX` Function Logic (B4 Item)**: The `src/bigquery/udfs/isbert_schema.concat_placeholder_udf.sql` file currently contains placeholder logic. The actual implementation of `sof$ab_con.concat1`, `sof$ab_con.concat1r`, `sof$ab_con.concat2`, `sof$ab_con.concat2r`, `sof$ab_con.concat3`, and `sof$ab_con.concat3r` is **critical** and must be extracted from the original Oracle source code and translated into BigQuery SQL UDFs. This is a significant follow-up item.
*   **`ORDER BY NULL` in `LEAD` Function**: The original Oracle SQL used `LEAD(..., 1, -1) OVER (ORDER BY NULL)`. In BigQuery, this was translated to `LEAD(..., 1, -1) OVER ()` or `ORDER BY cntrct_id, pds_description`. The business logic behind `ORDER BY NULL` (i.e., whether the order of rows for the `LEAD` function truly does not matter, or if there's an implicit ordering in Oracle that needs to be replicated) needs to be explicitly verified.
*   **Full Schema Definitions**: The complete schema definitions for `DWH$TA_C_VERTRAG`, `FOS-Tabelle`, and `sof$ta_bpr_opt_text_<dynamic_date>` were not fully available in the design document. These are required for accurate BigQuery table creation and data migration.
*   **`Stichtag` Determination Logic**: The original `r_ausd_bp_ta_tarifoption.ksh` script had logic to determine `p_stichtag` if not provided (e.g., `MIN(sysdate, maxladedatum)`). While the Airflow DAG passes `{{ ds }}`, any complex fallback or calculation logic for `stichtag` needs to be explicitly implemented in the PySpark script if it's not handled by the default Airflow macro.
*   **Dataproc Cluster Configuration**: The `DATAPROC_CLUSTER_NAME`, `GCP_PROJECT_ID`, and `GCP_REGION` in the DAG are placeholders. These must be updated to reflect the actual GCP environment.
*   **Airflow Schedule**: The `schedule_interval='0 0 * * *'` (daily at midnight) is an assumption. The exact schedule from the original UC4 job needs to be confirmed and updated in the DAG if different.
*   **Complexity/Automation Tiers**: The complexity tiers and automation buckets were inferred in the design document. A manual review of these metrics is recommended to ensure the effort estimation was accurate.

## 6. Validation

Validation should cover data integrity, functional correctness, and performance.

1.  **Unit Testing (PySpark)**:
    *   Run unit tests on `r_ausd_bp_ta_tarifoption_main.py` to verify argument parsing, BigQuery client initialization, and error handling.
    *   **Passing**: All unit tests pass, and mock BigQuery calls behave as expected.

2.  **BigQuery SQL Testing**:
    *   Execute `src/bigquery/sql/d_ausd_bp_ta_tarifoption.sql` directly in BigQuery (after UDF deployment and data migration) using a representative sample of source data.
    *   **Passing**: The script executes without syntax errors. The intermediate (`sof_ta_bpr_opt_filter`) and final (`sof_ta_tarifoption`) tables are created with the correct schema and populated with data.

3.  **Data Validation**:
    *   Compare the data in the target BigQuery tables (`isbert_schema.sof_ta_bpr_opt_filter`, `isbert_schema.sof_ta_tarifoption`) with the output of the original Oracle job for the same input data and `Stichtag`.
    *   **Passing**: Data in key columns matches exactly between BigQuery and Oracle. Any discrepancies are understood and justified (e.g., due to data type differences or intentional changes).

4.  **End-to-End (E2E) Testing (Airflow)**:
    *   Deploy the `dw_bert_ausd_bp_ta_tarifoption.py` DAG to Cloud Composer.
    *   Manually trigger the DAG or wait for its scheduled run.
    *   Monitor the Dataproc job execution and BigQuery query completion.
    *   **Passing**:
        *   The Airflow DAG runs successfully without any task failures.
        *   The Dataproc job completes successfully.
        *   The BigQuery SQL script executes successfully, creating/updating the target tables.
        *   Logs indicate successful execution and no unexpected errors.
        *   Data validation (as per step 3) confirms correctness.

5.  **Performance Testing**:
    *   Compare the execution time of the migrated job on GCP with the original Oracle/KornShell job.
    *   **Passing**: The migrated job's performance is comparable to or better than the original job, meeting defined SLAs.

## 7. Rollback procedure

In case of issues during or after go-live, the following rollback procedure can be followed:

1.  **Immediate Action (if issues detected post-go-live)**:
    *   **Pause the Airflow DAG**: Immediately pause the `dw_bert_ausd_bp_ta_tarifoption` DAG in the Cloud Composer UI to prevent further runs.
    *   **Notify Stakeholders**: Inform relevant teams (business, operations) about the issue and the rollback.

2.  **Short-Term Rollback (if data integrity is not severely compromised)**:
    *   **Revert Airflow DAG**: If a previous, stable version of the DAG exists, revert to it. Otherwise, disable the current DAG.
    *   **Data Restoration**: If the target BigQuery tables (`sof_ta_bpr_opt_filter`, `sof_ta_tarifoption`) were overwritten or corrupted, restore them from BigQuery table snapshots or backups if available. If not, the original Oracle job might need to be re-run to regenerate the data.

3.  **Full Rollback (if critical issues or data corruption)**:
    *   **Re-enable Original Job**: Re-enable and re-schedule the original `DW.BERT_AUSD_BP_TA_TARIFOPTION` job in UC4/Automic.
    *   **Data Regeneration**: If necessary, trigger the original Oracle job to regenerate the data in its original target tables.
    *   **Disable/Delete GCP Resources**:
        *   Delete the `dw_bert_ausd_bp_ta_tarifoption` DAG from Cloud Composer.
        *   Delete the BigQuery UDF `isbert_schema.concat_placeholder_udf`.
        *   Delete the target BigQuery tables `isbert_schema.sof_ta_bpr_opt_filter` and `isbert_schema.sof_ta_tarifoption`.
        *   Remove the PySpark and BigQuery SQL scripts from the GCS bucket.
        *   (Optional) Decommission the Dataproc cluster if it was dedicated to this job.

4.  **Post-Rollback Analysis**:
    *   Conduct a root cause analysis of the issue that necessitated the rollback.
    *   Address the identified issues, re-test thoroughly, and plan for a re-migration.