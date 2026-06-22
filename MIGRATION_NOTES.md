# MIGRATION_NOTES: BERT_V_TA_DISC_ZUSGF

## 1. Summary

The `BERT_V_TA_DISC_ZUSGF` job, originally an Oracle PL/SQL-driven process orchestrated by UC4 and KornShell scripts, has been migrated. Its primary function is to consolidate and concatenate discount descriptions associated with contract data, populating a target table (`SOF$TA_DISC_ZUSGF`).

The migration target platform is Google Cloud Platform (GCP), utilizing:
*   **Google Cloud Composer (Apache Airflow)** for job orchestration and scheduling, replacing UC4 and KornShell wrappers.
*   **Google BigQuery** for data storage and as the target for transformed data, replacing Oracle tables.
*   **Python with Pandas** for the core data transformation logic, replacing the Oracle PL/SQL pipelined table function.

## 2. Generated Artifacts

The migration produced the following key artifacts:

*   **`src/python/transform_discount_data.py`**
    *   **Role**: This Python script contains the core data transformation logic. It reads discount data from the `sof_ta_discount` BigQuery table, applies the complex concatenation and length-checking logic (re-engineered from the Oracle PL/SQL pipelined function `concat_discounts`), and writes the results to the `sof_ta_disc_zusgf` BigQuery table. It uses the `pandas` library for data manipulation and `google.cloud.bigquery` for interaction with BigQuery.
*   **`dags/bert_v_ta_disc_zusgf_dag.py`**
    *   **Role**: This Apache Airflow DAG defines the end-to-end workflow for the `BERT_V_TA_DISC_ZUSGF` job. It orchestrates the execution, including fetching the `v_datum` equivalent (processing date) and triggering the Python transformation script. It replaces the legacy UC4 job and KornShell control scripts, managing task dependencies, scheduling, and error handling within Cloud Composer.

## 3. Key Design Decisions

Several key design decisions were made during this migration:

*   **Orchestration: UC4/KornShell to Apache Airflow (Cloud Composer)**
    *   **Why**: Cloud Composer provides a managed, scalable, and cloud-native orchestration platform. It allows for Python-based DAG definitions, enabling seamless integration with other GCP services and modern development practices, replacing the legacy UC4 scheduler and KornShell scripting.
    *   **Trade-offs**: Requires re-implementation of job-specific variables, logging, and error handling logic from KornShell into Python/Airflow constructs.

*   **Data Storage: Oracle to BigQuery**
    *   **Why**: BigQuery offers a highly scalable, cost-effective, and fully managed data warehouse solution. It provides superior performance for analytical queries and integrates natively with other GCP data services, aligning with a cloud-first strategy.
    *   **Trade-offs**: Requires data type mapping and migration of existing Oracle tables to BigQuery. Initial data replication and ongoing CDC (Change Data Capture) mechanisms are necessary until the Oracle source is fully decommissioned.

*   **Transformation Logic: Oracle PL/SQL Pipelined Function to Python/Pandas**
    *   **Why**: The original Oracle PL/SQL `concat_discounts` function involved complex procedural logic, including explicit loops, `PIPE ROW` for generating multiple output rows per input group, and dynamic string concatenation with a 500-character length constraint. Re-implementing this directly in standard SQL (e.g., BigQuery SQL) would be challenging and potentially less efficient for such procedural requirements. Python with the Pandas library was chosen to accurately replicate this logic, allowing for iterative processing, conditional logic, and precise character-limit handling.
    *   **Trade-offs**: Introduces a Python dependency and requires careful testing to ensure functional equivalence with the original PL/SQL. For extremely large datasets, Pandas might become memory-bound, potentially necessitating a switch to PySpark (on Dataproc) for distributed processing, though Pandas is sufficient for current anticipated volumes.

*   **`v_datum` Handling**:
    *   **Why**: The legacy `v_datum` (a processing date from `dwtk_meldungen`) was identified as an input to the Oracle PL/SQL script. A dedicated Airflow task (`get_sysdate_equivalent_task`) was created to explicitly fetch this value from BigQuery, promoting clarity and modularity in the DAG.
    *   **Trade-offs**: While fetched, the current `transform_discount_data.py` script does not explicitly use `v_datum` in its core concatenation logic. This implies `v_datum` might have been used for filtering source data in the legacy system, which is not directly replicated in the current Python script's `SELECT` statement. This is a potential gap that needs further validation if `v_datum` was indeed a filter.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live in production, the following manual steps must be completed:

1.  **GCP Project Setup**:
    *   Ensure a Google Cloud Project is provisioned and configured.
    *   Enable necessary APIs (e.g., BigQuery API, Cloud Composer API).
2.  **BigQuery Dataset and Table Creation**:
    *   Create the target BigQuery dataset (e.g., `your_bigquery_dataset`).
    *   Create the following BigQuery tables with appropriate schemas, mirroring their Oracle counterparts:
        *   `project_id.dataset_id.dwtk_meldungen`
        *   `project_id.dataset_id.sof_ta_discount`
        *   `project_id.dataset_id.sof_ta_disc_zusgf` (target table)
3.  **IAM Roles and Permissions**:
    *   Ensure the Cloud Composer service account has the necessary IAM roles to:
        *   Read from `project_id.dataset_id.dwtk_meldungen` and `project_id.dataset_id.sof_ta_discount`.
        *   Write (truncate and load) to `project_id.dataset_id.sof_ta_disc_zusgf`.
        *   Access XComs for inter-task communication.
4.  **Cloud Composer Environment**:
    *   A Cloud Composer environment must be running and accessible.
    *   Ensure the Python environment in Composer includes `pandas` and `google-cloud-bigquery`.
5.  **Initial Data Replication**:
    *   Perform an initial historical data load from the legacy Oracle `dwtk_meldungen`, `sof$ta_discount`, and `SOF$TA_DISC_ZUSGF` tables to their respective BigQuery tables.
    *   Establish a continuous data replication mechanism (e.g., Google Cloud Datastream, Fivetran, or custom ETL) to keep `dwtk_meldungen` and `sof_ta_discount` in BigQuery synchronized with the operational Oracle system until Oracle is fully deprecated.
6.  **Airflow Configuration**:
    *   Update the `GCP_PROJECT_ID` and `BQ_DATASET_ID` variables in `dags/bert_v_ta_disc_zusgf_dag.py` to reflect the actual project and dataset IDs, or configure them as Airflow Variables.
    *   Define the desired `schedule` for the DAG in `dags/bert_v_ta_disc_zusgf_dag.py` (e.g., `@daily`, `0 5 * * *`).
7.  **Deployment**:
    *   Deploy `src/python/transform_discount_data.py` and `dags/bert_v_ta_disc_zusgf_dag.py` to the Cloud Composer DAGs folder.

## 5. Known Gaps & Unresolved References

*   **`v_datum` Usage in Transformation**: The `get_sysdate_equivalent_task` successfully fetches `v_datum` (equivalent processing date). However, the `transform_and_load_discount_data` Python script, as currently implemented, does not explicitly use this `v_datum` for filtering the source `sof_ta_discount` data. If the legacy PL/SQL used `v_datum` to filter the input discounts, this filtering logic needs to be added to the Python script.
*   **Full Scope of `v_carmen` DB-Link**: The original Oracle script defined a `v_carmen` DB-link. While it doesn't appear to be directly used in the `concat_discounts` logic itself, its broader role in the legacy data ecosystem (e.g., if `sof$ta_discount` or `dwtk_meldungen` were populated via this link) remains unclear. Any upstream dependencies related to `v_carmen` must be identified and addressed.
*   **Refactoring Shared KornShell Utilities**: While the main orchestration and transformation logic has been migrated, the full extent of unique, non-generic logic within the numerous legacy KornShell helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, etc.) is not fully known. Any critical business logic embedded in these utilities that is not yet replicated in Python modules or Airflow features represents a potential gap.
*   **Performance for Large Datasets**: The current Python/Pandas implementation for `concat_discounts` is suitable for moderate data volumes. For very large datasets, performance bottlenecks might arise, potentially requiring a re-evaluation and migration to PySpark on Dataproc for distributed processing.
*   **Error Handling and Logging Parity**: While Airflow provides robust logging and error handling, a thorough review is needed to ensure the new system's error reporting, alerting, and logging granularity matches or exceeds the legacy system's capabilities, especially concerning specific business error codes or messages.

## 6. Validation

Validation of the migrated job involves several steps to ensure functional equivalence and performance.

1.  **Unit Testing (`transform_discount_data.py`)**:
    *   **How to run**: Execute unit tests developed for `transform_discount_data.py` using a Python testing framework (e.g., `pytest`). These tests should cover various scenarios, including:
        *   Empty input data.
        *   Single discount per contract.
        *   Multiple discounts per contract, staying within the 500-character limit.
        *   Multiple discounts per contract, exceeding the 500-character limit and requiring multiple output rows.
        *   Edge cases for `rabatt` values (e.g., `NULL`, empty strings).
    *   **Passing criteria**: All unit tests pass, demonstrating that the Python logic correctly replicates the Oracle PL/SQL `concat_discounts` behavior.

2.  **Integration Testing (Airflow DAG)**:
    *   **How to run**:
        *   Deploy the `bert_v_ta_disc_zusgf_dag.py` to a staging Cloud Composer environment.
        *   Manually trigger the DAG or allow it to run on its defined schedule.
        *   Monitor the Airflow UI for task execution status and logs.
    *   **Passing criteria**:
        *   The DAG runs to completion without errors.
        *   All tasks within the DAG (`get_sysdate_equivalent_task`, `transform_and_load_discount_data_task`) complete successfully.
        *   Logs in Cloud Logging show expected output and no unexpected errors or warnings.

3.  **Data Reconciliation**:
    *   **How to run**:
        *   Execute the legacy Oracle job and the new BigQuery job (via Airflow) for the same input data (e.g., a specific date range or a full run).
        *   Extract the output from both the legacy `SOF$TA_DISC_ZUSGF` table and the new `project_id.dataset_id.sof_ta_disc_zusgf` BigQuery table.
        *   Perform a row-by-row comparison of the two datasets, focusing on `cntrct_id`, `cntrct_obj_version`, `rabatt_alle`, and `disc_vector_ty`.
        *   Use SQL queries (e.g., `EXCEPT` in BigQuery) or data comparison tools to identify discrepancies.
    *   **Passing criteria**: The output data in `project_id.dataset_id.sof_ta_disc_zusgf` is identical to the output from the legacy Oracle `SOF$TA_DISC_ZUSGF` table for the same input, within acceptable tolerances (e.g., for floating-point numbers if applicable, though not expected here).

4.  **Performance Testing**:
    *   **How to run**: Measure the execution time of the Airflow DAG and its individual tasks in the staging environment. Compare this against the historical execution times of the legacy UC4 job.
    *   **Passing criteria**: The new job's execution time is comparable to or better than the legacy job, and it meets any defined Service Level Objectives (SLOs).

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Disable New Job**:
    *   Immediately pause or un-schedule the `bert_v_ta_disc_zusgf_dag` in the Cloud Composer Airflow UI to prevent further execution of the new job.
2.  **Re-enable Legacy Job**:
    *   Re-enable and re-schedule the original `DW.BERT_AUSD_V_TA_DISC_ZUSGF` job in UC4/Automic.
    *   Verify that the legacy job runs successfully and populates the Oracle `SOF$TA_DISC_ZUSGF` table as expected.
3.  **Data State (Optional)**:
    *   If the new job has written incorrect data to `project_id.dataset_id.sof_ta_disc_zusgf`, decide whether to:
        *   Truncate the BigQuery target table to clear erroneous data.
        *   Restore the BigQuery target table from a previous snapshot if available (e.g., using BigQuery's time travel capabilities or a backup).
        *   Simply ignore the BigQuery table's content if the legacy system is the sole source of truth during rollback.
4.  **Investigation and Remediation**:
    *   Analyze the logs and metrics from the failed Airflow DAG runs in Cloud Logging and Airflow UI to identify the root cause of the issue.
    *   Address the identified problems in the Python code or Airflow DAG.
    *   Once the issues are resolved, re-validate the fix in a staging environment before attempting another go-live.