# MIGRATION_NOTES: DW.BERT_AUSD_BP_TA_APN_VERTRAG

## 1. Summary

This migration involved the re-platforming of an Oracle PL/SQL script, `d_ausd_bp_ta_apn_vertrag.sql`, responsible for processing and aggregating Access Point Name (APN) and contract reference data. The original script read from the `sof$ta_bpr_apn` source table, consolidated APN values and contract IDs based on a contract identifier, and inserted the aggregated results into the `sof$ta_apn_vertrag` target table.

The job has been migrated from an Oracle PL/SQL environment to Google Cloud's BigQuery platform. The procedural, cursor-based logic has been re-engineered into a set-based BigQuery SQL query, orchestrated by an Apache Airflow DAG running on Cloud Composer.

## 2. Generated artifacts

The migration process generated the following files:

*   **`ddl/sof_ta_bpr_apn.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `SOFTA_BPR_APN` table. This table serves as the BigQuery equivalent of the original Oracle `sof$ta_bpr_apn` source table.
*   **`ddl/sof_ta_apn_vertrag.sql`**
    *   **Role:** BigQuery DDL script to create the `SOFTA_APN_VERTRAG` table. This table is the BigQuery equivalent of the original Oracle `sof$ta_apn_vertrag` target table, designed to store the aggregated APN and contract reference data.
*   **`sql/d_ausd_bp_ta_apn_vertrag_bq.sql`**
    *   **Role:** The core BigQuery SQL transformation script. This script replaces the original Oracle PL/SQL logic. It performs a `TRUNCATE TABLE` on the target table and then uses `STRING_AGG` and `GROUP BY` to aggregate `access_point_name` and `cntrct_id_ref` values from `SOFTA_BPR_APN` before inserting them into `SOFTA_APN_VERTRAG`.
*   **`dags/dw_bert_ausd_bp_ta_apn_vertrag_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG is responsible for orchestrating the execution of the `d_ausd_bp_ta_apn_vertrag_bq.sql` script within a Google Cloud Composer environment. It defines the job's dependencies, scheduling, and execution parameters.

## 3. Key design decisions

*   **Platform Shift (Oracle PL/SQL to BigQuery SQL)**: The primary decision was to migrate from a proprietary Oracle PL/SQL environment to Google Cloud's BigQuery. This aligns with the broader strategy of leveraging cloud-native data warehousing capabilities for scalability, performance, and cost efficiency.
*   **Procedural to Set-Based Transformation**: The original Oracle script used a cursor-based `FOR` loop for row-by-row processing and string concatenation. In BigQuery, this procedural logic was re-engineered into a single, highly optimized set-based SQL query utilizing `STRING_AGG` and `GROUP BY` clauses. This approach is fundamental to BigQuery's columnar and distributed architecture, offering significant performance improvements over row-by-row processing.
*   **Handling `TRUNCATE TABLE` and `COMMIT`**:
    *   The Oracle `TRUNCATE TABLE` operation is directly translated to BigQuery's `TRUNCATE TABLE` statement, ensuring the target table is cleared before new data insertion.
    *   Oracle's `COMMIT` statements are not explicitly required in BigQuery DML operations, as BigQuery transactions are atomic by default. The `INSERT` statement is implicitly committed upon successful completion.
*   **Mimicking Oracle `VARCHAR2` Length Constraints**: The original Oracle script used `SUBSTR` and `RTRIM` to manage string lengths and remove trailing delimiters. The BigQuery SQL script replicates this behavior using `SUBSTR(RTRIM(STRING_AGG(...), ', '), 1, 100)` to ensure aggregated strings adhere to the original 100-character limit and remove any trailing comma-space.
*   **Deterministic Aggregation Order**: To ensure consistent output, especially for string aggregations, `STRING_AGG` in BigQuery includes an `ORDER BY` clause (e.g., `ORDER BY access_point_name`). This makes the order of concatenated elements deterministic, mimicking how a cursor might implicitly process records in a sorted manner.
*   **Airflow Orchestration**: Apache Airflow (via Cloud Composer) was chosen for job orchestration due to its flexibility, extensibility, and native integration with Google Cloud services, providing robust scheduling, monitoring, and error handling capabilities.

## 4. Manual steps before go-live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (e.g., `your_project.your_dataset`) exists. If not, create it in the Google Cloud Console or via `bq mk` command.
2.  **BigQuery Table Creation**:
    *   Execute the DDL scripts:
        *   `ddl/sof_ta_bpr_apn.sql` to create the `SOFTA_BPR_APN` table.
        *   `ddl/sof_ta_apn_vertrag.sql` to create the `SOFTA_APN_VERTRAG` table.
    *   **Action**: Run these SQL files in BigQuery.
3.  **Initial Data Load for `SOFTA_BPR_APN`**:
    *   Migrate all historical and current data from the source Oracle `sof$ta_bpr_apn` table to the newly created BigQuery `your_project.your_dataset.SOFTA_BPR_APN` table. This is a critical one-time or recurring data ingestion step.
    *   **Action**: Use Google Cloud Data Transfer Service, BigQuery Data Load jobs, or custom scripts (e.g., Python with `pandas-gbq`) to perform this initial data migration.
4.  **IAM Permissions**:
    *   Grant the service account used by the Airflow DAG (Cloud Composer environment) the necessary BigQuery permissions. At a minimum, this includes:
        *   `BigQuery Data Editor` on `your_project.your_dataset` to `TRUNCATE` and `INSERT` into `SOFTA_APN_VERTRAG`.
        *   `BigQuery Data Viewer` on `your_project.your_dataset` to `SELECT` from `SOFTA_BPR_APN`.
    *   **Action**: Configure IAM roles in Google Cloud Console.
5.  **Update Airflow DAG Schedule**:
    *   Modify the `schedule_interval` in `dags/dw_bert_ausd_bp_ta_apn_vertrag_dag.py` from `None` to the desired production schedule (e.g., `'0 3 * * *'` for daily at 3 AM UTC).
    *   **Action**: Edit the DAG file and deploy it to the Cloud Composer environment.
6.  **Replace Placeholders**:
    *   Ensure all instances of `your_project.your_dataset` within the SQL scripts and DAG are replaced with the actual BigQuery project ID and dataset name.
    *   **Action**: Search and replace in `ddl/*.sql` and `sql/*.sql`.

## 5. Known gaps & unresolved references

The following items were identified during the migration design and require further attention or are noted as potential risks:

*   **`file_complexity` data absence**: The lack of detailed complexity analysis from the original `file_complexity` data means that the migration effort and potential challenges were estimated based on manual code review. This could lead to an underestimation of effort if hidden complexities exist.
*   **Full `DWPA_UTIL_SKRIPT` functionality**: The analysis assumed `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` was primarily for `TRUNCATE TABLE`. If this utility performs other complex or critical operations (e.g., logging, auditing, conditional execution), these functionalities will need to be thoroughly understood and explicitly re-implemented in BigQuery SQL, Python, or another appropriate language.
*   **Data Type and Length Handling**: While `SUBSTR` is used in BigQuery to mimic Oracle's `VARCHAR2` length limits, potential data truncation or unexpected behavior due to character sets/encodings should be carefully reviewed and tested, especially if source data contains multi-byte characters.
*   **Error Handling and Logging**: The original Oracle script used a generic `EXCEPTION WHEN OTHERS` block. The BigQuery solution currently relies on Airflow's default error handling. A more robust error handling and logging strategy should be implemented, potentially using BigQuery audit logs, Cloud Logging, or custom logging tables for specific business errors.
*   **Performance of `STRING_AGG`**: While `STRING_AGG` is generally efficient in BigQuery, its performance with extremely large numbers of groups or very long aggregated strings should be monitored and tested with representative data volumes to ensure it meets performance SLAs.
*   **Historical Data Management**: The current BigQuery SQL script performs a `TRUNCATE TABLE` followed by an `INSERT`. If the `sof$ta_apn_vertrag` table in Oracle contained historical data that needs to be preserved or historized in BigQuery, the current approach will overwrite it. A different strategy (e.g., append-only, snapshotting, or SCD type 2) would be required to manage historical data.
*   **`isbert_schema.dwtk_meldungen` and `v_carmen`**: The design document noted references to `isbert_schema.dwtk_meldungen` (for `v_datum`) and `v_carmen` (`@pcrs1`). While their direct impact on the core aggregation logic was deemed minimal, their full purpose and any potential indirect dependencies should be confirmed. If they hold critical metadata or point to external data sources, these dependencies must be explicitly addressed in the BigQuery environment.

## 6. Validation

To ensure the migrated job functions correctly and produces accurate results, the following validation steps should be performed:

1.  **Execute the Airflow DAG**:
    *   **How to run**: Trigger the `dw_bert_ausd_bp_ta_apn_vertrag` DAG manually from the Airflow UI in your Cloud Composer environment.
    *   **Passing criteria**: The DAG should complete successfully without any task failures. All tasks within the DAG should show a "success" status.
2.  **Verify Target Table Population**:
    *   **How to run**: After the DAG completes, query the `your_project.your_dataset.SOFTA_APN_VERTRAG` table in BigQuery.
    *   **Passing criteria**: The table should be populated with data. It should not be empty.
3.  **Row Count Comparison**:
    *   **How to run**:
        *   Get the row count from the original Oracle `sof$ta_apn_vertrag` table (if it was populated by the original job with the same input data).
        *   Get the row count from the BigQuery `your_project.your_dataset.SOFTA_APN_VERTRAG` table.
    *   **Passing criteria**: The row counts should match. If there are discrepancies, investigate potential data filtering differences or issues during data migration from Oracle to BigQuery.
4.  **Data Content Validation (Sample-based)**:
    *   **How to run**:
        *   Select a representative sample of `cntrct_id` values from the BigQuery `SOFTA_BPR_APN` table.
        *   For these `cntrct_id`s, manually (or via a script) re-create the expected `aggregated_apn` and `aggregated_cntrct_ref` values based on the original Oracle logic (concatenation, length limits, `RTRIM`).
        *   Compare these expected values with the actual values in the BigQuery `your_project.your_dataset.SOFTA_APN_VERTRAG` table for the same `cntrct_id`s.
    *   **Passing criteria**: The aggregated `aggregated_apn` and `aggregated_cntrct_ref` values in BigQuery should exactly match the expected output derived from the source data and the original logic. Pay close attention to string lengths, delimiters, and the order of concatenated elements.
5.  **Data Type and Length Verification**:
    *   **How to run**: Inspect the schema of `your_project.your_dataset.SOFTA_APN_VERTRAG` in BigQuery.
    *   **Passing criteria**: Ensure that `cntrct_id`, `aggregated_apn`, and `aggregated_cntrct_ref` columns are of type `STRING` and that the actual data does not exceed the intended 100-character length for the aggregated fields.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Executions**:
    *   Pause or un-deploy the `dw_bert_ausd_bp_ta_apn_vertrag` Airflow DAG in the Cloud Composer environment to prevent further executions of the migrated job.
2.  **Revert to Original Oracle Job**:
    *   Re-enable and restart the original Oracle PL/SQL job (`DW.BERT_AUSD_BP_TA_APN_VERTRAG`) in its native environment. Ensure it has access to its original source and target tables.
3.  **Data Integrity Check (Optional but Recommended)**:
    *   If the BigQuery job ran and potentially produced incorrect data in `your_project.your_dataset.SOFTA_APN_VERTRAG`, and if this table is consumed by other processes, assess the impact. Since the BigQuery job performs a `TRUNCATE` before `INSERT`, it overwrites the table entirely. If a previous correct state of `SOFTA_APN_VERTRAG` is needed, it would require restoring from a BigQuery table snapshot or a backup if such mechanisms are in place.
4.  **Troubleshoot and Redesign**:
    *   Analyze the root cause of the failure in the BigQuery migration. This may involve reviewing logs, re-examining the BigQuery SQL, or re-evaluating the design decisions.
5.  **Clean Up (if necessary)**:
    *   If the migration is deemed permanently unsuccessful or requires a complete restart, the BigQuery tables (`SOFTA_BPR_APN`, `SOFTA_APN_VERTRAG`) and the Airflow DAG can be deleted.