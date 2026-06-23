# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `k_ausd_bp_ta_bpr_instance.ksh` job, along with its dependencies `d_ausd_bp_ta_bpr_instance.sql` and `gestern.ksh`. The original KornShell script orchestrated the execution of an Oracle PL/SQL script to prepare and load base product instance data into the `sof$ta_bpr_instance` table, handling parameter parsing, validation, and date calculations.

The entire process has been migrated from a KornShell/Oracle environment to **Google BigQuery**. The orchestration logic and data transformation logic are now encapsulated within BigQuery Stored Procedures, leveraging BigQuery's native capabilities for data processing, date handling, and logging. The `gestern.ksh` utility script has been retired, with its functionality absorbed into the main BigQuery Stored Procedure.

## 2. Generated artifacts

The migration process has generated the following BigQuery SQL files:

*   **`ddl/isbert_schema.dwtk_meldungen.sql`**:
    *   **Role**: BigQuery DDL for the source table `isbert_schema.dwtk_meldungen`. This table will serve as a landing zone for ingested data from the legacy Oracle `dwtk_meldungen` table.
*   **`ddl/dw_source_isrpt_isbert.cds_ta_cntrct.sql`**:
    *   **Role**: BigQuery DDL for the source table `dw_source_isrpt_isbert.cds_ta_cntrct`. This table will serve as a landing zone for ingested data from the legacy Oracle `cds$ta_cntrct` table.
*   **`ddl/dw_source_isrpt_isbert.pds_ta_bpri_com.sql`**:
    *   **Role**: BigQuery DDL for the source table `dw_source_isrpt_isbert.pds_ta_bpri_com`. This table will serve as a landing zone for ingested data from the legacy Oracle `pds$ta_bpri_com` table.
*   **`ddl/dw_source_isrpt_isbert.sof_ta_bpr_instance.sql`**:
    *   **Role**: BigQuery DDL for the target table `dw_source_isrpt_isbert.sof_ta_bpr_instance`. This table will be populated by the migrated job and is partitioned by `processing_date` for optimized querying.
*   **`ddl/control_log.job_log.sql`**:
    *   **Role**: BigQuery DDL for a centralized logging/control table `control_log.job_log`. This table will store metadata about job executions, including status, record counts, and key dates.
*   **`sp/dw_source_isrpt_isbert.d_ausd_bp_ta_bpr_instance.sql`**:
    *   **Role**: BigQuery Stored Procedure encapsulating the core data transformation logic, replacing the original `d_ausd_bp_ta_bpr_instance.sql` Oracle PL/SQL script. It performs the `TRUNCATE` and `INSERT` operations.
*   **`sp/dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance.sql`**:
    *   **Role**: BigQuery Stored Procedure encapsulating the orchestration logic, replacing the original `k_ausd_bp_ta_bpr_instance.ksh` KornShell script. This is the main entry point, handling parameter validation, date calculations, calling the transformation procedure, and logging.

## 3. Key design decisions

*   **Orchestration to BigQuery Stored Procedure**: The KornShell orchestrator (`k_ausd_bp_ta_bpr_instance.ksh`) was migrated to a BigQuery Stored Procedure (`r_ausd_bp_ta_bpr_instance`). This centralizes the job logic within BigQuery, leveraging its native capabilities for parameter handling, error management, and execution, reducing reliance on external shell scripts.
*   **Data Transformation to BigQuery Stored Procedure**: The Oracle PL/SQL script (`d_ausd_bp_ta_bpr_instance.sql`) was converted into a separate BigQuery Stored Procedure (`d_ausd_bp_ta_bpr_instance`). This maintains modularity and allows for focused development and testing of the core data manipulation logic.
*   **Native BigQuery Date Functions**: The `gestern.ksh` script, responsible for date calculations, was retired. Its functionality is now directly implemented using BigQuery's robust native date and time functions (e.g., `CURRENT_DATE()`, `DATE_SUB()`, `FORMAT_DATE()`, `EXTRACT()`), simplifying the solution and improving reliability.
*   **BigQuery Tables for Source Data**: The Oracle tables referenced via database link (`@pcrs1`) and directly (`isbert_schema.dwtk_meldungen`) are now represented as dedicated BigQuery tables (`cds_ta_cntrct`, `pds_ta_bpri_com`, `dwtk_meldungen`). This necessitates a robust data ingestion strategy to continuously load data from the Oracle source systems into BigQuery.
*   **Centralized Logging**: Custom KornShell logging and the commented-out `FOSJobErzeugeEintrag` calls are replaced by `INSERT` statements into a dedicated BigQuery logging table (`control_log.job_log`). This provides a centralized, queryable repository for job execution metadata.
*   **BigQuery Error Handling**: The KornShell script's error handling (`DWMSG_MeldeFehler`, `exit`) is replaced by BigQuery's `SIGNAL SQLSTATE` and `ASSERT` statements, providing structured error reporting within the BigQuery environment.
*   **Direct DDL for Truncation**: The Oracle `DWPA_UTIL_SKRIPT.runstatement` call for truncating the target table is replaced by a direct `TRUNCATE TABLE` DDL statement in BigQuery, which is the standard and most efficient way to clear a table.
*   **ICCID String Formatting**: Oracle's `TO_CHAR` and `||` for ICCID concatenation are converted to BigQuery's `CONCAT` and `LPAD` functions, ensuring consistent string formatting.
*   **Target Table Partitioning**: The target table `dw_source_isrpt_isbert.sof_ta_bpr_instance` is partitioned by `processing_date`. This design decision aims to optimize query performance and reduce costs for queries that filter by date, which is a common pattern for this type of data.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure the following BigQuery datasets exist in your target Google Cloud project:
    *   `isbert_schema`
    *   `dw_source_isrpt_isbert`
    *   `control_log`
    If they do not exist, create them using the Google Cloud Console or `bq mk` command.

2.  **Deploy DDL for Tables**: Execute the generated DDL scripts to create the necessary BigQuery tables:
    *   `ddl/isbert_schema.dwtk_meldungen.sql`
    *   `ddl/dw_source_isrpt_isbert.cds_ta_cntrct.sql`
    *   `ddl/dw_source_isrpt_isbert.pds_ta_bpri_com.sql`
    *   `ddl/dw_source_isrpt_isbert.sof_ta_bpr_instance.sql`
    *   `ddl/control_log.job_log.sql`

3.  **Deploy Stored Procedures**: Execute the generated Stored Procedure scripts to create them in BigQuery:
    *   `sp/dw_source_isrpt_isbert.d_ausd_bp_ta_bpr_instance.sql`
    *   `sp/dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance.sql`

4.  **Implement Data Ingestion Pipelines**: This is a **critical step**. The original job relied on Oracle source tables, some accessed via a database link (`@pcrs1`). Robust, continuous data ingestion pipelines must be established to populate the BigQuery source tables (`isbert_schema.dwtk_meldungen`, `dw_source_isrpt_isbert.cds_ta_cntrct`, `dw_source_isrpt_isbert.pds_ta_bpri_com`) from their respective Oracle sources. This could involve:
    *   **Cloud Data Fusion**: For managed ETL pipelines.
    *   **Cloud Dataflow**: For custom, scalable data processing.
    *   **Database Migration Service (DMS)**: For continuous replication from Oracle to BigQuery.
    *   **Initial Historical Data Load**: Before the first run of the migrated job, ensure that all necessary historical data for the source tables is loaded into BigQuery.

5.  **IAM Permissions**: Grant the necessary Identity and Access Management (IAM) permissions to the service account or user that will execute the BigQuery Stored Procedures. This typically includes:
    *   `bigquery.dataEditor` on the `isbert_schema`, `dw_source_isrpt_isbert`, and `control_log` datasets.
    *   `bigquery.jobs.create` to run BigQuery jobs.

6.  **Scheduling (Optional, if external orchestration is desired)**: If the job is to be scheduled externally (e.g., via Cloud Composer/Airflow or Cloud Scheduler), configure the respective orchestrator to call the main BigQuery Stored Procedure (`CALL \`project.dataset.r_ausd_bp_ta_bpr_instance\`(...)`) with the required parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).

## 5. Known gaps & unresolved references

*   **External Data Ingestion (B4 Item)**: The most significant gap is the implementation of the continuous data ingestion pipelines from the Oracle source systems (especially those accessed via `@pcrs1`) into BigQuery. This is a prerequisite for the migrated job to function correctly and requires dedicated effort.
*   **`d_ausd_bp_ta_bpr_instance.sql` Complexity**: The original SQL script was classified as "complex" and in the "manual" migration bucket. While a BigQuery equivalent has been generated, thorough manual review, validation, and potential optimization are required to ensure logical equivalence, data type accuracy, and performance in BigQuery.
*   **`D_AUSD_BP_BPR_INSTANCE` Package Reference**: The design document noted a `USES_PACKAGE:D_AUSD_BP_BPR_INSTANCE` edge. While it might be a self-reference, further investigation is recommended to confirm that no additional Oracle package logic was missed during the migration.
*   **Commented-out Logic Confirmation**: The original `k_ausd_bp_ta_bpr_instance.ksh` contained commented-out sections related to `FOSJobDeaktivate`, file post-processing (`sed`, `sort`, `join`), and `FOSJobErzeugeEintrag`. It was assumed these are inactive and not migrated. Business confirmation is advised to ensure no critical, albeit commented-out, logic was inadvertently excluded.
*   **ICCID `LPAD` Lengths**: The `LPAD` lengths used in the BigQuery `CONCAT` for ICCID (`LPAD(CAST(bp.iccid_mi AS STRING), 2, '0')`, etc.) are based on common ICCID segment lengths. It is crucial to validate these against actual source data to ensure correct formatting and prevent data truncation or incorrect padding.

## 6. Validation

To ensure the successful migration and correct functioning of the BigQuery job, the following validation steps should be performed:

1.  **Unit Testing `d_ausd_bp_ta_bpr_instance`**:
    *   **How to run**: Call the `dw_source_isrpt_isbert.d_ausd_bp_ta_bpr_instance` stored procedure directly with sample data in the source tables (`cds_ta_cntrct`, `pds_ta_bpri_com`, `dwtk_meldungen`).
    *   **What "passing" means**:
        *   The procedure completes without errors.
        *   The `dw_source_isrpt_isbert.sof_ta_bpr_instance` table is truncated and then populated with data.
        *   The number of records inserted matches the expected count from the source system for the given `p_Stichtag_date`.
        *   A sample of the inserted data (e.g., ICCID, CNTRCT_ID, BPR_ID) matches the expected output from the legacy Oracle job for the same input.

2.  **Integration Testing `r_ausd_bp_ta_bpr_instance`**:
    *   **How to run**: Call the main orchestration stored procedure `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance` with various parameter combinations:
        *   **Valid parameters**: Provide valid `p_JobKennung`, `p_EintragsNr`, `p_Stichtag` (DDMMYYYY format), and `p_wiederanlaufWert`.
        *   **Missing parameters**: Test cases where `p_JobKennung`, `p_EintragsNr`, or `p_Stichtag` are `NULL` or empty.
        *   **Invalid `p_Stichtag` format**: Provide a `p_Stichtag` that does not conform to DDMMYYYY.
    *   **What "passing" means**:
        *   **Valid parameters**:
            *   The procedure completes successfully.
            *   The `dw_source_isrpt_isbert.sof_ta_bpr_instance` table is correctly populated as per unit test criteria.
            *   An entry is recorded in `control_log.job_log` with `job_status = 'A'` (or 'C' if 'A' means active and 'C' means complete), the correct `tab_name`, `record_count`, and `stichtag`.
        *   **Missing/Invalid parameters**:
            *   The procedure terminates with an appropriate `SIGNAL SQLSTATE` error message, matching the expected error messages from the original KornShell script.
            *   No data is incorrectly processed or inserted.

3.  **Performance Testing**:
    *   **How to run**: Execute the `r_ausd_bp_ta_bpr_instance` procedure with production-like data volumes.
    *   **What "passing" means**: The execution time and BigQuery slot consumption are within acceptable limits, ideally matching or improving upon the legacy job's performance.

4.  **Data Quality Checks**:
    *   **How to run**: Query the `dw_source_isrpt_isbert.sof_ta_bpr_instance` table after a successful run.
    *   **What "passing" means**:
        *   All columns have the expected data types.
        *   Nullability constraints are respected.
        *   The `ICCID` format is correct (e.g., `XX-XXXXXX-X-XXXXXXXXX-X`).
        *   No unexpected data anomalies are observed.

## 7. Rollback procedure

In the event that the migrated BigQuery job encounters critical issues after go-live, the following rollback procedure can be initiated:

1.  **Stop New Executions**: Immediately halt any scheduled or manual executions of the BigQuery Stored Procedure `dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance`. If using an external orchestrator (e.g., Cloud Composer), pause or disable the corresponding DAG/job.

2.  **Revert Scheduling**: Re-enable the scheduling mechanism for the original legacy KornShell job (`k_ausd_bp_ta_bpr_instance.ksh`) to ensure business continuity and data generation.

3.  **Data Recovery (if necessary)**:
    *   Since the BigQuery job `TRUNCATE`s and `INSERT`s into `dw_source_isrpt_isbert.sof_ta_bpr_instance`, if the rollback is initiated quickly, the legacy job can simply re-run and populate its target table.
    *   If the `dw_source_isrpt_isbert.sof_ta_bpr_instance` table is critical for other downstream processes that might have consumed incorrect data, a more extensive data recovery might be needed. This could involve:
        *   Restoring the `sof_ta_bpr_instance` table from a BigQuery table snapshot or backup if available.
        *   Re-running the legacy job to populate its target, then re-ingesting that data into BigQuery.

4.  **Clean Up BigQuery (Optional)**: To prevent accidental re-execution of the problematic BigQuery job, consider temporarily renaming or dropping the BigQuery Stored Procedures (`dw_source_isrpt_isbert.r_ausd_bp_ta_bpr_instance` and `dw_source_isrpt_isbert.d_ausd_bp_ta_bpr_instance`) and potentially the target table `dw_source_isrpt_isbert.sof_ta_bpr_instance`.

5.  **Root Cause Analysis**: Investigate the root cause of the failure in the BigQuery environment, apply necessary fixes, and re-validate thoroughly before attempting another go-live.