# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh

## 1. Purpose & Scope

This document outlines the migration strategy for the KornShell script `k_ausd_v_ta_cntrct_crs2.ksh` to Google Cloud Platform, specifically BigQuery. The original script acts as a control and orchestration layer for a SQL script, managing job execution, parameter parsing, error handling, and reporting the number of processed records. Its primary function is to process data related to the `ta_cntrct_crs2` table. The migration aims to re-platform this ETL job into a BigQuery-native environment, leveraging BigQuery Stored Procedures for logic and orchestration, and potentially Cloud Composer for overall workflow management.

## 2. Source Inventory

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh`
    *   **Technology:** KornShell script
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Summary:** This KornShell script acts as a control script for data extraction, parsing parameters, sourcing utility functions, executing a SQL script (`d_ausd_v_ta_cntrct_crs2.sql`), and handling errors. It manages the execution of a SQL script that likely processes data for the `ta_cntrct_crs2` table. It takes command-line parameters for job identification, validates them, and uses a temporary file to capture record counts.

## 3. Target Architecture

The target architecture will leverage BigQuery's capabilities for data processing and orchestration:

*   **Data Storage:** All source and target tables (e.g., `DWTK_MELDUNGEN`, `SOF$TA_CNTRCT_CRS`, `SOF$TA_CNTRCT_CRS2`, `VIA`) will be migrated to BigQuery tables.
*   **Transformation Logic:** The SQL logic within `d_ausd_v_ta_cntrct_crs2.sql` and the orchestration logic of `k_ausd_v_ta_cntrct_crs2.ksh` will be encapsulated within BigQuery Stored Procedures.
*   **Orchestration:** The primary orchestration will be handled by a main BigQuery Stored Procedure, mirroring the KornShell script's control flow. For more complex scheduling, dependency management, or integration with other systems, Cloud Composer (Apache Airflow) could be introduced as an external orchestrator.
*   **Logging & Monitoring:** Job status and execution logs, including record counts, will be stored in dedicated BigQuery logging tables.

## 4. Data Flow & Lineage

The current data flow orchestrated by the KornShell script:

1.  **`k_ausd_v_ta_cntrct_crs2.ksh` (KornShell Script):**
    *   Parses command-line parameters (`p_JobKennung`, `p_EintragsNr`).
    *   Sources various utility shell scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    *   Calls an internal function `pruefeParameterGesetzt` for parameter validation.
    *   Executes the SQL script `d_ausd_v_ta_cntrct_crs2.sql` via a wrapper function `starteSQLSkript`.
    *   Reads record count from a temporary file (`tmpFile`) into a shell variable `v_records`.
    *   Handles errors and exits.

2.  **`d_ausd_v_ta_cntrct_crs2.sql` (SQL Script - invoked by ksh):**
    *   **Reads from:**
        *   `TABLE:DWTK_MELDUNGEN` (Oracle)
        *   `TABLE:SOF$TA_CNTRCT_CRS` (Oracle)
    *   **Writes to:**
        *   `TABLE:SOF$TA_CNTRCT_CRS2` (Oracle)
        *   `TABLE:VIA` (Oracle)
    *   **Uses:**
        *   `PACKAGE:DWPA_UTIL_SKRIPT` (Oracle PL/SQL)
        *   `PACKAGE:CR` (Oracle PL/SQL)

**Migrated Data Flow (BigQuery):**

1.  **Main BigQuery Stored Procedure (`control_k_ausd_v_ta_cntrct_crs2`):**
    *   Accepts input parameters (equivalent to `p_JobKennung`, `p_EintragsNr`).
    *   Calls a helper BigQuery Stored Procedure (`sp_job_prepare`) for job status management.
    *   Calls the core logic BigQuery Stored Procedure (`sp_d_ausd_v_ta_cntrct_crs2`).
    *   Logs execution details, including processed record counts, to a BigQuery logging table (`job_run_log`).
    *   Handles errors using `RAISE` or error logging.

2.  **Core Logic BigQuery Stored Procedure (`sp_d_ausd_v_ta_cntrct_crs2`):**
    *   Translates the original SQL logic from `d_ausd_v_ta_cntrct_crs2.sql` into BigQuery SQL.
    *   Reads from migrated BigQuery tables (e.g., `bq_dataset.dwtk_meldungen`, `bq_dataset.sof_ta_cntrct_crs`).
    *   Writes to migrated BigQuery tables (e.g., `bq_dataset.sof_ta_cntrct_crs2`, `bq_dataset.via`).
    *   Returns the count of processed records.

3.  **Helper BigQuery Stored Procedure (`sp_job_prepare`):**
    *   Manages job activation/deactivation and registration in a BigQuery job status table (e.g., `bq_dataset.job_table`).

## 5. Transformation Logic

### a. `k_ausd_v_ta_cntrct_crs2.ksh` (KornShell Script) to BigQuery Stored Procedure (`control_k_ausd_v_ta_cntrct_crs2`)

*   **Parameter Parsing (`getopts`):** Command-line arguments will be replaced by direct input parameters of the BigQuery Stored Procedure.
*   **Environment Sourcing:**
    *   Environment files like `.dw_init` will be replaced by BigQuery dataset and project references within the SQL code, or by configuration parameters passed to the procedure.
    *   Utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) will be reimplemented as BigQuery UDFs or separate BigQuery Stored Procedures, or their logic will be integrated directly where possible.
*   **SQL Script Execution (`starteSQLSkript`):** The `starteSQLSkript` function call will be replaced by a direct `CALL` statement to the `sp_d_ausd_v_ta_cntrct_crs2` BigQuery Stored Procedure.
*   **Temporary File for Record Count:** The use of `tmpFile` and `eval "v_records=\`cat $tmpFile\`"` will be replaced by capturing the return value (record count) from the `sp_d_ausd_v_ta_cntrct_crs2` stored procedure, and logging it to a BigQuery table (`job_run_log`).
*   **Error Handling:** Shell error codes and `DWMSG_MeldeFehler` will be replaced with BigQuery's `RAISE` statement for critical errors or by writing error details to a dedicated logging table.
*   **Job Management:** The implied job activation/deactivation logic from the comments will be handled by a `sp_job_prepare` BigQuery Stored Procedure that updates a `job_table`.

### b. `d_ausd_v_ta_cntrct_crs2.sql` (SQL Script) to BigQuery Stored Procedure (`sp_d_ausd_v_ta_cntrct_crs2`)

*   **SQL Dialect Conversion:** The Oracle SQL statements will be translated to BigQuery Standard SQL, including `MERGE`, `INSERT`, `SELECT`, and `UPDATE` operations.
*   **Package Reimplementation:** The logic of Oracle PL/SQL packages `DWPA_UTIL_SKRIPT` and `CR` (e.g., functions, procedures) will be reimplemented as BigQuery UDFs or integrated directly into the `sp_d_ausd_v_ta_cntrct_crs2` stored procedure.
*   **Temporary Tables:** Oracle global temporary tables or other temporary constructs will be mapped to BigQuery temporary tables or CTEs as appropriate.

## 6. External Dependencies

The following external dependencies will be addressed during migration:

*   **Oracle Tables (`DWTK_MELDUNGEN`, `SOF$TA_CNTRCT_CRS`, `SOF$TA_CNTRCT_CRS2`, `VIA`):** These will be migrated to BigQuery tables, maintaining their schema and data. Source tables will be ingested into BigQuery, and target tables will be created within BigQuery.
*   **Oracle PL/SQL Packages (`DWPA_UTIL_SKRIPT`, `PACKAGE:CR`):** The functionality embedded in these packages will be analyzed and reimplemented in BigQuery as UDFs (User-Defined Functions) or incorporated directly into the BigQuery Stored Procedures.
*   **Legacy Shell Utilities/Environment:**
    *   `$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`: These custom shell scripts and environment settings will be replaced by BigQuery's native features (e.g., parameters, logging, error handling) or by components in a Cloud Composer DAG if external orchestration is used.
    *   `BERT_DIR_ROOT`, `DW_DIR_UTL`: These environment variables will be replaced by explicit dataset/project references, BigQuery configuration tables, or parameters.
*   **Temporary Files:** The mechanism of using temporary files to pass data (like record counts) will be replaced by direct return values from BigQuery Stored Procedures or by logging to BigQuery tables.

## 7. Unresolved / Risks

*   **Exact Logic of Sourced Shell Scripts:** The full functionality of the sourced KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, and the implicitly referenced `h_alis_job.ksh`) needs to be fully reverse-engineered and accurately translated into BigQuery-compatible logic (stored procedures, UDFs) or managed by an orchestration layer.
*   **Full `d_ausd_v_ta_cntrct_crs2.sql` Content:** The detailed SQL logic of `d_ausd_v_ta_cntrct_crs2.sql` was not available for direct analysis. A thorough review and translation from Oracle SQL to BigQuery Standard SQL will be required.
*   **Oracle Package Functionality:** The precise implementation details of Oracle packages `DWPA_UTIL_SKRIPT` and `CR` are crucial for accurate translation to BigQuery. These may require significant re-engineering.
*   **`TABLE:VIA` Purpose:** The purpose and usage of the `TABLE:VIA` (written to by the SQL script) are not fully clear and need clarification to ensure correct migration.
*   **Job Table Schema:** The exact schema and all update/read operations for the `job_table` (implied by job activation/deactivation comments) must be defined for BigQuery.
*   **`semi_auto` Migration Bucket:** The "semi_auto" bucket suggests that manual intervention and detailed design are required due to potential complexities not fully captured by automated tools.

## 8. Build Plan

The migration will proceed with the following steps:

1.  **Define BigQuery Schemas:** Create DDL for target BigQuery tables:
    *   `bq_dataset.dwtk_meldungen` (migrated source)
    *   `bq_dataset.sof_ta_cntrct_crs` (migrated source)
    *   `bq_dataset.sof_ta_cntrct_crs2` (migrated target)
    *   `bq_dataset.via` (migrated target)
    *   `bq_dataset.job_table` (for job status management)
    *   `bq_dataset.job_run_log` (for job execution logging and record counts)
    (Language: BigQuery SQL)

2.  **Data Ingestion:** Establish data pipelines to ingest historical and incremental data from Oracle `DWTK_MELDUNGEN` and `SOF$TA_CNTRCT_CRS` into their respective BigQuery tables.
    (Language: Dataflow/Dataproc/BigQuery Data Transfer Service)

3.  **Reimplement Oracle Packages/Utilities:** Develop BigQuery UDFs or helper stored procedures to replicate the functionality of Oracle packages `DWPA_UTIL_SKRIPT` and `CR`, and the logic from the sourced KornShell utility scripts.
    (Language: BigQuery SQL)

4.  **Translate and Encapsulate Core SQL Logic:**
    *   Translate `d_ausd_v_ta_cntrct_crs2.sql` content into BigQuery Standard SQL.
    *   Encapsulate this logic into a BigQuery Stored Procedure, e.g., `sp_d_ausd_v_ta_cntrct_crs2`, ensuring it can accept necessary parameters and return the count of affected records.
    (Language: BigQuery SQL)

5.  **Develop Job Preparation Stored Procedure:** Create a BigQuery Stored Procedure, e.g., `sp_job_prepare`, to manage job activation/deactivation and registration within `bq_dataset.job_table`.
    (Language: BigQuery SQL)

6.  **Create Main Orchestration Stored Procedure:** Develop the main BigQuery Stored Procedure, e.g., `control_k_ausd_v_ta_cntrct_crs2`, that:
    *   Accepts `p_JobKennung` and `p_EintragsNr` as input parameters.
    *   Calls `sp_job_prepare`.
    *   Calls `sp_d_ausd_v_ta_cntrct_crs2` (passing parameters and capturing record count).
    *   Logs job completion and record counts to `bq_dataset.job_run_log`.
    *   Includes BigQuery `RAISE` statements for error handling.
    (Language: BigQuery SQL)

7.  **Orchestration Layer (Optional but Recommended):** If complex scheduling, dependency management, or integration with other GCP services is required, create a Cloud Composer (Airflow) DAG to trigger `control_k_ausd_v_ta_cntrct_crs2`.
    (Language: Python for Airflow DAG)