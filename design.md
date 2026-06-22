# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_cntrct_crs.ksh`. This script serves as an orchestration and wrapper layer for a contract data reconciliation process specifically targeting the `ta_cntrct_crs` table. It does not contain the core business logic for reconciliation but rather handles parameter parsing, environment setup, robust logging, error handling, and the invocation of a core data processing script. The primary goal is to re-platform this orchestration logic to Google Cloud Platform, leveraging BigQuery Stored Procedures and potentially Cloud Composer for scheduling and workflow management.

## 2. Source Inventory
The job is composed of a single source file:
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh`
    *   **Technology:** Shell Script (KornShell)
    *   **Purpose:** Orchestration Script / Job Wrapper
    *   **Complexity Tier:** Undetermined from metadata, but inferred as **Medium** due to custom error handling, environment sourcing, and script invocation logic that requires re-engineering for the target platform.
    *   **Automation Bucket:** Undetermined from metadata, but inferred as **B2 (Semi-Automated)** or **B4 (Redesign)** given the need to re-platform shell-specific constructs to BigQuery Stored Procedures and potentially Airflow.

## 3. Target Architecture
The migrated solution will primarily reside within Google BigQuery, utilizing:
*   **BigQuery Stored Procedures:** The wrapper logic, including parameter handling, job metadata management, and error handling, will be implemented as a BigQuery Stored Procedure. This procedure will call a separate core BigQuery Stored Procedure (to be derived from `k_ausd_v_ta_cntrct_crs.ksh`).
*   **BigQuery Tables for Auditing/Logging:** Dedicated BigQuery tables will replace the existing file-based logging and `DWMSG_*` framework functions for tracking job execution status, errors, and metadata.
*   **Google Cloud Composer (Apache Airflow):** For job scheduling and orchestration, replacing the script's direct execution and allowing for broader workflow management capabilities.

## 4. Data Flow & Lineage
The `lineage_edges` query for this run returned no explicit dependency information. Therefore, the data flow and lineage are inferred from the static analysis and the source code of `r_ausd_v_ta_cntrct_crs.ksh`.

The current process flow is:
1.  **Environment Initialization:** The script sources `$HOME/.dw_init` and other utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) to set up the environment and error handling framework.
2.  **Parameter Parsing:** Command-line arguments (`-h`, `-s`, `-l`) are parsed using `getopts`.
3.  **Job Metadata & Logging Setup:** Unique job identifiers are generated, a log file name is determined, and a job entry is created using `DWMSG_*` functions. Stichtag (key date) information is also set.
4.  **Error Trapping:** Shell `trap` commands are set up to catch `INT` (interrupt) and `ERR` (command error) signals, invoking custom error handling routines (`DWMSG_Fehlerbehandlung`).
5.  **Core Script Invocation:** The primary function of this script is to execute `k_ausd_v_ta_cntrct_crs.ksh` with specific parameters (`-j $JobKennung -f ${DW_EintragsNr}`). The output of this core script is redirected to the established log file.
6.  **Status Update:** Upon successful completion of the core script, a success message is logged, and the job status is updated via `DWMSG_SetzeStatusOK`. If the core script fails or an error is trapped, the error handling routines are invoked, and the script exits with an appropriate error code.

In the target BigQuery environment:
*   An Airflow DAG would trigger the main BigQuery Stored Procedure.
*   The wrapper Stored Procedure handles parameter validation, audit log writes (start/end/error), and calls the core reconciliation Stored Procedure.
*   The core reconciliation Stored Procedure (migrated from `k_ausd_v_ta_cntrct_crs.ksh`) will perform the actual data transformations and updates to `ta_cntrct_crs` or related tables.

## 5. Transformation Logic

The KornShell wrapper script's logic will be transformed into a BigQuery Stored Procedure, and its auxiliary functions will be mapped to BigQuery's capabilities or audit tables.

### Original KornShell Snippets and Target BigQuery Equivalents:

#### **A. Environment Sourcing:**
*   **Original:** `. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, etc.
*   **Target:** These environment variables and utility functions will be replaced by:
    *   BigQuery dataset/project specific configurations.
    *   Constants or variables within the BigQuery Stored Procedure.
    *   BigQuery audit log procedures for error messaging and logging.

#### **B. Parameter Parsing:**
*   **Original:** `getopts` loop for `-h`, `-s`, `-l`. Includes error codes `193` (missing arg) and `192` (unknown param).
*   **Target (BigQuery Stored Procedure):** The procedure will accept `p_s` and `p_l` as input parameters. Validation logic will be implemented using `IF` statements, and `RAISE` will be used for error conditions, mapping to the original error codes and messages.

```sql
-- Parameter validation equivalent to getopts handling
IF p_s IS NULL OR p_l IS NULL THEN
  SET v_status = 'FAILED';
  SET v_error_code = 193;
  SET v_message = 'Missing required argument';
  -- INSERT into job_audit_log
  RAISE USING MESSAGE = 'Missing required argument';
END IF;
```

#### **C. Job Metadata, Logging & Error Handling:**
*   **Original:** `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`. Shell `trap` for `INT` and `ERR`.
*   **Target (BigQuery Audit Tables & Stored Procedure):**
    *   A `job_audit_log` table will store all job execution details, replacing the `DWMSG_*` framework.
    *   Procedure variables (`v_job_id`, `v_job_name`, `v_status`, etc.) will manage job state.
    *   `INSERT` statements will record job start.
    *   `EXCEPTION WHEN ERROR THEN` blocks will catch execution errors, update the audit log with `FAILED` status, and log error details.
    *   `UPDATE` statements will record job success.

```sql
-- Suggested audit table DDL
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log` (
  job_id STRING,
  job_name STRING,
  script_name STRING,
  status STRING,
  message STRING,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  run_date DATE,
  error_code INT64,
  error_detail STRING
);

-- BigQuery Stored Procedure Wrapper Pseudocode (excerpt)
CREATE OR REPLACE PROCEDURE `project.dataset.sp_vertragsdatenabgleich`(\n  IN p_s STRING,\n  IN p_l STRING\n)\nBEGIN
  DECLARE v_job_id STRING DEFAULT GENERATE_UUID();
  -- ... other declarations ...

  -- Job start log
  INSERT INTO `project.dataset.job_audit_log`
  VALUES (\n    v_job_id, v_job_name, v_script_name, v_status, 'Job started',\n    v_start_ts, NULL, v_run_date, NULL, NULL\n  );

  BEGIN
    -- Equivalent to calling the core shell script
    CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`(v_job_id, p_s, p_l);

    SET v_status = 'SUCCESS';
    SET v_message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet';
    UPDATE `project.dataset.job_audit_log` SET status = v_status, message = v_message, end_ts = CURRENT_TIMESTAMP() WHERE job_id = v_job_id;

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'FAILED';
    SET v_error_detail = @@error.message;
    UPDATE `project.dataset.job_audit_log` SET status = v_status, message = 'AppError: Abbruch', end_ts = CURRENT_TIMESTAMP(), error_detail = v_error_detail WHERE job_id = v_job_id;
    RAISE USING MESSAGE = 'AppError: Abbruch';
  END;
END;
```

#### **D. Core Script Invocation:**
*   **Original:** `${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr}` calling `k_ausd_v_ta_cntrct_crs.ksh`.
*   **Target (BigQuery Stored Procedure):** The wrapper procedure will call another BigQuery Stored Procedure that encapsulates the logic from `k_ausd_v_ta_cntrct_crs.ksh`.

```sql
CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`(v_job_id, p_s, p_l);
```

## 6. External Dependencies
Based on the provided `lineage_assembled_jobs` and `file_analysis`, there are no explicitly identified external systems like Oracle, SFTP, or S3 that `r_ausd_v_ta_cntrct_crs.ksh` directly interacts with. All listed dependencies are other KornShell scripts or environment files within the same legacy source repository.

The `ta_cntrct_crs` is referenced as a table, which is assumed to be a source table that will either be migrated to BigQuery or accessible by BigQuery as an external table.

The external references are:
*   `$HOME/.dw_init`: An environment initialization file. Will be replaced by BigQuery/GCP environment variables or configuration.
*   `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`: Utility KornShell scripts for error messaging, parameter handling, and date functions. These functionalities will be absorbed directly into the BigQuery Stored Procedure or implemented as separate BigQuery functions/procedures.
*   `k_ausd_v_ta_cntrct_crs.ksh`: This is the core business logic script. It represents a significant dependency whose content will dictate the design of the core BigQuery Stored Procedure. Its own external dependencies (if any) are currently unknown and will need separate analysis.

## 7. Unresolved / Risks
*   **Core Script Content:** The most significant unresolved item is the actual business logic within `k_ausd_v_ta_cntrct_crs.ksh`. Its content will determine the complexity of the core BigQuery Stored Procedure and whether additional GCP services (e.g., Dataflow for complex transformations, Cloud Functions for specific external integrations) are required.
*   **Shell-Specific Features:** Direct emulation of shell `trap` commands and environment sourcing in BigQuery is not possible. These require re-engineering into BigQuery's error handling and configuration management paradigms.
*   **Missing Metadata:** The `file_complexity` and `automation_rate` metadata for this script were not available, which makes it challenging to formally assess its migration effort and automation bucket. Based on the analysis, it is likely a semi-automated or redesign effort.
*   **Parameter Usage:** The specific usage and meaning of parameters `-s` and `-l` are not explicitly detailed in the wrapper script but are passed to the core script. Their definition will be critical for the core script's migration.

## 8. Build Plan
1.  **Define BigQuery Audit Log Table:**
    *   Language: BigQuery DDL
    *   File: `bq_ddl_job_audit_log.sql`
    *   Content: `CREATE TABLE IF NOT EXISTS project.dataset.job_audit_log (...)` (as per pseudocode in Section 5).
2.  **Develop BigQuery Wrapper Stored Procedure:**
    *   Language: BigQuery SQL
    *   File: `sp_vertragsdatenabgleich.sql`
    *   Content: The `CREATE OR REPLACE PROCEDURE` statement including parameter validation, job logging (using the audit table), error handling, and the call to the core reconciliation procedure (as per pseudocode in Section 5).
3.  **Analyze and Develop Core Reconciliation Stored Procedure:**
    *   This step requires separate analysis of `k_ausd_v_ta_cntrct_crs.ksh`.
    *   Language: BigQuery SQL (or other GCP services like Dataflow/Python if shell script contains non-SQL logic).
    *   File: `sp_ausd_v_ta_cntrct_crs.sql` (placeholder).
4.  **Create Cloud Composer (Airflow) DAG:**
    *   Language: Python
    *   File: `dag_vertragsdatenabgleich.py`
    *   Content: An Airflow DAG that schedules and triggers the `sp_vertragsdatenabgleich` BigQuery Stored Procedure. This DAG will replace the legacy scheduler's role.
5.  **IAM and Permissions Configuration:**
    *   Configure service accounts with appropriate roles for BigQuery access and Cloud Composer execution.
6.  **Deployment:**
    *   Deploy DDL and Stored Procedures to BigQuery.
    *   Deploy the Airflow DAG to Cloud Composer.