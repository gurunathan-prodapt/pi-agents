# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh

## 1. Purpose & Scope

This document outlines the migration design for the `r_ausd_bp_ta_rn_vertrag.ksh` KornShell script. This script functions as an orchestration wrapper, responsible for the initial provisioning of selected base products for BERT (Basisprodukte fï¿½r BERT). Its core responsibilities include:
*   Parsing command-line parameters (`-s` for Stichtag/reference date, `-l` for Wiederanlaufwert/restart threshold).
*   Loading environment variables and shared utility scripts.
*   Determining a default `Stichtag` if not provided.
*   Initializing job-specific logging, tracking job entry numbers, and handling status.
*   Invoking a downstream "core" script, `k_ausd_bp_ta_rn_vertrag.ksh`, with resolved parameters.
*   Implementing robust error handling using shell `trap` mechanisms and a shared error framework.
*   Marking the job as successful upon the successful completion of the core script.

The scope of this migration focuses on re-platforming this orchestration logic from a KornShell script to a Google Cloud Platform (GCP) environment, leveraging BigQuery Stored Procedures for core logic and Cloud Composer (Apache Airflow) for overall orchestration.

## 2. Source Inventory

*   **File Name:** `r_ausd_bp_ta_rn_vertrag.ksh`
*   **Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh`
*   **Technology:** KornShell Script
*   **Category:** Shell, Orchestration Script
*   **Complexity Tier:** Medium
*   **Automation Bucket:** Semi-Automatic
*   **Summary:** A KornShell script designed to prepare selected base products for BERT, managing date determination and passing parameters to a core script (`k_ausd_bp_ta_rn_vertrag.ksh`). It handles parameter validation, logging, and error management, acting as a pipeline orchestrator.

## 3. Target Architecture

The target architecture on Google Cloud Platform will consist of:

*   **Orchestration Layer:** Cloud Composer (Apache Airflow) will manage the overall workflow, triggering the BigQuery components, handling parameter passing, and monitoring execution. A Python DAG will be developed to encapsulate the wrapper's logic.
*   **Data Processing Layer:** BigQuery Stored Procedures will replace the procedural logic of the KornShell script.
    *   A main BigQuery Stored Procedure, e.g., `project.dataset.ausd_bp_ta_rn_vertrag_wrapper`, will implement the parameter parsing, date defaulting, and job logging.
    *   This wrapper procedure will then invoke the migrated version of the `k_ausd_bp_ta_rn_vertrag.ksh` script, likely another BigQuery Stored Procedure.
*   **Logging and Monitoring:** BigQuery tables will serve as the persistent storage for job logs and error records, replacing the file-based logging mechanism. Cloud Logging will capture execution logs from Airflow and BigQuery.
    *   `project.dataset.job_log`: To store job execution details, similar to the `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK` functionalities.
    *   `project.dataset.job_error_log`: To store error messages and details, replacing `DWMSG_MeldeFehler` output.

## 4. Data Flow & Lineage

**Source (Legacy):**

1.  **Inputs:**
    *   Command-line parameters: `-s <Stichtag DDMMYYYY>`, `-l <Wiederanlaufwert>`
    *   Environment variables sourced from `$HOME/.dw_init`
    *   System date (via `DWDate_Gib_Zeitraum` from `h_alis_date.ksh`)
2.  **Processing (r_ausd_bp_ta_rn_vertrag.ksh):**
    *   Loads utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
    *   Parses and validates input parameters (`p_stichtag`, `p_wiederanlaufWert`).
    *   Determines default `p_stichtag` if not provided (falls back to `v_sysdate`).
    *   Initializes job logging and error handling.
    *   **Invokes** the core script: `k_ausd_bp_ta_rn_vertrag.ksh` with parameters (`-j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert}`).
3.  **Outputs:**
    *   Console `print` statements.
    *   Log file (`$LogDatei`) via redirection and `DWMSG_*` functions.
    *   Status updates via `DWMSG_SetzeStatusOK`.
    *   Exit codes (0 for success, non-zero for errors).

**Target (GCP):**

1.  **Inputs:**
    *   Airflow DAG parameters (e.g., `stichtag`, `wiederanlaufwert`).
2.  **Processing (Cloud Composer DAG -> BigQuery Stored Procedure):**
    *   An Airflow DAG will trigger the `ausd_bp_ta_rn_vertrag_wrapper` BigQuery Stored Procedure.
    *   The BigQuery SP will handle parameter defaults and validation.
    *   Logging actions (job start, progress, completion, errors) will insert records into `job_log` and `job_error_log` tables.
    *   The `ausd_bp_ta_rn_vertrag_wrapper` SP will then **CALL** the migrated `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure, passing the necessary parameters.
3.  **Outputs:**
    *   Log entries in BigQuery `job_log` and `job_error_log` tables.
    *   BigQuery result sets from the called procedures (if any).
    *   Cloud Logging for Airflow task execution and BigQuery query logs.

The overall flow is an orchestration (`r_ausd_bp_ta_rn_vertrag.ksh`) invoking a core processing script (`k_ausd_bp_ta_rn_vertrag.ksh`), which in turn is likely responsible for reading from `DWH$TA_C_VERTRAG` and writing other outputs. This dependency needs to be explicitly maintained.

## 5. Transformation Logic

| Legacy KornShell Logic                                                                       | Target BigQuery/Airflow Transformation                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| :------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `#!/bin/ksh`                                                                                 | Replaced by Python Airflow DAG definition (`.py` file) and BigQuery SQL Stored Procedure.                                                                                                                                                                                                                                                                                                                                                                                                  |
| Parameter Parsing (`getopts -s -l`)                                                          | Airflow DAG parameters or direct parameters to the BigQuery Stored Procedure. The SP will use `IFNULL` or `COALESCE` for defaulting.                                                                                                                                                                                                                                                                                                                                                        |
| Usage Message (`usage()` function)                                                           | Replaced by Airflow DAG documentation or parameter descriptions. Error messages for missing/invalid parameters will be raised as BigQuery errors or handled by Airflow task failures.                                                                                                                                                                                                                                                                                                       |
| Sourcing environment (`. $HOME/.dw_init`)                                                    | Environment variables will be managed via Airflow Connections, Variables, or directly within the BigQuery Stored Procedure definitions. Sensitive information should use Secret Manager.                                                                                                                                                                                                                                                                                                     |
| Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`)             | These shared functionalities will be reimplemented as modular BigQuery UDFs (User-Defined Functions) or BigQuery Stored Procedures, or directly integrated into the main wrapper SP. Date handling (`DWDate_Gib_Zeitraum`) will use BigQuery's `FORMAT_DATE`, `CURRENT_DATE`, etc.                                                                                                                                                                                                          |
| Error Handling (`set -e`, `trap`, `DWMSG_MeldeFehler`, `exit $ErrNr`)                        | BigQuery Stored Procedures will use `RAISE` for errors. Airflow tasks handle retries and failures, and any exceptions in the Python DAG or BigQuery SPs will be caught and logged to `job_error_log`. Airflow's built-in monitoring and alerting will replace `trap` functionality.                                                                                                                                                                                                      |
| Job Logging (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `print`)      | Insert statements into `project.dataset.job_log` table for audit trail. `DWMSG_ErmittleNr` will translate to `SELECT IFNULL(MAX(eintragsnr), 0) + 1 FROM job_log`. Console prints will be replaced by Airflow task logs and BigQuery console output.                                                                                                                                                                                                                                          |
| Invoking Core Script (`${Name_Kernskript} ... >> $LogDatei 2>&1`)                            | The BigQuery wrapper Stored Procedure will use a `CALL` statement to invoke the migrated core processing BigQuery Stored Procedure (e.g., `CALL project.dataset.k_ausd_bp_ta_rn_vertrag(v_jobkennung, v_stichtag, v_eintragsnr, v_wiederanlaufWert)`). Logging redirection will be implicitly handled by BigQuery's audit logs and the `job_log` table. |
| Setting Status OK (`DWMSG_SetzeStatusOK`)                                                    | An `UPDATE` statement on the `project.dataset.job_log` table to mark the job as 'OK' or 'COMPLETED'.                                                                                                                                                                                                                                                                                                                                                                                         |
| Default `p_wiederanlaufWert=0`                                                               | Handled by an `IFNULL` check in the BigQuery Stored Procedure: `SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, '0');`                                                                                                                                                                                                                                                                                                                                                                  |
| Default `p_stichtag=$v_sysdate`                                                              | Handled by an `IFNULL` check in the BigQuery Stored Procedure: `SET v_stichtag = IFNULL(p_stichtag, v_sysdate);`                                                                                                                                                                                                                                                                                                                                                                           |

## 6. External Dependencies

The `r_ausd_bp_ta_rn_vertrag.ksh` script has the following external dependencies:

*   **Utility Scripts (Internal):**
    *   `.dw_init`: An initialization script for the shell environment.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utilities (e.g., `DWDate_Gib_Zeitraum`).
    *   **Replacement:** These will be replaced by equivalent BigQuery UDFs or Stored Procedures, or their functionality will be absorbed into the main wrapper SP. Environment sourcing will be handled by Airflow's environment management.
*   **Core Processing Script:**
    *   `k_ausd_bp_ta_rn_vertrag.ksh`: This is the main script invoked by the wrapper.
    *   **Replacement:** This script is a critical dependency and must be migrated separately, likely to its own BigQuery Stored Procedure or a data transformation job (e.g., Dataflow, Dataproc, or another BigQuery job) depending on its internal logic. The wrapper SP will `CALL` this migrated component.
*   **Database Table:**
    *   `DWH$TA_C_VERTRAG`: Mentioned in `h_alis_date.ksh` (commented out in the provided script, but a reference was found in `file_analysis.analysis.references_out`).
    *   **Replacement:** This likely represents a source table for the core processing. In BigQuery, it would be migrated to `project.dataset.DWH_TA_C_VERTRAG`. The actual reads would occur in the migrated `k_ausd_bp_ta_rn_vertrag` component.

## 7. Unresolved / Risks

*   **Core Script (`k_ausd_bp_ta_rn_vertrag.ksh`) Logic:** The content and complexity of `k_ausd_bp_ta_rn_vertrag.ksh` are currently unknown. Its migration strategy (BQ SP, Dataflow, etc.) will heavily influence the overall design and effort.
*   **Shared Utilities Scope:** While the current script sources several utility `.ksh` files, a full understanding of their scope and whether they are used by other processes is needed. If they are truly shared and complex, they might need a more centralized migration approach (e.g., a common BigQuery utility library or a set of generic Python functions in Airflow).
*   **`trap` Semantics:** The direct translation of `trap` (signal handling) from shell to BigQuery SQL or Python in Airflow is not 1:1. Airflow's retry mechanisms and error callbacks provide a robust alternative, but any specific logic embedded in the `trap` handlers needs careful re-implementation.
*   **Parameter Format Strictness:** The current script expects `DDMMYYYY` for dates. Ensuring this format is maintained or properly converted during parameter passing in Airflow and BigQuery is crucial.
*   **`AL??` Comments:** The presence of `AL??` comments suggests potential alternative or deprecated logic that needs to be clarified during migration to avoid porting unnecessary or incorrect code.

## 8. Build Plan

The build plan will involve creating the necessary BigQuery assets and Airflow DAGs:

1.  **Define BigQuery Audit/Log Tables (SQL DDL):**
    *   `project.dataset.job_log` (to store job execution history, status, stichtag info).
    *   `project.dataset.job_error_log` (to store detailed error messages).
2.  **Migrate Utility Functions (BigQuery UDFs/SPs):**
    *   Create BigQuery equivalents for date handling (e.g., `DWDate_Gib_Zeitraum`).
    *   Implement parameter validation and error reporting logic directly within the wrapper SP or as helper UDFs.
3.  **Develop `ausd_bp_ta_rn_vertrag_wrapper` BigQuery Stored Procedure (SQL):**
    *   Implement parameter input and defaulting logic.
    *   Integrate logging into `job_log` table.
    *   Implement parameter validation and error handling using `RAISE`.
    *   Include a `CALL` statement for the migrated `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure (placeholder until migrated).
    *   Update `job_log` with final status.
4.  **Create Airflow DAG (`ausd_bp_ta_rn_vertrag_dag.py`):**
    *   Define a Python Airflow DAG that orchestrates the execution.
    *   Use `BigQueryExecuteStoredProcedureOperator` or `BigQueryOperator` to call the `ausd_bp_ta_rn_vertrag_wrapper` BigQuery Stored Procedure, passing runtime parameters.
    *   Configure task dependencies, retries, and error handling within the DAG.
    *   Integrate Airflow's native logging to Cloud Logging.
5.  **Migrate `k_ausd_bp_ta_rn_vertrag.ksh` (separate effort):**
    *   Analyze `k_ausd_bp_ta_rn_vertrag.ksh` to determine its migration path (e.g., another BigQuery Stored Procedure for SQL-heavy logic, Dataflow/Dataproc for complex transformations).
    *   Replace its invocation in `ausd_bp_ta_rn_vertrag_wrapper` once migrated.