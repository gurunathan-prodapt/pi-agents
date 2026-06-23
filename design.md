# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh

## 1. Purpose & Scope
This document outlines the migration plan for `k_ausd_v_ta_p_discount_rr.ksh`, a KornShell control script. The primary purpose of this script is to orchestrate the execution of a SQL script (`d_ausd_v_ta_p_discount_rr.sql`) for processing data related to the `ta_p_discount_rr` table. It handles parameter parsing, environment setup, error handling, and job control (e.g., ignoring active jobs and updating job status). The script itself does not contain core data transformation logic, but rather manages the execution flow and integrates with utility scripts for common functionalities like logging and parameter validation.

## 2. Source Inventory
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh`
    *   **Technology:** KornShell
    *   **Category:** Shell
    *   **Tool:** KornShell
    *   **Complexity Tier:** Unknown (file_complexity returned no rows)
    *   **Automation Bucket:** semi_auto
    *   **Summary:** Control script for `r_ausd_vertrag.ksh` handling parameter parsing, environment setup, error handling, and orchestrating the execution of an SQL script to process data for `ta_p_discount_rr`.

## 3. Target Architecture
The target platform is Google BigQuery. The existing KornShell script will be migrated to a BigQuery stored procedure to manage the orchestration logic. The SQL script it invokes (`d_ausd_v_ta_p_discount_rr.sql`) will also be migrated to a separate BigQuery stored procedure or SQL script.

The target architecture will consist of:
*   **BigQuery Stored Procedure (Orchestration):** `project.dataset.r_ausd_vertrag` (named after the original script's implied functionality of controlling `r_ausd_vertrag.ksh`) will encapsulate the parameter parsing, validation, job control, and invocation of the core data processing logic.
*   **BigQuery Stored Procedure (Data Processing):** `project.dataset.d_ausd_v_ta_p_discount_rr` will contain the SQL logic extracted from the original `d_ausd_v_ta_p_discount_rr.sql` file.
*   **BigQuery Control Tables:** Dedicated tables for `job_control`, `job_error_log`, and `job_audit` will be created to manage job status, log errors, and record execution metadata (e.g., processed record counts), replacing the temporary file and implicit job tracking.
*   **Scheduler:** A cloud-native scheduler (e.g., Cloud Composer/Airflow, Cloud Workflows, or BigQuery Scheduled Queries) will be configured to trigger the main orchestration stored procedure.

## 4. Data Flow & Lineage
The original script's data flow is primarily orchestration with an invocation of an external SQL script:
1.  **Invocation:** The `k_ausd_v_ta_p_discount_rr.ksh` script is executed, likely by an external scheduler (not explicitly identified in lineage_edges for this run).
2.  **Environment Initialization:** It sources `$HOME/.dw_init` to set up the environment.
3.  **Utility Script Loading:** It sources several KornShell utility scripts:
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error messaging)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utilities)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing/validation)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus related utilities)
4.  **Parameter Processing:** It uses `getopts` to parse command-line parameters (`-j` for `p_JobKennung`, `-f` for `p_EintragsNr`).
5.  **Parameter Validation:** It calls `pruefeParameterGesetzt` (from `h_alis_parameter.ksh`) to validate required parameters.
6.  **Job Control (Implicit):** The script sets `v_TabName='ta_p_discount_rr'`. The `starteSQLSkript` function (from `h_alis_sqlplus.ksh` or similar) is called, which is assumed to handle active job checks and updates to a job table.
7.  **SQL Script Execution:** It invokes `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_p_discount_rr.sql` through the `starteSQLSkript` wrapper. This SQL script is responsible for the actual data processing related to `ta_p_discount_rr`.
8.  **Record Count Capture:** After the SQL script execution, it reads the number of processed records from a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_p_discount_rr_$$.tmp`).
9.  **Job Completion:** It prints a completion message.

In BigQuery, this flow will be:
1.  **Scheduler Trigger:** A scheduler invokes `project.dataset.r_ausd_vertrag` stored procedure with parameters.
2.  **Parameter Validation:** Parameters are validated directly within the stored procedure.
3.  **Job Control:** `project.dataset.job_control` table is checked for active jobs, and updated for the current job status (start, complete, error).
4.  **Data Processing Call:** `project.dataset.d_ausd_v_ta_p_discount_rr` stored procedure is called, performing the data transformations.
5.  **Record Count & Audit:** The data processing stored procedure returns the record count, which `project.dataset.r_ausd_vertrag` then logs to `project.dataset.job_audit`.
6.  **Error Handling:** Errors are logged to `project.dataset.job_error_log`.

## 5. Transformation Logic
The `k_ausd_v_ta_p_discount_rr.ksh` script itself does not perform data transformations. Its logic is purely orchestration. The actual transformation logic resides in the SQL script `d_ausd_v_ta_p_discount_rr.sql`, which is not part of the provided components but is invoked by this script.

The migration of the orchestration logic involves:
*   **Parameter Handling:** Shell `getopts` will be replaced by direct BigQuery stored procedure input parameters (`p_JobKennung`, `p_EintragsNr`).
*   **Parameter Validation:** The `pruefeParameterGesetzt` calls will be replaced by `IF` statements and `IS NULL` / `TRIM('') = ''` checks within the BigQuery stored procedure.
*   **Error Handling:** Shell `ErrNr`, `ErrArg`, `DWMSG_MeldeFehler`, and `exit` will be replaced by BigQuery `DECLARE` variables, `INSERT` statements into `job_error_log`, and `RETURN` statements from the stored procedure.
*   **Job Control:** The logic to ignore active jobs and to mark job status will be implemented using `SELECT`, `INSERT`, and `UPDATE` statements against a `project.dataset.job_control` table.
*   **SQL Script Invocation:** The `starteSQLSkript` call will be replaced by a `CALL` statement to the `project.dataset.d_ausd_v_ta_p_discount_rr` BigQuery stored procedure.
*   **Record Count:** Reading from the temporary file (`tmpFile`) will be replaced by an `OUT` parameter from the `project.dataset.d_ausd_v_ta_p_discount_rr` stored procedure, which will then be recorded in the `project.dataset.job_audit` table.

## 6. External Dependencies
*   **Environment Initialization (`.dw_init`):** The `$HOME/.dw_init` file, which sets up environment variables, will be replaced by BigQuery connection configurations, environment variables managed by the orchestration tool (e.g., Airflow variables), or configuration tables in BigQuery.
*   **Sourced Utility Scripts:**
    *   `f_alis_msgerr.ksh`: Error logging will be replaced by BigQuery's native logging capabilities (e.g., Cloud Logging) or a custom `project.dataset.job_error_log` table.
    *   `h_alis_date.ksh`: Date utilities will be replaced by BigQuery's extensive date and time functions.
    *   `h_alis_parameter.ksh`: Parameter parsing and validation logic will be integrated directly into the main BigQuery stored procedure.
    *   `h_alis_sqlplus.ksh`: SQL*Plus specific routines will be deprecated. The direct invocation of BigQuery stored procedures and SQL scripts makes these unnecessary.
*   **Database Interactions:** The implicit database interactions via `starteSQLSkript` (likely Oracle) will be replaced by direct BigQuery SQL operations. The target table `ta_p_discount_rr` will be recreated in BigQuery.

No explicit `external_systems` were identified in the initial lineage query for this job, suggesting the primary external dependency is the underlying database where `d_ausd_v_ta_p_discount_rr.sql` operates.

## 7. Unresolved / Risks
*   **Missing SQL Script (`d_ausd_v_ta_p_discount_rr.sql`):** The core data transformation logic in this SQL script is not provided as part of this job's components. Its content must be obtained and migrated separately as a prerequisite for completing the `project.dataset.d_ausd_v_ta_p_discount_rr` BigQuery stored procedure.
*   **Detailed Logic of Sourced Utilities:** While the general purpose of the sourced `.ksh` files is understood, their precise internal logic (especially `h_alis_parameter.ksh` and `f_alis_msgerr.ksh`) needs further analysis to ensure faithful replication or appropriate replacement in BigQuery.
*   **`starteSQLSkript` Functionality:** The `starteSQLSkript` function likely contains critical job control and execution logic. Its full implementation details (especially concerning "active job ignoring" and how it writes to the temporary file) must be thoroughly understood and replicated in the BigQuery orchestration procedure and control tables.
*   **Complexity Tier:** The `file_complexity` table did not return a specific tier, which indicates a potential gap in the automated assessment of this file's migration difficulty beyond being `semi_auto`. Further manual review might be required.
*   **Original `r_ausd_vertrag.ksh` Context:** The script is described as a "Kontrollscript zu r_ausd_vertrag.ksh". Understanding the `r_ausd_vertrag.ksh` context might reveal further dependencies or orchestrational patterns that need to be considered.

## 8. Build Plan
1.  **Define BigQuery Schemas:** Create DDL for `job_control`, `job_error_log`, and `job_audit` tables in the target BigQuery dataset (`project.dataset`). (BigQuery DDL)
2.  **Migrate Core SQL Logic:**
    *   Obtain the source code for `d_ausd_v_ta_p_discount_rr.sql`.
    *   Convert this SQL script into a BigQuery-compatible SQL stored procedure, `project.dataset.d_ausd_v_ta_p_discount_rr`, which will encapsulate the data transformation logic. This procedure should accept necessary input parameters (e.g., `p_EintragsNr`, `p_JobKennung`) and return the number of processed records via an `OUT` parameter. (BigQuery SQL)
3.  **Create Orchestration Stored Procedure:** Develop the `project.dataset.r_ausd_vertrag` BigQuery stored procedure, implementing the orchestration logic:
    *   Parameter declaration and validation.
    *   Interactions with `project.dataset.job_control` table for active job checks, status updates (STARTING, COMPLETED, FAILED).
    *   Error logging to `project.dataset.job_error_log`.
    *   Calling `project.dataset.d_ausd_v_ta_p_discount_rr` to perform data processing.
    *   Logging processed record counts and final job status to `project.dataset.job_audit`. (BigQuery SQL)
4.  **Scheduler Integration:** Configure a scheduler (e.g., create an Airflow DAG if using Cloud Composer) to regularly invoke the `project.dataset.r_ausd_vertrag` stored procedure, passing the required parameters. (GCP Configuration / Python for Airflow DAG)
5.  **Testing:** Develop unit and integration tests for both BigQuery stored procedures and the scheduler configuration to ensure functional equivalence and data integrity. (BigQuery SQL / Python for Orchestration Tests)