# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh

## 1. Purpose & Scope
This document outlines the migration design for `r_ausd_v_ta_vvl_upgrade.ksh`, a KornShell wrapper script responsible for orchestrating a contract data reconciliation job for the `ta_vvl_upgrade` table. Its primary functions include environment initialization, command-line parameter parsing, setup of job and error logging, invocation of a core "kernel" script (likely `k_ausd_v_ta_vvl_upgrade.ksh`), and handling job success/failure. The scope of this migration is to re-implement this orchestration logic on the Google Cloud Platform, specifically utilizing BigQuery scripting capabilities, with the goal of replacing the legacy KornShell environment.

## 2. Source Inventory
The job consists of a single primary source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Not explicitly determined by analysis, but generally considered **Medium** due to external script sourcing, parameter parsing, and custom error/logging framework.
    *   **Automation Bucket:** `semi_auto`
    *   **Summary:** This is an orchestration script for the `ta_vvl_upgrade` data reconciliation process. It sets up the environment, parses input parameters (`-h`, `-s`, `-l`), initializes a custom logging and error handling framework (`DWMSG_*` utilities), and then executes a core KornShell script (`k_ausd_v_ta_vvl_upgrade.ksh`). It handles job status updates and logging.

## 3. Target Architecture
The target architecture will leverage BigQuery's scripting features for the orchestration logic.

*   **BigQuery Stored Procedure/Script:** The KornShell wrapper script (`r_ausd_v_ta_vvl_upgrade.ksh`) will be migrated into a BigQuery SQL script or a stored procedure. This will handle parameter passing, job metadata initialization, and the orchestration of the core data transformation.
*   **Logging and Status Tables:** The custom `DWMSG_*` logging and error handling framework will be replaced by dedicated BigQuery tables:
    *   `job_log`: To store detailed log messages (timestamp, job number, job identifier, severity, message).
    *   `job_status`: To track the overall status of jobs (job number, job identifier, status, stichtag, updated timestamp).
*   **Kernel Script Migration:** The core data reconciliation logic, currently residing in `k_ausd_v_ta_vvl_upgrade.ksh`, will need to be separately migrated. The wrapper script will invoke this as a BigQuery Stored Procedure (e.g., `CALL project.dataset.k_ausd_v_ta_vvl_upgrade(JobKennung, DW_EintragsNr)`).
*   **Configuration Management:** Environment variables and sourced init files will be replaced by BigQuery procedure parameters, `DECLARE` variables, or configuration tables.
*   **Orchestration (Optional):** If the overall workflow requires external scheduling or dependency management, Cloud Composer (Airflow) or Cloud Workflows can be considered to trigger the BigQuery script/stored procedure. However, for this specific wrapper, direct BigQuery scripting is the primary target.

## 4. Data Flow & Lineage
The original script's data flow is primarily orchestrational. It does not directly perform data transformations but sets up the environment and executes a core script (`k_ausd_v_ta_vvl_upgrade.ksh`).

**Legacy Flow:**
1.  `r_ausd_v_ta_vvl_upgrade.ksh` starts execution.
2.  Sources environment (`. $HOME/.dw_init`) and utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
3.  Parses command-line arguments.
4.  Initializes `DWMSG_*` logging framework (generates job number, log file name, creates job entry).
5.  Sets up `trap` handlers for `INT` and `ERR` signals.
6.  Executes `k_ausd_v_ta_vvl_upgrade.ksh` with job identifier and entry number, redirecting output to a log file.
7.  Upon successful completion, updates job status to OK.
8.  On error (caught by `trap`), performs error handling and updates job status to ERROR.

**Target BigQuery Flow:**
1.  A BigQuery SQL script/stored procedure (`r_ausd_v_ta_vvl_upgrade_bq`) is executed.
2.  Parameters for the script (equivalent to `-s`, `-l`, `JobKennung`, `DW_EintragsNr`) are passed in or declared.
3.  Initializes `DECLARE` variables for program metadata (`ProgName`, `ProgVersion`).
4.  Generates a job entry number by querying `job_status` table.
5.  Inserts initial job status and log entries into `job_status` and `job_log` tables.
6.  `BEGIN...EXCEPTION...END` block wraps the core logic for error handling (replacing `trap`).
7.  Inside the `BEGIN` block:
    *   Prints job header information using `SELECT` statements (for console output) and `INSERT` into `job_log`.
    *   Invokes the migrated kernel script as a BigQuery Stored Procedure: `CALL project.dataset.k_ausd_v_ta_vvl_upgrade(JobKennung, DW_EintragsNr)`.
    *   Upon successful return, updates `job_status` to 'OK' and inserts a success message into `job_log`.
8.  In the `EXCEPTION` block:
    *   Updates `job_status` to 'ERROR' and inserts an error message into `job_log`.

**Lineage Note:** The lineage analysis did not find explicit `lineage_edges` for this file. The inferred lineage is that this script orchestrates the execution of `k_ausd_v_ta_vvl_upgrade.ksh`, which is expected to contain the core data transformation logic related to `ta_vvl_upgrade`.

## 5. Transformation Logic
The `r_ausd_v_ta_vvl_upgrade.ksh` script is a wrapper and contains minimal direct data transformation logic. Its "transformation" is primarily in managing the job execution context and status.

*   **Parameter Parsing:** The `getopts` logic for `-h`, `-s`, `-l` and error handling will be translated into BigQuery `IF` statements or explicit parameter declarations for the stored procedure. Unhandled parameters (`-s`, `-l`) need clarification if they impact core logic.
*   **Job Metadata:** The script captures `ProgName`, `ProgVersion`, and generates `JobKennung` and `v_sysdate`. This will map directly to `DECLARE` variables or procedure parameters in BigQuery.
*   **Error Handling and Logging:** The custom `DWMSG_*` functions and `trap` statements will be replaced by:
    *   `job_log` and `job_status` BigQuery tables for persistent logging and status tracking.
    *   `BEGIN...EXCEPTION...END` blocks in BigQuery SQL for robust error handling.
    *   `FORMAT_DATE` for date formatting.
*   **Kernel Script Invocation:** The line `${Name_Kernskript} ...` will be replaced by a `CALL` statement to the migrated BigQuery stored procedure for `k_ausd_v_ta_vvl_upgrade`.

## 6. External Dependencies
The script has several external dependencies:

*   **Environment Initialization (`. $HOME/.dw_init`):** This loads environment variables. In BigQuery, this will be handled by:
    *   Defining relevant variables as `DECLARE` statements within the BigQuery script/procedure.
    *   Passing values as procedure parameters.
    *   Potentially using a BigQuery configuration table for more complex environment settings.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These scripts contain the `DWMSG_*` functions and parameter/date handling logic.
    *   **Replacement:** The core functionality of these utilities (error messaging, job number generation, logging, date formatting) will be re-implemented directly within the BigQuery script using `INSERT` statements into log/status tables, `SELECT` for messaging, `FORMAT_DATE` for dates, and SQL logic for parameter validation.
*   **Core Kernel Script (`k_ausd_v_ta_vvl_upgrade.ksh`):** This is the most significant external dependency, as it contains the actual data reconciliation logic.
    *   **Replacement:** This script *must* be migrated into a BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_v_ta_vvl_upgrade`). The current wrapper's `CALL` to this script will then translate directly to a BigQuery `CALL` statement.
*   **Filesystem Logging:** The script writes to a log file (`$LogDatei`).
    *   **Replacement:** This will be replaced by inserting log entries into the `job_log` BigQuery table.

## 7. Unresolved / Risks
*   **Kernel Script (`k_ausd_v_ta_vvl_upgrade.ksh`) Content:** The full functionality and complexity of the invoked kernel script are unknown. Its migration is critical and will dictate the overall complexity of the data transformation. If it involves complex shell logic, external tools, or non-SQL operations, it will require careful design, potentially leading to a BigQuery stored procedure, Python UDFs, or even a Cloud Function/Cloud Run service.
*   **`getopts` Unhandled Parameters (`-s`, `-l`):** The script declares these parameters but does not show explicit handling for them. Their purpose and impact on the overall job need to be identified and incorporated into the BigQuery design.
*   **Environment Variables:** The exact content and usage of variables loaded from `$HOME/.dw_init` and `BERT_DIR_ROOT` need to be cataloged and mapped to BigQuery parameters or configuration tables.
*   **`DWMSG_*` Functionality:** While the general idea is to replace these with BigQuery tables, the exact schema and functionality of each `DWMSG_*` function (e.g., `DWMSG_MeldeFehler`, `DWMSG_SetzeStichtagInfo`) must be fully understood to ensure an accurate BigQuery re-implementation.

## 8. Build Plan
The migration build plan involves creating the following BigQuery components:

1.  **`job_log` Table DDL:**
    *   Define schema for logging.
2.  **`job_status` Table DDL:**
    *   Define schema for job status tracking.
3.  **`k_ausd_v_ta_vvl_upgrade` Stored Procedure (BigQuery SQL):**
    *   **Dependency:** This is the critical component that contains the actual data reconciliation logic and must be designed and built first.
4.  **`r_ausd_v_ta_vvl_upgrade_bq` BigQuery SQL Script/Stored Procedure:**
    *   **Language:** BigQuery SQL
    *   **Content:** Re-implement the wrapper logic, including:
        *   Parameter handling.
        *   Job metadata initialization.
        *   Integration with `job_log` and `job_status` tables.
        *   Error handling via `BEGIN...EXCEPTION...END`.
        *   `CALL` statement to the `k_ausd_v_ta_vvl_upgrade` stored procedure.
5.  **Configuration Objects (Optional):**
    *   If needed, BigQuery configuration tables or views to manage environment-specific parameters.

This ordered approach ensures that the core data processing logic (`k_ausd_v_ta_vvl_upgrade`) is in place before the wrapper that orchestrates it is built.