# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_cntrct_crs2.ksh`. The job's primary purpose is to act as a wrapper/orchestration script for a contract data reconciliation process, specifically for the `ta_cntrct_crs2` table. It handles environment setup, parameter parsing, job and error logging, and the invocation of a core processing script. This script does not contain direct business logic for data transformation but rather manages the execution flow and error handling around the core logic.

## 2. Source Inventory
The job is composed of a single KornShell script.
*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh`
    *   **Technology**: KornShell (ksh)
    *   **Complexity Tier**: medium
    *   **Automation Bucket**: semi_auto
    *   **Purpose**: Orchestration, parameter handling, logging, and invocation of the core script `k_ausd_v_ta_cntrct_crs2.ksh`.

The core business logic is delegated to `k_ausd_v_ta_cntrct_crs2.ksh`, which is invoked by this wrapper script.

## 3. Target Architecture
The target platform is BigQuery. This wrapper script will be migrated to a BigQuery Stored Procedure (BQSPL) that encapsulates its orchestration, parameter handling, and logging functions.

Key BigQuery components:
*   **BigQuery Stored Procedure**: `sp_vertragsdatenabgleich` (or similar) to replace the KornShell wrapper. This procedure will accept parameters, handle error conditions, manage logging, and call the migrated core logic.
*   **Audit/Log Tables**: BigQuery tables (e.g., `job_execution_log`, `job_error_log`) to replace file-based logging and `DWMSG` framework interactions.
*   **Configuration Tables**: BigQuery tables (e.g., `config_job_control`, `config_runtime`) to store job metadata, environment variables, and script paths previously managed by shell environment sourcing.
*   **Orchestration (Optional but recommended)**: Cloud Composer (Airflow), Workflows, or Cloud Run could manage the execution of the BigQuery Stored Procedure and potentially any non-SQL components of the core logic.

## 4. Data Flow & Lineage
The original script `r_ausd_v_ta_cntrct_crs2.ksh` functions as an orchestrator:
1.  **Environment Initialization**: Sources `.dw_init` and utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
2.  **Parameter Parsing**: Uses `getopts` to process command-line arguments, specifically `-h` for help and potentially `-s`, `-l` (though not fully handled in the provided script).
3.  **Error Handling Setup**: Initializes error variables and sets up `trap` commands for `INT` and `ERR` signals.
4.  **Logging Initialization**: Utilizes `DWMSG` framework functions to obtain a job entry number, construct a log file name, create a log entry, and set a "Stichtag" (reference date).
5.  **Core Script Invocation**: Executes the `Name_Kernskript` (`k_ausd_v_ta_cntrct_crs2.ksh`) passing `JobKennung` and `DW_EintragsNr` as arguments, redirecting output to the log file.
6.  **Status Update**: After the core script completes, it logs a success message and updates the job status via `DWMSG_SetzeStatusOK`.
7.  **Exit**: Exits with status `0` on success or a relevant error code on failure.

In BigQuery, this flow will be replicated within the `sp_vertragsdatenabgleich` stored procedure. Parameters will be passed as procedure arguments. Logging will write to BigQuery tables. The core script invocation will become a call to its corresponding BigQuery stored procedure (e.g., `CALL sp_k_ausd_v_ta_cntrct_crs2(JobKennung, DW_EintragsNr);`).

## 5. Transformation Logic
The `r_ausd_v_ta_cntrct_crs2.ksh` wrapper script contains no direct data transformation logic. Its logic is primarily control flow, parameter handling, and interaction with a custom logging/error framework.

**Mapping Shell Constructs to BigQuery SQL/Stored Procedures:**

*   **Environment Variables**: Shell variables like `$HOME`, `$BERT_DIR_ROOT`, `ProgName`, `JobKennung` will be mapped to BigQuery Stored Procedure parameters, `DECLARE` variables, or values retrieved from BigQuery configuration tables.
*   **Parameter Parsing (`getopts`)**: Will be replaced by input parameters to the BigQuery Stored Procedure (`p_h`, `p_s`, `p_l`). Validation logic will use `IF` statements.
*   **Conditionals (`if [ ! $ErrNr -eq 0 ]`)**: Directly translates to BigQuery `IF ErrNr != 0 THEN ... END IF;`.
*   **Date Formatting (`date +%d%m%Y`)**: Replaced by BigQuery's `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Output (`print`, `tee`)**: Console output will be simulated by `SELECT` statements within the stored procedure, and log entries will be `INSERT`ed into BigQuery audit/log tables.
*   **External Framework Functions (`DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, etc.)**: These custom functions will be replaced by dedicated BigQuery helper stored procedures or direct `INSERT` statements into audit tables.
*   **Core Script Invocation (`${Name_Kernskript}`)**: Will translate to a `CALL` statement to the migrated BigQuery stored procedure for `k_ausd_v_ta_cntrct_crs2.ksh`.

**Pseudocode (BigQuery SQL):**
A detailed pseudocode is provided in the CM MCP tool output and suggests a stored procedure named `sp_vertragsdatenabgleich` that:
1.  Declares variables for program name, version, job ID, and current date.
2.  Handles the `-h` parameter by returning usage information.
3.  Validates other parameters (`-s`, `-l`) and sets `ErrNr` if missing.
4.  If errors exist, inserts into `job_error_log`, prints usage, and signals an SQLSTATE error.
5.  Determines `DW_EintragsNr` (job entry number) from `job_execution_log`.
6.  Constructs `LogDatei` name.
7.  Inserts a 'STARTED' record into `job_execution_log`.
8.  Prints job metadata to the console.
9.  **Calls** `sp_k_ausd_v_ta_cntrct_crs2` (the migrated core procedure) with relevant parameters.
10. Inserts an 'OK' record into `job_execution_log` on successful completion.
11. Prints a success message.

## 6. External Dependencies
The wrapper script itself does not directly interact with external systems like Oracle, SFTP, or S3. Its primary external dependencies are:

*   **Shell Environment Initialization**: `$HOME/.dw_init` and `BERT_DIR_ROOT` environment variable.
    *   **Replacement**: Configured in Cloud Composer DAG parameters, environment variables for Cloud Run/Workflows, or values fetched from BigQuery configuration tables.
*   **Custom Shell Utility Scripts**:
    *   `f_alis_msgerr.ksh`: Error messaging framework.
    *   `h_alis_parameter.ksh`: Parameter handling helpers.
    *   `h_alis_date.ksh`: Date handling helpers.
    *   **Replacement**: These will be replaced by custom BigQuery helper stored procedures or direct SQL logic for logging and parameter handling within `sp_vertragsdatenabgleich` and its supporting procedures/tables.
*   **Core Processing Script**: `k_ausd_v_ta_cntrct_crs2.ksh`.
    *   **Replacement**: This script requires separate migration into a BigQuery stored procedure (e.g., `sp_k_ausd_v_ta_cntrct_crs2`), which will then be called by `sp_vertragsdatenabgleich`.

## 7. Unresolved / Risks
*   **Core Logic Migration**: The most significant unresolved item is the migration of the core business logic within `k_ausd_v_ta_cntrct_crs2.ksh`. This needs to be analyzed and migrated separately, likely to another BigQuery stored procedure.
*   **Shell-Specific Features**:
    *   `trap` semantics for OS signal handling are not native to BigQuery. This will be replaced by standard exception handling (`EXCEPTION` blocks) within BigQuery Stored Procedures and robust error logging.
    *   `tee` command for simultaneous console and file output. This can be replicated by inserting into a log table and selectively `SELECT`ing outputs in the orchestration layer.
    *   Sourcing shell environment files directly is not possible. Values will need to be provided via parameters, config tables, or environment variables in the orchestration layer.
*   **Dynamic Script Invocation**: The dynamic execution of `Name_Kernskript` needs to be handled by direct calls to the corresponding BigQuery stored procedure.
*   **Parameter Handling**: The script defines `-s` and `-l` parameters in `ParamList` but does not explicitly process them in the `case` statement. It's assumed these are either unused or implicitly handled by the core script. Clarification is needed if these are critical.
*   **Migration Bucket**: `semi_auto` indicates that some manual effort will be required, aligning with the need to manually design and implement the core script's migration and the replacement of shell-specific constructs.

## 8. Build Plan
1.  **Define BigQuery Audit/Log Tables**: Create `job_execution_log` and `job_error_log` tables in BigQuery to capture job metadata, status, and errors.
2.  **Define BigQuery Configuration Tables**: Create tables for storing environment-specific paths, job names, and other configuration parameters.
3.  **Migrate Wrapper Logic to BigQuery Stored Procedure**:
    *   Create `sp_vertragsdatenabgleich` BigQuery Stored Procedure.
    *   Implement parameter handling based on the original `getopts` logic.
    *   Replace `DWMSG` calls with `INSERT` statements into the new audit/log tables or calls to simple helper procedures.
    *   Implement `IF` conditions for error checking.
    *   Include `SELECT` statements for console output.
4.  **Migrate Core Script (`k_ausd_v_ta_cntrct_crs2.ksh`)**: (This is a separate, dependent task)
    *   Analyze `k_ausd_v_ta_cntrct_crs2.ksh` to identify its business logic, data sources, transformations, and targets.
    *   Design and implement `sp_k_ausd_v_ta_cntrct_crs2` (or similar) in BigQuery SQL, transforming its logic.
5.  **Integrate Core Procedure Call**: Update `sp_vertragsdatenabgleich` to `CALL sp_k_ausd_v_ta_cntrct_crs2` passing the necessary arguments.
6.  **Implement Orchestration**: If complex scheduling or external dependencies are involved, consider using Cloud Composer (Airflow) to schedule and execute `sp_vertragsdatenabgleich`.
7.  **Testing**: Thoroughly test the migrated BigQuery stored procedure, including parameter handling, logging, error conditions, and the invocation of the core logic.