# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh

## 1. Purpose & Scope
This job, `r_ausd_v_ta_discount_rr.ksh`, is an orchestration KornShell script designed to reconcile contract data for the `ta_discount_rr` table. Its primary purpose is to act as a wrapper, handling environment setup, parsing command-line parameters, managing error logging, and reporting job status. It invokes a core script, `k_ausd_v_ta_discount_rr.ksh`, where the actual data reconciliation logic is presumed to reside. The scope of this migration design focuses on transforming this wrapper script's orchestration and error handling into a BigQuery-native solution.

## 2. Source Inventory
The job consists of a single source file:
- **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh`
- **Technology**: KornShell
- **Complexity Tier**: medium
- **Automation Bucket**: semi_auto
- **Summary**: This KornShell script acts as a wrapper to orchestrate the execution of a core data reconciliation script for the 'ta_discount_rr' table, handling environment setup, parameter parsing, error logging, and status reporting.

## 3. Target Architecture
The target architecture in BigQuery will primarily consist of:
- **BigQuery Stored Procedures**: A main stored procedure will replace the KornShell wrapper, handling parameter parsing, job initialization, logging, and invoking the migrated core logic. Helper stored procedures will encapsulate the logic of the `DWMSG_*` functions.
- **BigQuery Tables**: Dedicated tables will be used for:
    - **Job Logging**: To replace file-based logs, storing job execution details, messages, and statuses.
    - **Configuration**: To manage environment variables, paths, and error codes that were previously sourced from files or hardcoded.
- **External Orchestration (Optional but Recommended)**: For robust interrupt handling and broader workflow management, Cloud Composer (Airflow) or Cloud Workflows could orchestrate the BigQuery stored procedures, especially if the core script's migrated logic (`k_ausd_v_ta_discount_rr.ksh`) remains non-SQL or requires complex dependencies.

## 4. Data Flow & Lineage
The original lineage analysis (`lineage_edges`) showed no direct data flow or execution dependencies for this wrapper script. However, upon manual inspection of the source code, the following implicit invocations and dependencies are identified:

- **Orchestration**: `r_ausd_v_ta_discount_rr.ksh` (this script) INVOKES `k_ausd_v_ta_discount_rr.ksh`.
- **Utility Sourcing**: This script sources several utility KornShell scripts:
    - `. $HOME/.dw_init`: For environment initialization.
    - `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: For error messaging framework.
    - `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: For parameter handling utilities.
    - `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: For date handling utilities.

In the BigQuery target, this orchestration will be handled by the main BigQuery stored procedure. The `k_ausd_v_ta_discount_rr.ksh` will be migrated into its own BigQuery stored procedure, which will then be called from the wrapper procedure. The sourced utility scripts' functionalities will be absorbed into the main procedure or separate helper stored procedures/UDFs.

## 5. Transformation Logic
The `r_ausd_v_ta_discount_rr.ksh` script primarily contains orchestration and error-handling logic, not direct business transformation logic.

**Key components and their BigQuery mapping:**

-   **Parameter Handling (`getopts`)**:
    -   Original: Uses `getopts` to parse `-h` (help), and declares `-s`, `-l` parameters.
    -   Target: Replaced by BigQuery stored procedure input parameters (e.g., `p_param_h STRING`, `p_param_s STRING`, `p_param_l STRING`).
-   **Environment Variables (`$HOME/.dw_init`, `${BERT_DIR_ROOT}`)**:
    -   Original: Sources environment files and uses `BERT_DIR_ROOT` for pathing.
    -   Target: Will be replaced by BigQuery `DECLARE` variables within the stored procedure, potentially initialized from a configuration table or passed as parameters.
-   **Error Handling (`f_alis_msgerr.ksh`, `trap`, `DWMSG_*` functions)**:
    -   Original: Employs a custom error framework (`f_alis_msgerr.ksh`), sets `trap` for `INT` and `ERR` signals, and uses functions like `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`.
    -   Target: `trap` mechanisms will be replaced by `BEGIN...EXCEPTION...END` blocks within BigQuery SQL stored procedures for error catching. The `DWMSG_*` functions will be migrated into dedicated helper BigQuery stored procedures that log to a centralized job logging table.
-   **Logging (`>> $LogDatei 2>&1`, `tee -a`)**:
    -   Original: Appends output and error streams to a dynamically named log file.
    -   Target: All logging will be directed to a BigQuery job logging table via `INSERT` statements, managed by the helper logging procedures.
-   **Script Invocation (`${Name_Kernskript}`)**:
    -   Original: Executes `k_ausd_v_ta_discount_rr.ksh` as a child process.
    -   Target: The main BigQuery stored procedure will directly `CALL` the migrated BigQuery stored procedure corresponding to `k_ausd_v_ta_discount_rr.ksh`.

## 6. External Dependencies
The `lineage_assembled_jobs` record indicated no external systems for this job. However, based on the script's content, the following dependencies are present:

-   **Filesystem Dependencies**:
    -   `$HOME/.dw_init`: An environment initialization script.
    -   Utility scripts under `${BERT_DIR_ROOT}/allgemein/is/util/bin/`:
        - `f_alis_msgerr.ksh`
        - `h_alis_parameter.ksh`
        - `h_alis_date.ksh`
    -   `k_ausd_v_ta_discount_rr.ksh`: The core script containing business logic.

**Replacement Strategy:**
-   `$HOME/.dw_init` and `${BERT_DIR_ROOT}` based paths: These environment configurations will be replaced by BigQuery `DECLARE` variables or by values retrieved from a BigQuery configuration table.
-   Utility scripts: The functionalities within `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` will be reimplemented as BigQuery helper stored procedures or UDFs, or integrated directly into the main orchestration procedure.
-   `k_ausd_v_ta_discount_rr.ksh`: This core script is the primary dependency for actual data processing and needs to be migrated separately, likely into its own BigQuery stored procedure or a PySpark job if its logic is complex and non-SQL-native.

## 7. Unresolved / Risks
-   **Core Script (`k_ausd_v_ta_discount_rr.ksh`)**: The content and complexity of the core script are unknown. Its migration strategy (BQ SQL, PySpark, etc.) will significantly impact the overall project. This is the main unresolved component and carries the highest risk.
-   **Shell Traps (`INT`, `ERR`)**: The direct equivalent of shell `trap` commands is not available in BigQuery SQL. While `BEGIN...EXCEPTION...END` blocks provide similar error handling, the exact behavior of signal trapping for system-level interrupts might require external orchestration (e.g., Cloud Composer) to fully replicate.
-   **File-based Logging**: The original uses file-based logging. This is replaced by BigQuery tables, which is a conceptual shift but fully implementable.
-   **Dynamic Sourcing**: The `. dw_init` and other sourced scripts represent dynamic runtime includes. While BQ stored procedures can call other procedures, the dynamic nature of sourcing environment variables needs careful mapping to BigQuery's static variable declarations or configuration tables.
-   **Parameter `-s` and `-l`**: These parameters are declared in `getopts` but not explicitly handled in the provided script. Their intended use in the original system is unresolved and needs clarification if they are relevant to the core logic.

## 8. Build Plan
The migration will involve creating the following BigQuery components:

1.  **Job Logging Table (BQ Table)**:
    -   **Language**: BigQuery DDL
    -   **Purpose**: To store job execution logs, including job ID, messages, timestamps, and status.
    -   **Schema**: `job_entry_nr INT64, job_key STRING, log_level STRING, message STRING, created_at TIMESTAMP, status STRING`.

2.  **Configuration Table (BQ Table)**:
    -   **Language**: BigQuery DDL
    -   **Purpose**: To store environment variables and paths like `BERT_DIR_ROOT` that were previously sourced from files.
    -   **Schema**: `config_key STRING, config_value STRING`.

3.  **DWMSG_ErmittleNr Stored Procedure (BQ Stored Procedure)**:
    -   **Language**: BigQuery SQL
    -   **Purpose**: To generate a unique job entry number for logging.

4.  **DWMSG_Logdateiname Stored Procedure (BQ Stored Procedure)**:
    -   **Language**: BigQuery SQL
    -   **Purpose**: To determine the log file name (conceptually, in BQ this might just return a string for record-keeping, as actual logging is to a table).

5.  **DWMSG_ErzeugeEintrag Stored Procedure (BQ Stored Procedure)**:
    -   **Language**: BigQuery SQL
    -   **Purpose**: To create an initial log entry for the job.

6.  **DWMSG_SetzeStichtagInfo Stored Procedure (BQ Stored Procedure)**:
    -   **Language**: BigQuery SQL
    -   **Purpose**: To set a reference date for the job.

7.  **DWMSG_MeldeFehler Stored Procedure (BQ Stored Procedure)**:
    -   **Language**: BigQuery SQL
    -   **Purpose**: To log error messages.

8.  **DWMSG_Fehlerbehandlung Stored Procedure (BQ Stored Procedure)**:
    -   **Language**: BigQuery SQL
    -   **Purpose**: To handle error conditions and log them.

9.  **DWMSG_SetzeStatusOK Stored Procedure (BQ Stored Procedure)**:
    -   **Language**: BigQuery SQL
    -   **Purpose**: To update the job status to OK.

10. **Main Orchestration Stored Procedure (`project.dataset.Vertragsdatenabgleich`)**:
    -   **Language**: BigQuery SQL
    -   **Purpose**: To replace `r_ausd_v_ta_discount_rr.ksh`, handling parameter parsing, calling helper procedures for logging and error handling, and invoking the migrated core script.

11. **Migrated Core Script (`project.dataset.k_ausd_v_ta_discount_rr`)**:
    -   **Language**: BigQuery SQL (or PySpark/Dataflow if non-SQL logic is involved).
    -   **Purpose**: The actual data reconciliation logic from the original `k_ausd_v_ta_discount_rr.ksh`. (This is a placeholder and requires separate design and build).