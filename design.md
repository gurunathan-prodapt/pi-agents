# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_cntrct_crs2.ksh` to Google BigQuery. The primary purpose of the original script is to act as a wrapper or orchestration layer for the contract data reconciliation process related to the `ta_cntrct_crs2` table. It handles environment setup, parameter parsing, logging, error trapping, and then invokes a core processing script, `k_ausd_v_ta_cntrct_crs2.ksh`, which is expected to contain the actual data processing logic. The scope of this migration is to translate this orchestration logic into a BigQuery-native solution, likely a stored procedure, while accounting for the invocation of its core script and integrating BigQuery's logging and error handling mechanisms.

## 2. Source Inventory
The job consists of a single KornShell script.

*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh`
*   **Technology**: KornShell
*   **Summary**: KornShell script serving as a wrapper for data reconciliation of the `ta_cntrct_crs2` table. It handles parameter parsing, environment setup, logging, error trapping, and then calls a core processing script.
*   **File Purpose**: Orchestration/Wrapper
*   **Complexity Tier**: (Information not available from `file_complexity` table.)
*   **Migration Flags**: (Information not available from `file_complexity` table.)
*   **Automation Bucket**: (Information not available from `automation_rate` table.)

## 3. Target Architecture
The target architecture will leverage BigQuery's capabilities for orchestration, data processing, logging, and error handling.

*   **Main Component**: A BigQuery Stored Procedure, named similarly to the original job (e.g., `project.dataset.BERT_V_TA_CNTRCT_CRS2`), will serve as the primary migration target for the wrapper script.
*   **Core Logic**: The core processing logic currently within `k_ausd_v_ta_cntrct_crs2.ksh` will likely be migrated to a separate BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_v_ta_cntrct_crs2`) or a series of SQL statements/views, invoked by the wrapper stored procedure.
*   **Logging**: A dedicated BigQuery logging table (e.g., `project.dataset.job_log`) will capture execution details, messages, and status updates, replacing file-based logging.
*   **Error Handling**: A BigQuery error logging table (e.g., `project.dataset.job_error_log`) will store detailed error information. BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks will replace shell `trap` mechanisms.
*   **Parameter Management**: Stored procedure parameters will replace shell `getopts` for input handling.
*   **Metadata/Control**: A job control table (e.g., `project.dataset.job_control`) will manage job numbers, status, and stichtag (reference date) information, replacing utility functions like `DWMSG_ErmittleNr` and `DWMSG_SetzeStichtagInfo`.

## 4. Data Flow & Lineage
The original script `r_ausd_v_ta_cntrct_crs2.ksh` acts as an orchestrator.

**Original Flow:**
1.  `r_ausd_v_ta_cntrct_crs2.ksh` starts execution.
2.  Environment variables and utility functions (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are sourced.
3.  Command-line parameters are parsed.
4.  Logging and error handling mechanisms are initialized (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `trap`).
5.  The core script `k_ausd_v_ta_cntrct_crs2.ksh` is invoked with relevant parameters, with its output redirected to a log file.
6.  Upon successful completion of the core script, a success message is logged, and the job status is updated (`DWMSG_SetzeStatusOK`).
7.  If any error or interrupt occurs, the `trap` handlers execute error logging (`DWMSG_Fehlerbehandlung`).

**Target BigQuery Flow:**
1.  A BigQuery Stored Procedure, `project.dataset.BERT_V_TA_CNTRCT_CRS2`, is called, potentially with input parameters for `-h`, `-s`, `-l`.
2.  Initial variable declarations for `ProgName`, `ProgVersion`, `JobKennung`, `v_sysdate`, `DW_EintragsNr`, `ErrNr`, `ErrArg`, `LogDatei`, `Name_Kernskript`, `v_status`.
3.  Conditional logic handles the `-h` (help) parameter, exiting the procedure if `p_h` is set.
4.  Parameter validation is performed, equivalent to `getopts` error checking. If validation fails, errors are inserted into `project.dataset.job_error_log`, and the procedure signals an error.
5.  `DW_EintragsNr` is determined by querying `project.dataset.job_control` to get the next job number.
6.  `LogDatei` is derived (though not used for file redirection in BQ).
7.  A new entry for the job is inserted into `project.dataset.job_control` with initial status 'RUNNING'.
8.  `stichtag_info` is updated in `project.dataset.job_control`.
9.  A `BEGIN...EXCEPTION WHEN ERROR THEN...END` block encapsulates the core execution:
    *   Inside the block, a BigQuery Stored Procedure representing the core script, `CALL project.dataset.k_ausd_v_ta_cntrct_crs2(JobKennung, DW_EintragsNr);`, is invoked.
    *   If `k_ausd_v_ta_cntrct_crs2` completes successfully, an 'INFO' message is inserted into `project.dataset.job_log`, and the status in `project.dataset.job_control` is updated to 'OK'.
    *   If an error occurs during the `CALL`, the `EXCEPTION` handler catches it, inserts error details into `project.dataset.job_error_log`, updates job status to 'ERROR' in `project.dataset.job_control`, and signals an error.
10. The procedure concludes, returning success or error status.

## 5. Transformation Logic
The original script `r_ausd_v_ta_cntrct_crs2.ksh` contains no direct data transformation logic; it is purely an orchestration layer. All transformation logic is delegated to the core script `k_ausd_v_ta_cntrct_crs2.ksh` and potentially the utility scripts it sources.

**Wrapper Script Transformation (KornShell to BigQuery SQL Stored Procedure):**

*   **Environment Sourcing**: The `. $HOME/.dw_init` and utility script sourcing (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) will be replaced by direct BigQuery Stored Procedure calls for equivalent functionality, or by ensuring required environment settings are configured at the BigQuery session level or within the calling orchestration tool (e.g., Cloud Composer).
*   **Parameter Parsing (`getopts`)**: Translated into input parameters for the BigQuery Stored Procedure (`p_h`, `p_s`, `p_l`). Validation logic will be implemented using `IF` statements.
*   **Error Handling (`trap`)**: Replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks to catch and handle runtime errors, updating a BigQuery error log table.
*   **Logging (`DWMSG_...`, `tee`, redirection)**: Replaced by `INSERT` statements into BigQuery logging and job control tables (`project.dataset.job_log`, `project.dataset.job_error_log`, `project.dataset.job_control`).
*   **Date Generation (`date +%d%m%Y`)**: Replaced by BigQuery's `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Core Script Invocation (`${Name_Kernskript}`)**: Translated into a `CALL` statement to the migrated BigQuery Stored Procedure for the core script (e.g., `CALL project.dataset.k_ausd_v_ta_cntrct_crs2(JobKennung, DW_EintragsNr);`).

The actual data transformation logic for `ta_cntrct_crs2` will be contained within the migrated `k_ausd_v_ta_cntrct_crs2` BigQuery Stored Procedure.

## 6. External Dependencies
*   **Sourced Utility Scripts**:
    *   `. $HOME/.dw_init`: Likely contains environment variables or functions. These need to be identified and either set as BigQuery session variables, hardcoded if static, or managed by an external orchestrator (e.g., Cloud Composer).
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging functions. These will be replaced by logic writing to `project.dataset.job_error_log` and potentially BigQuery's `SIGNAL SQLSTATE`.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling. This functionality will be absorbed into the stored procedure's input parameter parsing and validation.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utilities. These will be replaced by BigQuery's native date functions.
*   **Core Processing Script**:
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh`: This is the most critical dependency. It must be migrated separately. Its shell script functionality will be replaced by a BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_cntrct_crs2`) that contains the actual SQL or data processing logic for `ta_cntrct_crs2`.
*   **`ta_cntrct_crs2` table**: The script reconciles data for `ta_cntrct_crs2`. This table must be migrated to BigQuery. Its schema and data will become `project.dataset.ta_cntrct_crs2`.

## 7. Unresolved / Risks
*   **Missing Complexity and Automation Data**: The complexity tier, migration flags, and automation bucket for this file were not found in the database. This limits the ability to assess the estimated effort and specific migration challenges, necessitating manual assessment if detailed project planning requires it.
*   **`k_ausd_v_ta_cntrct_crs2.ksh` Migration**: The core processing script `k_ausd_v_ta_cntrct_crs2.ksh` is critical. Its internal logic and dependencies are currently unknown but will dictate the complexity of the overall migration. This script must be analyzed and migrated before `r_ausd_v_ta_cntrct_crs2.ksh` can be fully operational in BigQuery.
*   **Detailed Functionality of Sourced Scripts**: The exact implementation details of utility scripts like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` were not fully analyzed. Any complex logic within these may require specific BigQuery UDFs or further investigation.
*   **Environment Initialization (`. $HOME/.dw_init`)**: The contents and purpose of `.dw_init` are unknown. This could contain crucial environment variables or path settings that need to be replicated or replaced in the BigQuery environment or a GCP orchestration layer.
*   **Shell-specific Constructs**: While the pseudocode addresses many shell constructs, any deeply embedded shell-specific file operations, system calls, or complex command-line piping not identified by the tool would need careful manual conversion.

## 8. Build Plan
1.  **Define BigQuery Schema for Logging and Control Tables**:
    *   `project.dataset.job_log` (job_kennung, eintrags_nr, log_level, message, created_ts)
    *   `project.dataset.job_error_log` (job_kennung, eintrags_nr, error_nr, error_arg, error_message, created_ts)
    *   `project.dataset.job_control` (eintrags_nr, job_kennung, script_name, log_name, stichtag_info, status, created_ts, finished_ts)
    *   (Language: BigQuery DDL SQL)
2.  **Migrate Core Processing Script (`k_ausd_v_ta_cntrct_crs2.ksh`)**:
    *   Analyze the content of `k_ausd_v_ta_cntrct_crs2.ksh` to identify its data processing logic, source/target tables, and any embedded SQL.
    *   Develop a BigQuery Stored Procedure, `project.dataset.k_ausd_v_ta_cntrct_crs2`, that replicates this logic using BigQuery SQL.
    *   (Language: BigQuery SQL)
3.  **Create BigQuery Stored Procedure for `r_ausd_v_ta_cntrct_crs2.ksh`**:
    *   Implement the BigQuery SQL pseudocode provided by the CM MCP tool as `project.dataset.BERT_V_TA_CNTRCT_CRS2`.
    *   Ensure proper parameter handling, error trapping using `BEGIN...EXCEPTION`, and logging to the defined BigQuery tables.
    *   Include the `CALL` statement to the migrated core script `project.dataset.k_ausd_v_ta_cntrct_crs2`.
    *   (Language: BigQuery SQL)
4.  **Integration Testing**:
    *   Test the `project.dataset.BERT_V_TA_CNTRCT_CRS2` stored procedure with various parameters (including `-h` and invalid parameters) to ensure correct error handling and logging.
    *   Test the end-to-end flow, ensuring the invocation of `project.dataset.k_ausd_v_ta_cntrct_crs2` works as expected and data reconciliation completes successfully.
    *   (Language: BigQuery SQL, Python for test orchestration if needed)
5.  **Orchestration (Optional but Recommended)**:
    *   If the original script was part of a larger workflow, consider integrating the BigQuery Stored Procedure into a GCP orchestration tool like Cloud Composer (Apache Airflow) or Cloud Workflows.
    *   (Language: Python for Airflow DAGs, YAML/JSON for Cloud Workflows)