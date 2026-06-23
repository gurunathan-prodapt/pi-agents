# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_cntrct_crs.ksh` to Google BigQuery. The script serves as an orchestration framework for the contract data reconciliation process for the `ta_cntrct_crs` table. Its primary function is to handle parameter parsing, environment setup, robust logging, error handling, and to orchestrate the execution of a core data processing script (`k_ausd_v_ta_cntrct_crs.ksh`). The migration aims to translate this orchestration logic into a BigQuery-native format, ensuring equivalent functionality and integrating with BigQuery's data processing capabilities.

## 2. Source Inventory
- **File Name**: `r_ausd_v_ta_cntrct_crs.ksh`
- **Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh`
- **Technology**: KornShell Script (shell)
- **Complexity Tier**: medium
- **Automation Bucket**: semi_auto
- **Summary**: This KornShell script is an orchestration layer for a contract data reconciliation process related to the `ta_cntrct_crs` table. It manages runtime environment, parameters, logging, and error handling for a subordinate core processing script.

## 3. Target Architecture
The shell script's orchestration logic will be migrated to a BigQuery Stored Procedure. This procedure will encapsulate the parameter handling, environment variable equivalents, logging, and error management.

-   **BigQuery Stored Procedure (`sp_vertragsdatenabgleich`)**: This will be the primary BigQuery component. It will:
    -   Accept input parameters (analogous to command-line arguments).
    -   Manage internal variables for program metadata.
    -   Implement parameter validation.
    -   Handle error conditions and raise appropriate signals.
    -   Interact with dedicated BigQuery tables for logging and auditing.
    -   Invoke a separate BigQuery Stored Procedure (e.g., `sp_ausd_v_ta_cntrct_crs`) that will contain the core data reconciliation logic, currently residing in `k_ausd_v_ta_cntrct_crs.ksh`.

-   **BigQuery Audit/Log Tables**:
    -   `project.dataset.job_error_log`: To record job-related errors.
    -   `project.dataset.job_control`: To manage job entry numbers and metadata.
    -   `project.dataset.job_audit_log`: For detailed logging of job start, success, and error messages.
    -   `project.dataset.job_status`: To track the overall status of a job.

-   **Orchestration (Implied)**: While this script itself is an orchestrator, its invocation in the legacy system (e.g., by a scheduler like UC4) would be replaced by an Airflow DAG on Cloud Composer or a Cloud Workflow. This external orchestration would be responsible for calling the migrated `sp_vertragsdatenabgleich` procedure in BigQuery.

## 4. Data Flow & Lineage
The original KornShell script `r_ausd_v_ta_cntrct_crs.ksh` acts as a control flow orchestrator. It does not directly read from or write to business data tables. Its primary "data flow" involves:

1.  **Input**: Receives command-line parameters.
2.  **Internal Processing**: Sets environment variables, includes utility functions, generates log file names and job entry numbers.
3.  **Invocation**: Calls the core script `k_ausd_v_ta_cntrct_crs.ksh` passing parameters and redirecting its output to a log file.
4.  **Output**: Writes status messages to standard output and a dynamically generated log file. Updates job status via utility functions.

In the target BigQuery architecture, this flow will be:

1.  **External Orchestrator (e.g., Airflow DAG)**: Invokes `project.dataset.sp_vertragsdatenabgleich` with required parameters.
2.  **`sp_vertragsdatenabgleich`**:
    -   Validates parameters.
    -   Records job metadata and status in `job_control`, `job_audit_log`.
    -   **Invokes Core Logic**: Calls `project.dataset.sp_ausd_v_ta_cntrct_crs` (the migrated `k_ausd_v_ta_cntrct_crs.ksh`). This is where the actual data reads, transformations, and writes to tables (e.g., `ta_cntrct_crs`) would occur, but the details are dependent on the core script's content.
    -   Captures exceptions from the core logic.
    -   Updates `job_audit_log`, `job_error_log`, and `job_status` based on the outcome of the core logic execution.
3.  **Output**: Status and error messages are written to BigQuery audit tables instead of filesystem log files.

## 5. Transformation Logic
The transformation logic focuses on converting the KornShell orchestration patterns to BigQuery SQL.

| Legacy KornShell Construct | Target BigQuery SQL Equivalent | Notes |
| :------------------------- | :----------------------------- | :---- |
| Program metadata (`ProgName`, `ProgVersion`) | `DECLARE` statements for variables. | |
| `usage()` function | `SELECT` statement returning usage text; `LEAVE` for early exit. | |
| Sourcing shell scripts (`. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/...`) | Equivalent functionality translated into BigQuery Stored Procedures or user-defined functions (UDFs) as needed. Shared helper procedures. | No direct equivalent for sourcing. |
| Parameter parsing (`getopts`) | Stored Procedure input parameters (`IN p_param_name STRING`). | Parameter validation logic translated using `IF` statements. |
| Error handling (`if [ ! $ErrNr -eq 0 ]`, `DWMSG_MeldeFehler`, `exit`) | `IF` statements, `INSERT` into `job_error_log`, `SIGNAL SQLSTATE '45000'`. | Specific error codes (192, 193) to be mapped to BigQuery error handling. |
| Job identification/logging (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`) | `INSERT` statements into BigQuery audit/log tables (`job_control`, `job_audit_log`, `job_status`). | Logic for generating unique entry numbers will use `MAX(entry_nr)` or sequences if available. |
| Shell traps (`trap "DWMSG_Fehlerbehandlung..." INT ERR`) | `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks for error handling. | BigQuery's transaction and exception handling replace shell traps. |
| Executing core script (`${Name_Kernskript} -j ...`) | `CALL project.dataset.sp_ausd_v_ta_cntrct_crs(v_job_kennung, v_eintragsnr);` | The core script is assumed to be migrated to its own BigQuery Stored Procedure. |
| Standard output (`print "..."`, `tee -a $LogDatei`) | `SELECT` statements for immediate output (for debugging/monitoring) and `INSERT` into `job_audit_log` for persistent logging. | `tee` functionality is replaced by direct inserts into audit tables. |

The provided pseudocode for `sp_vertragsdatenabgleich` in the tool output serves as the blueprint for this transformation.

## 6. External Dependencies
Based on the `lineage_assembled_jobs` for this specific job, no external systems (like Oracle, SFTP, S3, etc.) are directly referenced or used by `r_ausd_v_ta_cntrct_crs.ksh`. All its dependencies are other shell scripts within the legacy file system.

The "external commands" and "shell features" listed in the `shellscript_to_bqsql_design` output (`getopts`, `date`, `tee`, `trap`, `print`, `exit`, `set -eu`) are native shell functionalities and will be replaced by BigQuery SQL constructs or equivalent patterns as described in Section 5.

## 7. Unresolved / Risks
-   **Content of Core Script (`k_ausd_v_ta_cntrct_crs.ksh`)**: The actual data processing and reconciliation logic resides in `k_ausd_v_ta_cntrct_crs.ksh`, which was not part of this assembled job. The migration of this core script is critical and will dictate the specific BigQuery SQL transformations for the data-related tasks. Its content needs separate analysis.
-   **Sourced Utility Scripts**: The scripts sourced by `r_ausd_v_ta_cntrct_crs.ksh` (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) contain utility functions crucial for error handling, parameter management, and date operations. These must be analyzed and migrated to BigQuery Stored Procedures or UDFs, or their functionalities absorbed directly into the main orchestration procedure.
-   **Environment Variables**: The script relies on environment variables like `$HOME` and `${BERT_DIR_ROOT}`. These will need to be mapped to BigQuery project, dataset, or configuration parameters in the target environment.
-   **Shell-specific Behavior**: Some shell-specific behaviors like `set -eu` for strict error handling and `trap` for signal management have conceptual BigQuery equivalents (`BEGIN...EXCEPTION`), but direct, exact behavioral parity might require careful implementation and testing.
-   **Log File Management**: The dynamic generation and writing to log files in the filesystem will be replaced by structured logging into BigQuery audit tables. This is a conceptual shift from file-based logs to database-based logs.

## 8. Build Plan
1.  **Analyze Core Script (`k_ausd_v_ta_cntrct_crs.ksh`)**: Perform a full analysis of `k_ausd_v_ta_cntrct_crs.ksh` to determine its data sources, transformations, and targets. This will inform the creation of `project.dataset.sp_ausd_v_ta_cntrct_crs`.
2.  **Design and Implement Utility Procedures**:
    -   Translate the functionality of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` into BigQuery Stored Procedures or UDFs as needed.
3.  **Create BigQuery Audit Tables**:
    -   Define schemas and create `project.dataset.job_error_log`, `project.dataset.job_control`, `project.dataset.job_audit_log`, and `project.dataset.job_status` tables in BigQuery.
4.  **Develop `sp_vertragsdatenabgleich`**:
    -   Write the BigQuery Stored Procedure `project.dataset.sp_vertragsdatenabgleich` based on the pseudocode generated by the MCP tool, implementing parameter handling, logging, and error management.
    -   Integrate calls to the utility procedures and audit tables.
    -   Include the `CALL` to the future `sp_ausd_v_ta_cntrct_crs`.
5.  **Develop `sp_ausd_v_ta_cntrct_crs`**:
    -   Implement the core data reconciliation logic in `project.dataset.sp_ausd_v_ta_cntrct_crs` based on the analysis of `k_ausd_v_ta_cntrct_crs.ksh`.
6.  **Develop External Orchestration**:
    -   Create an Airflow DAG (using Python) on Cloud Composer or a Cloud Workflow to orchestrate the execution of `sp_vertragsdatenabgleich`. This DAG will handle scheduling, parameter passing, and monitoring.
7.  **Testing**:
    -   Unit test `sp_vertragsdatenabgleich` and `sp_ausd_v_ta_cntrct_crs` independently.
    -   Integration test the full flow via the external orchestrator.
8.  **Deployment**: Deploy the BigQuery Stored Procedures and the external orchestrator to the target environment.

**Build Language**:
-   BigQuery SQL for Stored Procedures and UDFs.
-   Python for Cloud Composer Airflow DAGs or Cloud Workflows.