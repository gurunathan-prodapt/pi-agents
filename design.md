# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_bp_ta_bpr_beschr.ksh` to Google BigQuery.

The original script acts as an orchestration wrapper for the initial provisioning of selected base products for the BERT system. Its primary functions include:
*   Setting up the execution environment by sourcing common utility scripts.
*   Parsing and validating command-line parameters, specifically a `Stichtag` (cutoff date) and an optional `Wiederanlaufwert` (restart value).
*   Determining an effective processing date, defaulting to the system date if the `Stichtag` is not explicitly provided.
*   Initializing a comprehensive logging and error handling framework for the job.
*   Invoking a downstream "kernel" script, `k_ausd_bp_ta_bpr_beschr.ksh`, with the prepared parameters to execute the core business logic.
*   Logging the overall job status (start, success, or failure) to a central log.

The scope of this migration design specifically covers the translation of this wrapper script's orchestration, parameter handling, and logging mechanisms into a BigQuery-native solution. The core business logic residing in `k_ausd_bp_ta_bpr_beschr.ksh` is identified as a separate migration effort, which will integrate with this wrapper.

## 2. Source Inventory
The job is composed of a single KornShell script.

| File Path                                                                   | Technology | Complexity Tier | Automation Bucket | Purpose Note                                                                                                                                                                                                                                                                                                                                                            |
| :-------------------------------------------------------------------------- | :--------- | :-------------- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh` | KornShell  | medium          | semi_auto         | Job assembled from 1 component(s); stage dist: medium=1. This script orchestrates the provisioning of selected base products for BERT, handling parameter parsing, date determination, logging, and invoking a core kernel script. Its primary role is control flow rather than direct data processing. |

## 3. Target Architecture
The shell script will be refactored into a BigQuery-native solution, primarily using BigQuery Stored Procedures for modularity and execution.

*   **BigQuery Stored Procedure (Orchestrator)**: The `r_ausd_bp_ta_bpr_beschr.ksh` wrapper script will be translated into a BigQuery Stored Procedure, named `project.dataset.ausd_bp_ta_bpr_beschr_wrapper`. This procedure will manage input parameters, environment initialization (translated to BigQuery variables/configuration), date logic, validation, and log management.
*   **BigQuery Stored Procedure (Core Logic)**: The kernel script, `k_ausd_bp_ta_bpr_beschr.ksh`, which contains the actual data processing, will be migrated into a separate BigQuery Stored Procedure, named `project.dataset.ausd_bp_ta_bpr_beschr_core`. The wrapper procedure will invoke this core procedure.
*   **Audit/Log Table**: A dedicated BigQuery table, `project.dataset.job_log`, will be created to store job execution metadata, status updates, and error messages, replacing the file-based logging of the original script.
*   **Orchestration Layer**: For scheduling and dependency management, particularly if the job becomes part of a larger workflow or requires external interaction (e.g., triggering other Cloud services), Google Cloud Composer (Apache Airflow) or Cloud Workflows could be utilized. For simple scheduling, BigQuery Scheduled Queries may suffice.

## 4. Data Flow & Lineage
The original shell script acts as an orchestrator. The data flow originates from the invocation of `r_ausd_bp_ta_bpr_beschr.ksh` with specific parameters, which then manages the execution of the core business logic.

**Original System Data Flow:**
1.  **Invocation**: `r_ausd_bp_ta_bpr_beschr.ksh` is executed, potentially with `-s <DDMMYYYY>` (Stichtag) and `-l <Wiederanlaufwert>` parameters.
2.  **Environment Setup**: Sources `.dw_init` and various helper scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
3.  **Parameter Processing**: Parses input parameters, initializes `p_wiederanlaufWert` to `0` if not provided, and determines `p_stichtag` (defaulting to `v_sysdate` if not supplied).
4.  **Logging & Error Handling**: Sets up `DWMSG_*` functions and `trap` commands for robust logging and error management throughout the script's execution.
5.  **Core Logic Delegation**: Calls `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh` (via the `Name_Kernskript` variable), passing `JobKennung`, `p_stichtag`, `DW_EintragsNr`, and `p_wiederanlaufWert`.
6.  **Status Logging**: Logs the final execution status (success or failure) to a log file.

**Migrated BigQuery Data Flow:**
1.  **Wrapper Procedure Call**: The `project.dataset.ausd_bp_ta_bpr_beschr_wrapper` stored procedure is invoked, receiving `p_stichtag` (STRING) and `p_wiederanlaufWert` (INT64) as input.
2.  **Internal Processing**: The wrapper procedure performs:
    *   Parameter initialization and validation using BigQuery SQL constructs.
    *   Date determination (`v_sysdate`, `v_effective_stichtag`) using BigQuery functions.
    *   Logging of job events (start, parameter details, progress) by inserting records into the `project.dataset.job_log` table.
3.  **Core Logic Procedure Call**: The wrapper procedure calls `project.dataset.ausd_bp_ta_bpr_beschr_core`, passing the necessary parameters (`p_jobkennung`, `p_stichtag`, `p_dweintragsnr`, `p_wiederanlaufWert`).
4.  **Core Logic Execution**: The `project.dataset.ausd_bp_ta_bpr_beschr_core` procedure executes the main business logic, performing data operations on BigQuery tables (e.g., selecting contracts, filtering by `DWH_VERTRAG_ID`, deleting/inserting into FOS tables).
5.  **Status Logging**: The wrapper procedure logs the completion status (success or error) of the entire job into `project.dataset.job_log` using BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks for robust error handling.

## 5. Transformation Logic
The transformation logic for the `r_ausd_bp_ta_bpr_beschr.ksh` wrapper script focuses on converting shell constructs and utility function calls into BigQuery SQL scripting capabilities.

*   **Parameter Handling**:
    *   Shell's `getopts` for `-s` and `-l` will be replaced by `IN` parameters (`p_stichtag STRING`, `p_wiederanlaufWert INT64`) in the `ausd_bp_ta_bpr_beschr_wrapper` stored procedure.
    *   The defaulting of `p_wiederanlaufWert` to `0` will be achieved with `IFNULL(p_wiederanlaufWert, 0)`.
    *   The defaulting of `p_stichtag` to `v_sysdate` if not provided will use `IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate)`.
*   **Date Determination**:
    *   The `DWDate_Gib_Zeitraum` function call for `v_sysdate` will be replaced by BigQuery functions like `CURRENT_DATE()` and `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Parameter Validation**:
    *   The `pruefeParameterGesetzt` call and subsequent `if [ ! $ErrNr -eq 0 ]` check will be translated into explicit `IF ... THEN SELECT ERROR('Error message') END IF;` statements within the BigQuery stored procedure.
*   **Logging and Error Handling**:
    *   All calls to `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`, `DWMSG_MeldeFehler` will be replaced by `INSERT` statements into the `project.dataset.job_log` audit table.
    *   The shell `trap` commands for error handling (`INT`, `STOP`, `CONT`, `ERR`) will be replaced by BigQuery SQL's `BEGIN...EXCEPTION WHEN ERROR THEN...END` block structure, providing similar error capture and logging capabilities.
*   **Script Invocation**:
    *   The execution of `${Name_Kernskript}` will be replaced by a `CALL` statement to the `project.dataset.ausd_bp_ta_bpr_beschr_core` stored procedure, passing all necessary runtime parameters.
*   **Variables**: Shell variables like `ProgName`, `ProgVersion`, `JobKennung`, `ErrNr`, `ErrArg`, `DW_EintragsNr`, `v_sysdate`, `p_stichtag`, `p_wiederanlaufWert`, `LogDatei` will be mapped to `DECLARE`d variables within the BigQuery stored procedure.

## 6. External Dependencies
The original script relies on several sourced shell scripts and custom framework functions. These dependencies will be handled as follows:

*   **Environment Initialization (`. $HOME/.dw_init`)**:
    *   **Original**: Initializes shell environment variables.
    *   **Migration**: BigQuery stored procedures run in a managed environment. Configuration values will be passed as procedure parameters, retrieved from BigQuery configuration tables, or hardcoded as `DECLARE`d constants if static.
*   **Error/Message Framework (`. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` and `DWMSG_*` functions)**:
    *   **Original**: Provides standardized logging and error reporting.
    *   **Migration**: Replaced by direct `INSERT` statements into the `project.dataset.job_log` table, along with BigQuery's native error handling (`EXCEPTION WHEN ERROR`).
*   **Parameter Helper (`. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` and `pruefeParameterGesetzt`)**:
    *   **Original**: Assists in parsing and validating script arguments.
    *   **Migration**: Replaced by explicit `IF` conditions and `SELECT ERROR()` statements within the BigQuery stored procedure, leveraging its parameter passing and control flow features.
*   **Date Helper (`. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` and `DWDate_Gib_Zeitraum`)**:
    *   **Original**: Provides date manipulation utilities.
    *   **Migration**: Replaced by BigQuery's rich set of date and time functions, such as `CURRENT_DATE()`, `FORMAT_DATE()`, `PARSE_DATE()`.
*   **Core Kernel Script (`${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh`)**:
    *   **Original**: Contains the primary business logic.
    *   **Migration**: This will be transformed into `project.dataset.ausd_bp_ta_bpr_beschr_core`, a separate BigQuery Stored Procedure. The `ausd_bp_ta_bpr_beschr_wrapper` procedure will `CALL` this new core procedure.

No external systems (like Oracle, SFTP, S3) were explicitly identified as direct dependencies of this wrapper script, other than the underlying data sources/targets that `k_ausd_bp_ta_bpr_beschr.ksh` might interact with.

## 7. Unresolved / Risks
*   **Core Logic Translation (High Risk)**: The most significant unresolved item is the translation of the `k_ausd_bp_ta_bpr_beschr.ksh` kernel script into the `project.dataset.ausd_bp_ta_bpr_beschr_core` BigQuery Stored Procedure. This migration design provides a framework for the wrapper, but the actual data transformation and loading logic remains to be analyzed and designed. Without the core logic, the end-to-end job cannot function.
*   **Dynamic OS Interactions (Medium Risk)**: While the wrapper script mainly orchestrates, if the core kernel script (`k_ausd_bp_ta_bpr_beschr.ksh`) contains complex filesystem operations, calls to external binaries, or non-SQL specific utilities, these parts cannot be directly translated to BigQuery SQL. They would require re-design using Cloud Functions, Cloud Workflows, or Cloud Composer tasks. The current analysis suggests this is less likely for `k_ausd_bp_ta_bpr_beschr.ksh` as it's assumed to be data-processing focused.
*   **Performance Tuning (Medium Risk)**: Once the core logic is migrated to BigQuery, performance tuning will be critical, especially regarding large datasets and complex operations (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM` filters).
*   **Data Integrity and Idempotency**: The original script mentions `Wiederanlaufwert` for handling restarts and potentially deleting/inserting data. The exact logic for ensuring data integrity and idempotency during restarts in BigQuery will need careful implementation within the `ausd_bp_ta_bpr_beschr_core` procedure.

## 8. Build Plan
The build plan focuses on an ordered sequence of development tasks to implement the migrated job in BigQuery.

1.  **BigQuery Job Log Table Creation (Language: BigQuery DDL)**
    *   Create the `project.dataset.job_log` table with columns like `job_name`, `job_version`, `job_number`, `log_level`, `log_message`, `created_at`, and potentially `error_code`, `error_arg`.
    *   `CREATE TABLE project.dataset.job_log ( ... )`

2.  **`project.dataset.ausd_bp_ta_bpr_beschr_core` Stored Procedure (Language: BigQuery SQL)**
    *   **Phase 1 (Placeholder)**: Create an empty placeholder stored procedure definition.
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_beschr_core`(
          IN p_jobkennung STRING,
          IN p_stichtag STRING,
          IN p_dweintragsnr INT64,
          IN p_wiederanlaufWert INT64
        )
        BEGIN
          -- Placeholder for translated logic from k_ausd_bp_ta_bpr_beschr.ksh
          -- This procedure will be developed in a separate task.
        END;
        ```
    *   **Phase 2 (Full Implementation)**: This will be a subsequent task after the analysis of `k_ausd_bp_ta_bpr_beschr.ksh`. It will involve writing the BigQuery SQL for data selection, filtering, and DML operations.

3.  **`project.dataset.ausd_bp_ta_bpr_beschr_wrapper` Stored Procedure (Language: BigQuery SQL)**
    *   Implement the wrapper stored procedure based on the pseudocode provided by the migration tool:
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_beschr_wrapper`(
          IN p_stichtag STRING,
          IN p_wiederanlaufWert INT64
        )
        BEGIN
          DECLARE v_progname STRING DEFAULT 'Bereitstellung Basisprodukte BERT';
          DECLARE v_progversion STRING DEFAULT 'V2.0.0';
          DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_bpr_beschr';
          DECLARE v_dweintragsnr INT64 DEFAULT 0;
          DECLARE v_sysdate STRING;
          DECLARE v_effective_stichtag STRING;
          DECLARE v_restartwert INT64;
          DECLARE v_log_message STRING;

          SET v_restartwert = IFNULL(p_wiederanlaufWert, 0);
          SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
          SET v_effective_stichtag = IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate);

          -- Parameter validation
          IF v_effective_stichtag IS NULL OR TRIM(v_effective_stichtag) = '' THEN
            INSERT INTO `project.dataset.job_log`
            (job_name, job_version, job_number, log_level, log_message, created_at)
            VALUES
            (v_jobkennung, v_progversion, v_dweintragsnr, 'ERROR', 'Stichtag parameter missing', CURRENT_TIMESTAMP());
            SELECT ERROR('Required parameter Stichtag is missing');
          END IF;

          -- Log job start and determine job number
          INSERT INTO `project.dataset.job_log`
          (job_name, job_version, job_number, log_level, log_message, created_at)
          VALUES
          (v_jobkennung, v_progversion, v_dweintragsnr, 'INFO', 'Job started', CURRENT_TIMESTAMP());

          SET v_dweintragsnr = (
            SELECT IFNULL(MAX(job_number), 0) + 1 FROM `project.dataset.job_log` WHERE job_name = v_jobkennung
          );

          INSERT INTO `project.dataset.job_log`
          (job_name, job_version, job_number, log_level, log_message, created_at)
          VALUES
          (v_jobkennung, v_progversion, v_dweintragsnr, 'INFO', CONCAT('Job number assigned: ', v_dweintragsnr), CURRENT_TIMESTAMP());

          INSERT INTO `project.dataset.job_log`
          (job_name, job_version, job_number, log_level, log_message, created_at)
          VALUES
          (v_jobkennung, v_progversion, v_dweintragsnr, 'INFO', CONCAT('Stichtag=', v_effective_stichtag, ', Sysdate=', v_sysdate), CURRENT_TIMESTAMP());

          BEGIN
            -- Call downstream business procedure
            CALL `project.dataset.ausd_bp_ta_bpr_beschr_core`(
              v_jobkennung,
              v_effective_stichtag,
              v_dweintragsnr,
              v_restartwert
            );

            INSERT INTO `project.dataset.job_log`
            (job_name, job_version, job_number, log_level, log_message, created_at)
            VALUES
            (v_jobkennung, v_progversion, v_dweintragsnr, 'INFO', 'Job completed successfully', CURRENT_TIMESTAMP());

          EXCEPTION WHEN ERROR THEN
            INSERT INTO `project.dataset.job_log`
            (job_name, job_version, job_number, log_level, log_message, created_at)
            VALUES
            (v_jobkennung, v_progversion, v_dweintragsnr, 'ERROR', 'AppError: Abbruch', CURRENT_TIMESTAMP());
            SELECT ERROR('AppError: Abbruch');
          END;
        END;
        ```

4.  **Orchestration Configuration (Language: YAML/Python/BigQuery DDL - dependent on choice)**
    *   If using Cloud Composer, create a Python DAG to invoke the `project.dataset.ausd_bp_ta_bpr_beschr_wrapper` procedure.
    *   If using Cloud Workflows, define a YAML workflow.
    *   If using BigQuery Scheduled Queries, configure the schedule to call the wrapper procedure.

5.  **Testing Plan (Manual/Automated)**
    *   Develop unit tests for the wrapper procedure, covering parameter validation, default values, and logging.
    *   Develop integration tests for the full workflow (wrapper calling core), once `ausd_bp_ta_bpr_beschr_core` is implemented.
    *   Test restart logic and idempotency, especially for `p_wiederanlaufWert`.
    *   Verify all log entries in `project.dataset.job_log`.

6.  **Deployment**: Deploy the BigQuery stored procedures and orchestration configuration to the target environment.