# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_p_discount.ksh`. The script serves as an orchestration wrapper for a data synchronization process concerning the `ta_p_discount` table. Its primary purpose is to set up the execution environment, parse parameters, manage logging and error handling, and invoke a core processing script (`k_ausd_v_ta_p_discount.ksh`) that presumably handles the actual data reconciliation. The scope of this migration focuses on transforming this shell-based orchestration into a BigQuery-native solution, leveraging BigQuery stored procedures and associated logging/control mechanisms.

## 2. Source Inventory
The job is composed of a single KornShell script.

*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh`
*   **Technology**: KornShell
*   **Purpose**: Orchestration/Wrapper script for `ta_p_discount` data synchronization.
*   **Complexity Tier**: medium
*   **Migration Bucket**: semi_auto

## 3. Target Architecture
The migrated solution will primarily reside in Google Cloud's BigQuery.

*   **Orchestration**: The shell script's orchestration logic (parameter handling, job control, logging, invocation of the core script) will be translated into a BigQuery Stored Procedure, named `project.dataset.BERT_V_TA_P_DISCOUNT`.
*   **Logging and Control**: Dedicated BigQuery tables will be created to replace the shell script's logging and status tracking mechanisms:
    *   `project.dataset.job_control`: To manage job entries, status, and metadata.
    *   `project.dataset.job_log`: To store detailed log messages.
    *   `project.dataset.job_error_log`: To record error details during execution.
*   **Core Logic Invocation**: The invocation of the `k_ausd_v_ta_p_discount.ksh` will be replaced by a BigQuery Stored Procedure call, `project.dataset.k_ausd_v_ta_p_discount`, assuming the core logic is also migrated to a BigQuery Stored Procedure.
*   **External Orchestration (Optional)**: For advanced scheduling, dependency management, or integration with other systems, Cloud Workflows or Cloud Composer (Airflow) can be considered to invoke the BigQuery Stored Procedure.

## 4. Data Flow & Lineage
The original script `r_ausd_v_ta_p_discount.ksh` acts as a wrapper.

*   **Execution Flow**:
    1.  `r_ausd_v_ta_p_discount.ksh` (ksh) starts.
    2.  It sources various utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) for environment setup, error handling, and parameter/date functions.
    3.  It initializes job-specific variables and logging.
    4.  It invokes the core processing script: `k_ausd_v_ta_p_discount.ksh`.
    5.  It logs the success or failure of the core script's execution.
*   **Data Flow**: This wrapper script itself does not directly process data. It orchestrates the process for the `ta_p_discount` table, implying that `k_ausd_v_ta_p_discount.ksh` is responsible for reading from source systems, performing transformations, and writing to the `ta_p_discount` target.
*   **Lineage**: The `lineage_edges` query did not return explicit invocation relationships. However, the source code clearly indicates an `INVOKES` relationship from `r_ausd_v_ta_p_discount.ksh` to `k_ausd_v_ta_p_discount.ksh`. The latter is the main data processing component for `ta_p_discount`.

## 5. Transformation Logic
The `r_ausd_v_ta_p_discount.ksh` script contains minimal transformation logic itself. Its primary function is job metadata manipulation:
*   **Job Identifier**: Converts `JobKennung` to uppercase (`BERT_V_TA_P_DISCOUNT`).
*   **Date Formatting**: Formats the current system date to `DDMMYYYY`.
*   **Parameter Handling**: Parses command-line parameters `-s`, `-l`, and `-h`.
All business-specific data transformations and aggregations related to the `ta_p_discount` table are delegated to the `k_ausd_v_ta_p_discount.ksh` script, which will need its own dedicated migration design.

## 6. External Dependencies
The `lineage_assembled_jobs` record indicates no explicit external systems (`external_systems: []`) for this job. However, the script itself has several internal dependencies.

*   **Sourced Shell Files**:
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utility.
    *   **Migration Plan**: These will be replaced by BigQuery's native capabilities (e.g., UDFs for date formatting, stored procedure parameters for `getopts` logic, and logging/control tables for error messages). Environment variables will be handled via BigQuery procedure parameters or configured at the job execution level (e.g., in a Cloud Composer DAG).
*   **Core Job Script**:
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh`: This is the crucial dependency containing the core data reconciliation logic.
    *   **Migration Plan**: This script must be migrated separately. In the target BigQuery architecture, it is expected to be a BigQuery Stored Procedure, `project.dataset.k_ausd_v_ta_p_discount`, called by the wrapper procedure.
*   **System Commands**: `date`, `getopts`, `tee`, `trap`, `print`.
    *   **Migration Plan**: These will be replaced by BigQuery SQL functions (e.g., `FORMAT_DATE` for `date`), control flow (`IF`/`ELSE`), logging inserts, and BigQuery's built-in error handling (`EXCEPTION WHEN ERROR`).

## 7. Unresolved / Risks
*   **Core Script Dependency**: The full functionality of the job hinges on `k_ausd_v_ta_p_discount.ksh`. This migration design is for the wrapper only. The core script needs to be analyzed and migrated to BigQuery SQL, potentially as a stored procedure or a series of SQL statements. The current design assumes this core script will be migrated to `project.dataset.k_ausd_v_ta_p_discount`.
*   **Shell-specific features**:
    *   `trap` handling for `INT` and `ERR` signals cannot be directly replicated in BigQuery SQL. Equivalent behavior will be achieved through `EXCEPTION WHEN ERROR` blocks and status updates in the `job_control` table.
    *   Shell file sourcing (`. $HOME/.dw_init`) will be replaced by BigQuery procedure parameters or configuration data.
    *   `tee`/stdout redirection to log files will be replaced by inserting records into BigQuery logging tables.
*   **Parameter `s` and `l`**: The source script declares these parameters but does not explicitly handle them in the provided code. Their purpose in the original script or their expected values are unknown and need clarification during the migration of the core script.
*   **No explicit `unresolved_targets`** or `external_systems` were found in the lineage analysis, which suggests the primary unknowns are within the logic of the dependent `k_ausd_v_ta_p_discount.ksh` script and the specific implementations of the sourced shell utilities.

## 8. Build Plan
The build plan involves creating the necessary BigQuery assets.

1.  **Define BigQuery Tables**:
    *   `job_control` table (for job metadata, status, etc.)
    *   `job_log` table (for detailed log messages)
    *   `job_error_log` table (for error tracking)
    *   **Language**: BigQuery DDL (Data Definition Language)
2.  **Create BigQuery Stored Procedure for Orchestration**:
    *   Translate the KornShell wrapper logic into a BigQuery Stored Procedure, `project.dataset.BERT_V_TA_P_DISCOUNT`.
    *   This procedure will handle parameter validation, job entry creation, logging, date formatting, and the invocation of the core reconciliation procedure.
    *   **Language**: BigQuery SQL
    *   **Code Example (Pseudocode)**:
    ```sql
    -- BigQuery Stored Procedure: wrapper orchestration for ta_p_discount reconciliation

    CREATE OR REPLACE PROCEDURE `project.dataset.BERT_V_TA_P_DISCOUNT`(
      IN p_s STRING,
      IN p_l STRING,
      IN p_h BOOL
    )
    BEGIN
      DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
      DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
      DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_P_DISCOUNT';
      DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
      DECLARE DW_EintragsNr INT64 DEFAULT 0;
      DECLARE ErrNr INT64 DEFAULT 0;
      DECLARE ErrArg STRING DEFAULT '';
      DECLARE LogDatei STRING DEFAULT '';
      DECLARE Name_Kernskript STRING DEFAULT 'project.dataset.k_ausd_v_ta_p_discount';
      DECLARE v_status STRING DEFAULT 'INIT';

      -- Usage/help branch
      IF p_h = TRUE THEN
        SELECT
          ProgName AS Programm,
          ProgVersion AS Version,
          'Aufruf: Parameter -h zeigt diese Seite an' AS Beschreibung;
        RETURN;
      END IF;

      -- Parameter validation (simplified for pseudocode)
      IF ErrNr != 0 THEN
        INSERT INTO `project.dataset.job_error_log`
        (job_name, job_entry_nr, error_nr, error_arg, created_at)
        VALUES
        (JobKennung, DW_EintragsNr, ErrNr, ErrArg, CURRENT_TIMESTAMP());

        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'Parameter validation failed';
      END IF;

      -- Job number and log file initialization
      SET DW_EintragsNr = (
        SELECT IFNULL(MAX(job_entry_nr), 0) + 1
        FROM `project.dataset.job_control`
        WHERE job_name = JobKennung
      );

      SET LogDatei = CONCAT(JobKennung, '_', CAST(DW_EintragsNr AS STRING), '.log');

      INSERT INTO `project.dataset.job_control`
      (job_entry_nr, job_name, script_name, log_file, status, stichtag, created_at)
      VALUES
      (DW_EintragsNr, JobKennung, 'BERT_V_TA_P_DISCOUNT.sql', LogDatei, 'RUNNING', v_sysdate, CURRENT_TIMESTAMP());

      -- Core processing call placeholder
      BEGIN
        CALL `project.dataset.k_ausd_v_ta_p_discount`(JobKennung, DW_EintragsNr);

        SET v_status = 'OK';

        INSERT INTO `project.dataset.job_log`
        (job_entry_nr, job_name, log_message, created_at)
        VALUES
        (DW_EintragsNr, JobKennung, 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

        UPDATE `project.dataset.job_control`
        SET status = 'OK',
            finished_at = CURRENT_TIMESTAMP()
        WHERE job_entry_nr = DW_EintragsNr
          AND job_name = JobKennung;

      EXCEPTION WHEN ERROR THEN
        SET v_status = 'ERROR';

        INSERT INTO `project.dataset.job_log`
        (job_entry_nr, job_name, log_message, created_at)
        VALUES
        (DW_EintragsNr, JobKennung, 'AppError: Abbruch', CURRENT_TIMESTAMP());

        UPDATE `project.dataset.job_control`
        SET status = 'ERROR',
            finished_at = CURRENT_TIMESTAMP()
        WHERE job_entry_nr = DW_EintragsNr
          AND job_name = JobKennung;

        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'Job aborted due to error';
      END;

    END;
    ```