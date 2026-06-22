# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_barrier.ksh`. The script serves as a wrapper and orchestration component for a contract data reconciliation job specifically for the `ta_barrier` table. Its primary functions include setting up the execution environment, parsing command-line parameters, initializing logging and error handling, and orchestrating the execution of a core data reconciliation script, `k_ausd_v_ta_barrier.ksh`. The migration aims to re-implement this orchestration logic within Google Cloud's BigQuery environment.

## 2. Source Inventory
The job consists of a single source file:

*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh`
*   **Technology**: KornShell
*   **Summary**: A KornShell script acting as a wrapper to manage environment setup, parameter handling, logging, and execution of a core data reconciliation script for the `ta_barrier` table.
*   **Complexity Tier**: Unknown (information not found in `file_complexity`)
*   **Automation Bucket**: Unknown (information not found in `automation_rate`)

## 3. Target Architecture
The target architecture will leverage Google BigQuery's capabilities for data processing and orchestration.

*   **Main Component**: A BigQuery Stored Procedure, `project.dataset.Vertragsdatenabgleich`, will replace the main KornShell wrapper script.
*   **Core Logic**: The core data reconciliation logic, currently residing in `k_ausd_v_ta_barrier.ksh`, is assumed to be migrated into a separate BigQuery Stored Procedure, `project.dataset.k_ausd_v_ta_barrier`, which will be invoked by the wrapper procedure.
*   **Logging and Auditing**: Dedicated BigQuery tables will be created to replace the existing file-based logging and error reporting mechanisms:
    *   `project.dataset.job_control`: For managing job entries, status, and metadata.
    *   `project.dataset.job_log`: For storing detailed log messages.
    *   `project.dataset.job_error_log`: For recording error incidents.

## 4. Data Flow & Lineage
The original script `r_ausd_v_ta_barrier.ksh` acts as an orchestrator.
1.  **Initialization**: It sources several utility scripts and environment variables for setup:
    *   `. $HOME/.dw_init` (environment initialization)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error messaging framework)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter handling utilities)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling utilities)
2.  **Parameter Processing**: It parses command-line parameters using `getopts`.
3.  **Error Checking**: Validates parsed parameters and exits if critical errors are found.
4.  **Logging Setup**: Initializes job-specific logging, including determining a unique job entry number, log file name, and setting up traps for signal handling.
5.  **Core Script Invocation**: The script executes the core data reconciliation logic by invoking `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_barrier.ksh` with specific parameters (`-j $JobKennung -f ${DW_EintragsNr}`). The output of this core script is redirected to the log file.
6.  **Completion**: Upon successful completion, it logs a success message and updates the job status. In case of errors, the installed traps would handle reporting and exit.

In BigQuery, this flow will be managed by the main stored procedure calling the core logic stored procedure, with all logging and status updates performed via inserts/updates to the dedicated BigQuery log tables.

## 5. Transformation Logic
The transformation logic for the wrapper script primarily involves converting shell scripting constructs into BigQuery SQL (or BigQuery Scripting for procedural logic).

*   **Parameter Handling**: `getopts` logic will be replaced by standard BigQuery Stored Procedure parameters and conditional `IF` statements for validation.
*   **Environment Initialization**: Sourcing of `.dw_init` and utility `.ksh` files will be replaced by:
    *   Directly incorporating necessary values as `DECLARE` variables or parameters within the BigQuery Stored Procedure.
    *   Migrating common shell functions (like `DWMSG_*` routines) into separate BigQuery SQL UDFs or helper stored procedures if they contain reusable logic, or inlining them.
*   **Error Handling**: The `set -eu` and `trap` mechanisms will be replaced by BigQuery's `EXCEPTION WHEN ERROR THEN` blocks within a `BEGIN...END` structure. Error details will be logged to `job_error_log` and the procedure will `SIGNAL SQLSTATE` to indicate failure.
*   **Logging**: All `print` and `tee` operations writing to `LogDatei` will be converted to `INSERT` statements into the `job_log` table. Job control and status updates will translate to `INSERT` and `UPDATE` statements on the `job_control` table.
*   **Core Script Invocation**: The execution of `k_ausd_v_ta_barrier.ksh` will be replaced by a `CALL` to the corresponding BigQuery Stored Procedure: `CALL project.dataset.k_ausd_v_ta_barrier(JobKennung, DW_EintragsNr);`.

**BigQuery SQL Pseudocode (Orchestration Wrapper)**:

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.Vertragsdatenabgleich`(
  IN p_h STRING,
  IN p_s STRING,
  IN p_l STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64 DEFAULT 0;
  DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_BARRIER';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE LogDatei STRING DEFAULT ''; -- Will be a logical name, actual logging to table
  DECLARE Name_Kernskript STRING DEFAULT 'project.dataset.k_ausd_v_ta_barrier';
  DECLARE usage_text STRING DEFAULT '''
    Programm: Vertragsdatenabgleich
    Version:  V1.0.0
    Aufruf:   Parameter
    Parameter:
        -h     zeigt diese Seite an

    Beschreibung:
        Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_barrier.
  ''';

  -- Parameter handling
  IF p_h IS NOT NULL AND p_h = '-h' THEN
    SELECT usage_text AS usage;
    LEAVE;
  END IF;

  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.job_error_log`
      (job_kennung, eintrags_nr, err_nr, err_arg, created_at)
    VALUES
      (JobKennung, DW_EintragsNr, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SELECT usage_text AS usage;
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = CONCAT('Parameterfehler: ', CAST(ErrNr AS STRING), ' ', ErrArg);
  END IF;

  -- Get unique job entry number
  SET DW_EintragsNr = (
    SELECT IFNULL(MAX(eintrags_nr), 0) + 1
    FROM `project.dataset.job_control`
    WHERE job_kennung = JobKennung
  );

  -- Determine logical log file name (for logging purposes in tables)
  SET LogDatei = CONCAT('log_', JobKennung, '_', CAST(DW_EintragsNr AS STRING), '.log');

  -- Create initial job control entry
  INSERT INTO `project.dataset.job_control`
    (eintrags_nr, job_kennung, script_name, log_datei, stichtag_info, status, created_at)
  VALUES
    (DW_EintragsNr, JobKennung, 'Vertragsdatenabgleich', LogDatei, v_sysdate, 'RUNNING', CURRENT_TIMESTAMP());

  -- Set reference date
  UPDATE `project.dataset.job_control`
  SET stichtag_info = v_sysdate
  WHERE eintrags_nr = DW_EintragsNr
    AND job_kennung = JobKennung;

  BEGIN
    -- Core script replacement: Call the migrated core stored procedure
    CALL `project.dataset.k_ausd_v_ta_barrier`(JobKennung, DW_EintragsNr);

    -- Log success message
    INSERT INTO `project.dataset.job_log`
      (eintrags_nr, job_kennung, log_level, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, 'INFO', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

    -- Update job status to OK
    UPDATE `project.dataset.job_control`
    SET status = 'OK',
        finished_at = CURRENT_TIMESTAMP()
    WHERE eintrags_nr = DW_EintragsNr
      AND job_kennung = JobKennung;

  EXCEPTION WHEN ERROR THEN
    -- Handle errors and log
    INSERT INTO `project.dataset.job_log`
      (eintrags_nr, job_kennung, log_level, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, 'ERROR', 'AppError: Abbruch', CURRENT_TIMESTAMP());

    UPDATE `project.dataset.job_control`
    SET status = 'ERROR',
        finished_at = CURRENT_TIMESTAMP()
    WHERE eintrags_nr = DW_EintragsNr
      AND job_kennung = JobKennung;

    RAISE USING MESSAGE = 'AppError: Abbruch';
  END;

END;
```

## 6. External Dependencies
The `lineage_assembled_jobs` record indicated no explicit external systems. The shell script itself defines and references several components:

*   **Environment Initialization**: `. $HOME/.dw_init` will need its relevant environment variables (e.g., `BERT_DIR_ROOT`) to be defined and passed as parameters or configured in the BigQuery execution environment (e.g., Cloud Composer variables).
*   **Utility Scripts**:
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    These utility scripts, particularly the `DWMSG_*` functions, will need to be re-implemented as BigQuery helper procedures or inlined within the main wrapper procedure.
*   **Core Logic Script**: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_barrier.ksh` is a critical dependency. This script must be migrated to a separate BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_barrier`) before or in conjunction with this wrapper script.

## 7. Unresolved / Risks
*   **Core Logic Migration**: This design focuses solely on the wrapper script. The actual data reconciliation logic within `k_ausd_v_ta_barrier.ksh` is a significant unresolved item and must undergo its own migration design.
*   **Missing Complexity/Automation Data**: The `file_complexity` and `automation_rate` for the source file were not available, which means the estimated effort and precise migration bucket (`B0-B4`) are unknown. The migration is assumed to be `B1` (automated) or `B2` (semi-automated) given the nature of the script and the capabilities of the CM MCP tool.
*   **Shell-specific constructs**: Shell-specific features like `trap`, `source` command for shell files, `print` to console, `tee` for file redirection, and direct external shell script execution do not have direct BigQuery SQL equivalents. These will be replaced by BigQuery's procedural language features, error handling, and BigQuery table-based logging.
*   **JobKennung and DW_EintragsNr management**: The logic for generating `DW_EintragsNr` and `LogDatei` relies on sequence-like generation and concatenation. In BigQuery, this is handled by querying a control table and using string functions.

## 8. Build Plan
The build plan will involve creating the necessary BigQuery assets.

1.  **Create BigQuery Dataset**:
    *   `project.dataset` (if it doesn't already exist).
2.  **Create BigQuery Control and Log Tables**:
    *   `project.dataset.job_control`: This table will store metadata about job executions.
        ```sql
        CREATE TABLE `project.dataset.job_control` (
          eintrags_nr INT64,
          job_kennung STRING,
          script_name STRING,
          log_datei STRING,
          stichtag_info STRING,
          status STRING,
          created_at TIMESTAMP,
          finished_at TIMESTAMP
        );
        ```
    *   `project.dataset.job_log`: This table will store detailed log messages from job executions.
        ```sql
        CREATE TABLE `project.dataset.job_log` (
          eintrags_nr INT64,
          job_kennung STRING,
          log_level STRING,
          message STRING,
          created_at TIMESTAMP
        );
        ```
    *   `project.dataset.job_error_log`: This table will store records of errors encountered during job executions.
        ```sql
        CREATE TABLE `project.dataset.job_error_log` (
          job_kennung STRING,
          eintrags_nr INT64,
          err_nr INT64,
          err_arg STRING,
          created_at TIMESTAMP
        );
        ```
3.  **Develop/Migrate `k_ausd_v_ta_barrier` Core Logic**:
    *   **(Prerequisite)** The core data reconciliation logic currently in `k_ausd_v_ta_barrier.ksh` needs to be migrated into its own BigQuery Stored Procedure: `project.dataset.k_ausd_v_ta_barrier`. The design for this core script is out of scope for this document but is a critical dependency.
4.  **Create BigQuery Stored Procedure `Vertragsdatenabgleich`**:
    *   Deploy the BigQuery SQL pseudocode provided in Section 5 as a stored procedure named `project.dataset.Vertragsdatenabgleich`.
    *   **Language**: BigQuery SQL
5.  **Orchestration (Optional but Recommended)**:
    *   If scheduled execution is required, integrate the `CALL project.dataset.Vertragsdatenabgleich` into a Cloud Composer DAG or Cloud Workflows definition.
    *   **Language**: Python (for Cloud Composer) or YAML (for Cloud Workflows).