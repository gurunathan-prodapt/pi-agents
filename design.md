# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_bp_ta_bpr_basis.ksh`, serves as an orchestration wrapper. Its primary purpose is to prepare and provide selected basic products (e.g., FAX, Data24) for BERT. It achieves this by creating a cutoff-date extraction of contract cache data from the Data Warehouse (DWH) and delivering this data to the Forderungsscoring (FOS) system. The script handles parameter parsing, date determination, and logging, ultimately delegating the core business logic to an external kernel script. It operates with a `Stichtag` (cutoff date) and supports a `Wiederanlaufwert` (restart value) for resume functionality.

## 2. Source Inventory
The job consists of a single source file:
*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh`
    *   **Technology**: KornShell
    *   **Category**: shell
    *   **Complexity Tier**: medium
    *   **Migration Bucket**: semi_auto
    *   **Summary**: This KornShell script orchestrates the initial provision of selected basic products for BERT by preparing parameters and executing a core processing script.

## 3. Target Architecture
The migration will target Google BigQuery. The existing KornShell wrapper script functionality will be re-implemented as a BigQuery Stored Procedure. This stored procedure will handle parameter validation, default value assignment, and logging. The external helper scripts sourced by the original shell script will be replaced by equivalent BigQuery functions or logic within the stored procedure, or by separate, smaller BigQuery procedures if they contain reusable logic.

A critical component of the target architecture will be the migration of the core kernel script, `k_ausd_bp_ta_bpr_basis.ksh`, which is invoked by this wrapper. This kernel script's functionality, which performs the actual data extraction and processing, will need to be translated into BigQuery SQL DML and potentially integrated directly into the stored procedure or called as a separate stored procedure.

Orchestration of this BigQuery Stored Procedure will be handled by a modern cloud-native orchestrator such as Cloud Composer (Airflow), Cloud Workflows, or Cloud Scheduler, replacing the existing shell script's execution context.

Logging and error handling will be managed through dedicated BigQuery audit and error log tables:
*   `project.dataset.job_audit_log`: To store job metadata, start/end times, and status.
*   `project.dataset.job_error_log`: To record any errors encountered during execution.
*   `project.dataset.job_run_log`: For detailed job run information including log file names.

## 4. Data Flow & Lineage
The original shell script's data flow is primarily one of control and parameter passing, with the actual data manipulation occurring in a sub-process.

**Legacy Flow:**
1.  The `r_ausd_bp_ta_bpr_basis.ksh` script is executed with optional parameters `-s` (Stichtag) and `-l` (Wiederanlaufwert).
2.  It sources several helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) for environment setup, error handling, parameter parsing, and date functions.
3.  It determines the cutoff date (`p_stichtag`), defaulting to the system date if not provided. The `Wiederanlaufwert` is defaulted to `0` if not set.
4.  Parameter validation is performed. If validation fails, an error is logged, usage is printed, and the script exits.
5.  Logging setup is initiated using `DWMSG_` functions, determining a job entry number (`DW_EintragsNr`) and a log file name.
6.  The core kernel script, `k_ausd_bp_ta_bpr_basis.ksh`, is invoked with the parsed parameters (`JobKennung`, `p_stichtag`, `DW_EintragsNr`, `p_wiederanlaufWert`).
7.  Upon successful completion of the kernel script, a success message is logged, and the job status is updated.
8.  Error traps are set up to catch signals and errors, directing them to the `DWMSG_Fehlerbehandlung` function.

**Target BigQuery Flow:**
1.  A Cloud Composer DAG (or similar orchestrator) triggers the BigQuery Stored Procedure `project.dataset.Bereitstellung_Basisprodukte_BERT`.
2.  The stored procedure accepts `p_stichtag` (STRING) and `p_wiederanlaufWert` (INT64) as input parameters.
3.  Inside the procedure, `DW_EintragsNr`, `JobKennung`, `v_sysdate`, and `LogDatei` are declared and initialized.
4.  Parameter defaulting logic is applied for `p_wiederanlaufWert` and `p_stichtag`.
5.  Parameter validation is performed. If `v_stichtag` is null/empty, an error is inserted into `job_error_log`, and a SQLSTATE error is signaled, causing the procedure to abort.
6.  Job audit and run log entries are created/updated in `job_audit_log` and `job_run_log` tables to track job status (STARTED, RUNNING).
7.  A separate BigQuery Stored Procedure `project.dataset.k_ausd_bp_ta_bpr_basis` (representing the migrated kernel script) is called with the necessary parameters.
8.  Upon successful return from the kernel procedure, `job_run_log` and `job_audit_log` are updated to reflect an 'OK' / 'SUCCESS' status.
9.  Error handling within the BigQuery Stored Procedure will use `EXCEPTION` blocks or explicit `IF` conditions to log errors and raise appropriate BigQuery exceptions.

## 5. Transformation Logic
The `r_ausd_bp_ta_bpr_basis.ksh` script primarily manages control flow and parameter preparation, with minimal data transformation itself. The transformation logic for the wrapper script will focus on replicating its control structures and parameter handling within a BigQuery Stored Procedure.

**Parameter Handling:**
*   **`p_stichtag`**: Passed as `STRING` to the BigQuery Stored Procedure. If `NULL` or empty, it defaults to the current system date (`FORMAT_DATE('%d%m%Y', CURRENT_DATE())`).
*   **`p_wiederanlaufWert`**: Passed as `INT64` to the BigQuery Stored Procedure. If `NULL`, it defaults to `0`.
*   Error checking for missing required parameters will be converted to BigQuery `IF` statements, logging to `job_error_log`, and signaling an `SQLSTATE '45000'` error.

**Logging and Audit:**
*   The `DWMSG_` functions and log file generation will be replaced by `INSERT` and `UPDATE` statements against `project.dataset.job_audit_log`, `project.dataset.job_error_log`, and `project.dataset.job_run_log` tables. This ensures centralized, structured logging within BigQuery.
*   The `DW_EintragsNr` will be derived from `MAX(job_id) + 1` from the `job_audit_log` table.

**Orchestration and Execution:**
*   The shell script's mechanism of sourcing helper files (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) will be replaced. Environment variables or configuration will be managed by the orchestrator (e.g., Cloud Composer variables, BigQuery project/dataset settings). Utility functions (like date formatting or parameter validation) will be coded directly into the stored procedure or as separate, small BigQuery functions/procedures.
*   The core invocation of `k_ausd_bp_ta_bpr_basis.ksh` will become a `CALL` statement to its migrated BigQuery Stored Procedure equivalent, `project.dataset.k_ausd_bp_ta_bpr_basis`.

**Example BigQuery SQL Pseudocode (as provided by CM MCP tool):**
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.Bereitstellung_Basisprodukte_BERT`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- Variable declarations and initializations
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64 DEFAULT 0;
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE v_stichtag STRING DEFAULT p_stichtag;
  DECLARE v_wiederanlaufWert INT64 DEFAULT p_wiederanlaufWert;
  DECLARE JobKennung STRING DEFAULT 'ausd_bp_ta_bpr_basis';

  -- Defaulting logic
  IF v_wiederanlaufWert IS NULL THEN
    SET v_wiederanlaufWert = 0;
  END IF;

  IF v_stichtag IS NULL OR v_stichtag = '' THEN
    SET v_stichtag = v_sysdate;
  END IF;

  -- Parameter validation
  IF v_stichtag IS NULL OR v_stichtag = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Stichtag';
  END IF;

  -- Error handling (logging and signaling)
  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.job_error_log` (job_name, error_nr, error_arg, created_at)
    VALUES (JobKennung, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Parameter validation failed';
  END IF;

  -- Job audit logging (STARTED)
  INSERT INTO `project.dataset.job_audit_log` (...) VALUES (...);

  -- Derive DW_EintragsNr and LogDatei for run logging
  SET DW_EintragsNr = (SELECT IFNULL(MAX(job_id), 0) + 1 FROM `project.dataset.job_audit_log`);
  -- SET LogDatei = CONCAT('job_', CAST(DW_EintragsNr AS STRING), '_', JobKennung, '.log'); -- (Log file name for reference)

  -- Job run logging (RUNNING)
  INSERT INTO `project.dataset.job_run_log` (...) VALUES (...);

  -- Call to the migrated kernel script's stored procedure
  CALL `project.dataset.k_ausd_bp_ta_bpr_basis`(JobKennung, v_stichtag, DW_EintragsNr, v_wiederanlaufWert);

  -- Update job run and audit logs (OK/SUCCESS)
  UPDATE `project.dataset.job_run_log` SET status = 'OK', finished_at = CURRENT_TIMESTAMP() WHERE job_id = DW_EintragsNr;
  INSERT INTO `project.dataset.job_audit_log` (...) VALUES (...);
END;
```

## 6. External Dependencies
The original script has several external dependencies, primarily other shell scripts and potentially the DWH system.

*   **Sourced Helper Scripts:**
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error concept.
    *   `${BERT_DIR_ROOT}/allgemeen/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helpers.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling helpers.
    *   **Replacement in BigQuery**: These functionalities will be absorbed directly into the BigQuery Stored Procedure, implemented as BigQuery functions, or their configurations will be managed by the orchestration layer. Shell environment variables will be replaced by BigQuery project/dataset settings or orchestrator variables.
*   **Invoked Kernel Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh`: The core business logic script.
    *   **Replacement in BigQuery**: This script *must* be migrated separately. Its logic will be converted into a dedicated BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_bpr_basis`) that is then called by the wrapper procedure. The parameters passed to it will be translated directly.
*   **DWH (Data Warehouse):** The script indicates interaction with DWH for contract cache data.
    *   **Replacement in BigQuery**: The DWH tables will be migrated to BigQuery tables. The references like `DWH_VERTRAG_ID` and `DWH$TA_C_VERTRAG` will become BigQuery table and column references.
*   **FOS (Forderungsscoring):** The script delivers data to FOS.
    *   **Replacement in BigQuery**: The mechanism for "providing to FOS" will need to be re-evaluated. This could involve BigQuery exports to Cloud Storage, direct data sharing, or a push mechanism via Cloud Functions/Pub/Sub if FOS is an external system with an API.

## 7. Unresolved / Risks
*   **Kernel Script Migration (Major Risk):** The most significant unresolved item is the actual business logic contained within `k_ausd_bp_ta_bpr_basis.ksh`. This migration design document focuses only on the wrapper script. The kernel script itself will require its own detailed migration design to convert its data extraction, transformation, and loading (ETL) logic into BigQuery SQL. This could involve complex SQL queries, views, or even Python scripts using the BigQuery API if the logic is procedural.
*   **`trap` Command Replacement:** The `trap` commands in the KornShell script for OS signal handling (`INT`, `STOP`, `CONT`, `ERR`) do not have a direct equivalent in BigQuery SQL. Error handling will rely on BigQuery's built-in `EXCEPTION` handling mechanisms or explicit `IF` conditions, combined with the orchestration layer's retry and error management capabilities.
*   **Shell `source` Replacement:** Sourcing external shell scripts for environment or utility functions will be replaced by either incorporating the logic directly into the BigQuery Stored Procedure or converting reusable utility logic into BigQuery User-Defined Functions (UDFs) or separate helper procedures.
*   **`FOSHoleLadedatum` function:** The commented-out `FOSHoleLadedatum "DWH$TA_C_VERTRAG" v_ladedatum` indicates a potential external function to get the max load date. If this logic was ever active or needed, its BigQuery equivalent would involve querying the DWH table directly for the `MAX(ladedatum)`. The current script defaults `p_stichtag` to `v_sysdate`.
*   **Language-Specific Features**: KornShell-specific features such as `getopts` for parameter parsing, `print` for output, and `tee -a` for logging to console and file, will be replaced by BigQuery Stored Procedure parameters, logging to audit tables, and standard BigQuery output mechanisms or orchestrator logs.

## 8. Build Plan
The migration will proceed in the following steps:

1.  **Define BigQuery Datasets**:
    *   Create the target BigQuery dataset (e.g., `project.dataset`) for the stored procedures and log tables.
2.  **Create Logging and Audit Tables (BigQuery DDL)**:
    *   `project.dataset.job_audit_log` (job_name STRING, stichtag STRING, wiederanlaufwert INT64, sysdate_value STRING, status STRING, created_at TIMESTAMP)
    *   `project.dataset.job_error_log` (job_name STRING, error_nr INT64, error_arg STRING, created_at TIMESTAMP)
    *   `project.dataset.job_run_log` (job_id INT64, job_name STRING, log_file STRING, stichtag STRING, sysdate_value STRING, status STRING, created_at TIMESTAMP, finished_at TIMESTAMP)
    *   **Language**: BigQuery DDL SQL
3.  **Migrate Helper Script Functionality to BigQuery**:
    *   Identify reusable logic from `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`.
    *   Implement as BigQuery functions or integrate directly into the main stored procedure.
    *   **Language**: BigQuery SQL
4.  **Develop BigQuery Stored Procedure for `k_ausd_bp_ta_bpr_basis.ksh`**:
    *   This is a separate, major migration task for the core logic.
    *   Create `CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_bp_ta_bpr_basis(...)`
    *   **Language**: BigQuery SQL (potentially PySpark or other if complex logic warrants it)
5.  **Develop BigQuery Stored Procedure for `r_ausd_bp_ta_bpr_basis.ksh`**:
    *   Create `CREATE OR REPLACE PROCEDURE project.dataset.Bereitstellung_Basisprodukte_BERT(...)` using the pseudocode provided in the Transformation Logic section.
    *   Include parameter handling, defaulting, validation, logging, and the call to `k_ausd_bp_ta_bpr_basis`.
    *   **Language**: BigQuery SQL
6.  **Create Orchestration (Cloud Composer/Workflows)**:
    *   Develop a Cloud Composer DAG or Cloud Workflow to schedule and execute the `project.dataset.Bereitstellung_Basisprodukte_BERT` BigQuery Stored Procedure.
    *   Configure parameters, error handling, and retries at the orchestrator level.
    *   **Language**: Python (for Airflow DAGs) or YAML/JSON (for Cloud Workflows)