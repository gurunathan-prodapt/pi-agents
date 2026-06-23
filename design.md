# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `k_ausd_v_ta_apn_ve.ksh` to Google BigQuery.

The original script serves as a control mechanism for a larger process, specifically `r_ausd_vertrag.ksh`. Its primary functions include:
- Ignoring already active jobs to prevent redundant execution.
- Invoking a core SQL script (`d_ausd_v_ta_apn_ve.sql`) that performs the actual data processing.
- Registering job execution details in a job table.
- Deactivating older active jobs.
- Handling and validating input parameters (`JobKennung`, `EintragsNr`).
- Reporting execution status and errors, including capturing the number of processed records.

The scope of this migration is to re-implement the orchestration and parameter handling logic of this KornShell script, along with its interaction with the downstream SQL, into a BigQuery-native solution, likely a BigQuery Stored Procedure, while preserving its core business logic and control flow.

## 2. Source Inventory
The job is composed of a single KornShell script.

| File Path                                                               | Technology | Complexity Tier | Automation Bucket |
| :---------------------------------------------------------------------- | :--------- | :-------------- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh` | KornShell  | medium          | semi_auto         |

The `complexity_signals` and `file_purpose` were not explicitly captured in the metadata for this file.

## 3. Target Architecture
The target architecture will leverage BigQuery's capabilities for data processing and orchestration.
- **Orchestration**: The control logic currently in the KornShell script will be migrated to a **BigQuery Stored Procedure**. This procedure will encapsulate parameter validation, error handling, and the invocation of the core data processing logic.
- **Data Processing**: The underlying SQL logic (from `d_ausd_v_ta_apn_ve.sql`, which is implicitly called by the KSH script) will be translated into a separate **BigQuery Stored Procedure or SQL script**. The design assumes the `d_ausd_v_ta_apn_ve.sql` also needs migration and will become a BigQuery artifact.
- **Logging and Monitoring**:
    - Error messages will be captured and stored in a dedicated **BigQuery error logging table** (`project.dataset.job_error_log`).
    - Job run details, including the record count, will be logged to a **BigQuery job run log table** (`project.dataset.job_run_log`).
- **Data Storage**: The table `ta_apn_ve` (referenced by the script) will be migrated to a **BigQuery table** within the target dataset (`project.dataset.ta_apn_ve`).
- **Parameterization**: Command-line arguments will be replaced by **stored procedure parameters**.
- **External Orchestration (Optional)**: If the complexity of the overall workflow requires it, an external orchestrator like Cloud Composer (Apache Airflow) or Cloud Workflows could be used to invoke the BigQuery Stored Procedure.

## 4. Data Flow & Lineage
The original script's data flow is primarily an orchestration layer for an underlying SQL script.

**Legacy Flow:**
1. **KornShell Script (`k_ausd_v_ta_apn_ve.ksh`)**:
    - Initializes environment variables by sourcing various utility scripts (e.g., `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    - Parses command-line parameters `-j` (JobKennung) and `-f` (EintragsNr).
    - Validates parameters. If validation fails, logs an error and exits.
    - Defines the name of the main SQL script to execute: `d_ausd_v_ta_apn_ve.sql`.
    - Defines a temporary file path (`$DW_DIR_UTL/bert_k_ausd_v_ta_apn_ve_$$.tmp`) for record counting.
    - Calls a function `starteSQLSkript` (from sourced utilities) which is responsible for executing `d_ausd_v_ta_apn_ve.sql` with the parsed parameters. This function is expected to:
        - Handle active job checks.
        - Execute the SQL script.
        - Update a job table.
        - Deactivate old jobs.
    - Reads the record count from the temporary file.
    - Prints completion messages.

**Target BigQuery Flow:**
1. **BigQuery Orchestration Stored Procedure (`project.dataset.r_ausd_vertrag_control`)**:
    - Accepts `p_JobKennung` and `p_EintragsNr` as `IN` parameters.
    - Performs parameter validation using BigQuery scripting `IF` statements.
    - If validation fails, logs an error into `project.dataset.job_error_log` and raises an error (`SIGNAL SQLSTATE`).
    - Calls the core data processing BigQuery Stored Procedure (`project.dataset.d_ausd_v_ta_apn_ve`) with the validated parameters.
        - *Assumption*: This procedure will handle the logic for ignoring active jobs, executing the main data logic, updating the job table, and deactivating old jobs.
    - After the core procedure completes, it queries the `project.dataset.ta_apn_ve` table to get the count of processed records (e.g., `COUNT(*) WHERE eintrags_nr = p_EintragsNr`).
    - Logs the job run details and the record count into `project.dataset.job_run_log`.
    - Outputs a completion message (e.g., using `SELECT '...' AS message`).

## 5. Transformation Logic
The transformation primarily involves converting shell scripting constructs into BigQuery scripting language and T-SQL equivalents.

**Key Transformations:**

- **Shell Sourcing (`. /path/to/script.ksh`)**:
    - Replaced by defining equivalent helper functions or consolidating logic directly within the main BigQuery Stored Procedure.
    - Environment variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) will be replaced by:
        - BigQuery project/dataset references.
        - Stored procedure constants or configuration tables.
        - BigQuery functions (e.g., for temporary file handling, replace with BigQuery tables).
- **Parameter Parsing (`getopts`)**:
    - Replaced by BigQuery Stored Procedure input parameters (`IN p_JobKennung STRING`, `IN p_EintragsNr STRING`).
- **Parameter Validation (`pruefeParameterGesetzt`)**:
    - Replaced by BigQuery scripting `IF` conditions and `IS NULL`/`='' `checks.
    - Error handling will use `INSERT` statements into an error log table and `SIGNAL SQLSTATE` for procedure termination.
- **SQL Script Execution (`starteSQLSkript`)**:
    - Replaced by `CALL` to a migrated BigQuery Stored Procedure (`project.dataset.d_ausd_v_ta_apn_ve`) that contains the transformed SQL logic.
- **Temporary File (`tmpFile`, `cat $tmpFile`)**:
    - Replaced by direct `SELECT COUNT(*)` queries against the target table (`project.dataset.ta_apn_ve`) or intermediate tables, storing the result in a `DECLARE`d variable (`v_records INT64`).
    - If a temporary output table is needed for intermediate results, a transient BigQuery table can be created and dropped within the procedure.
- **Error Logging (`DWMSG_MeldeFehler`, `echo "FEHLER..."`)**:
    - Replaced by `INSERT` statements into a `project.dataset.job_error_log` table, capturing relevant error details (job ID, entry number, error code, message, timestamp).
- **Exit Codes (`exit $ErrNr`)**:
    - Replaced by `SIGNAL SQLSTATE '45000'` with a custom message to indicate failure and stop procedure execution.

**Example BigQuery Pseudocode (from MCP tool output):**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_apn_ve';
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 0; -- Original script sets ErrNr=0 then checks for it. Need to replicate this logic carefully or adapt.
    SET ErrArg = 'Jobkennung';
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET ErrNr = 0;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.job_error_log`
    (job_kennung, eintrags_nr, error_nr, error_arg, error_ts)
    VALUES
    (p_JobKennung, p_EintragsNr, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen';
  END IF;

  BEGIN -- Main processing block
    CALL `project.dataset.d_ausd_v_ta_apn_ve`(
      p_EintragsNr,
      p_JobKennung
    );

    SET v_records = (
      SELECT COUNT(*)
      FROM `project.dataset.ta_apn_ve`
      WHERE eintrags_nr = p_EintragsNr
    );

    INSERT INTO `project.dataset.job_run_log`
    (job_kennung, eintrags_nr, tab_name, records, run_ts)
    VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.job_error_log`
    (job_kennung, eintrags_nr, error_nr, error_arg, error_ts)
    VALUES
    (p_JobKennung, p_EintragsNr, 1, 'SQL execution failed', CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'SQL execution failed';
  END;
END;
```

## 6. External Dependencies
Based on the `lineage_assembled_jobs` analysis, there are no external systems explicitly identified (`external_systems: []`).

The script does rely on:
- **Environment variables**: `$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`. These will be replaced by direct BigQuery project/dataset references, configuration tables, or procedure constants.
- **Sourced utility scripts**:
    - `.dw_init`
    - `f_alis_msgerr.ksh`
    - `h_alis_date.ksh`
    - `h_alis_parameter.ksh`
    - `h_alis_sqlplus.ksh`
    These will be absorbed into the BigQuery Stored Procedure's logic, either by direct translation of their functions or by utilizing BigQuery's native capabilities for error handling, date functions, parameter handling, and SQL execution.
- **Downstream SQL script**: `d_ausd_v_ta_apn_ve.sql`. This is a critical dependency and will need its own migration to a BigQuery Stored Procedure or equivalent SQL script.

## 7. Unresolved / Risks
- **Unresolved Targets**: The `lineage_assembled_jobs` analysis indicates no `unresolved_targets`.
- **Core SQL Script (`d_ausd_v_ta_apn_ve.sql`)**: The actual business logic is in this SQL script, which is not part of the current job's component files. Its migration is assumed but requires separate analysis and design. This is the biggest risk for a complete end-to-end migration.
- **`starteSQLSkript` function**: The exact implementation of this function and the functions within the sourced scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) is not fully known without analyzing their code. The migration design makes assumptions about their functionalities (e.g., error logging, date handling, SQL execution). A thorough review of these helper scripts is crucial to ensure all functionality is replicated correctly in BigQuery.
- **`DWMSG_MeldeFehler`**: The specific implementation of this error messaging system needs to be fully understood to replicate its logging and reporting accurately in BigQuery.
- **`eval "v_records=`cat $tmpFile`"`**: The precise mechanism by which the temporary file is populated with the record count is not clear from the shell script alone. It's assumed the `starteSQLSkript` function or the underlying `d_ausd_v_ta_apn_ve.sql` is responsible for this. The BigQuery design proposes a direct `SELECT COUNT(*)` on the target table, which might need adjustment based on how `v_records` is truly generated.

## 8. Build Plan
The build plan focuses on creating the necessary BigQuery components.

1. **Schema Definition (DDL)**:
    - Create the target BigQuery table `project.dataset.ta_apn_ve`.
    - Create logging tables:
        - `project.dataset.job_error_log` (to store error details).
        - `project.dataset.job_run_log` (to store job execution metadata and record counts).
    - **Language**: BigQuery DDL

2. **Core Data Processing Stored Procedure**:
    - Develop the BigQuery Stored Procedure for `project.dataset.d_ausd_v_ta_apn_ve`, which will contain the translated logic from the original `d_ausd_v_ta_apn_ve.sql` script. This procedure will be responsible for the actual data transformations, job table updates, and deactivation of older jobs.
    - **Language**: BigQuery SQL (Stored Procedure)

3. **Orchestration Stored Procedure**:
    - Develop the BigQuery Stored Procedure `project.dataset.r_ausd_vertrag_control` based on the pseudocode provided in Section 5. This procedure will handle parameter validation, error logging, and invocation of `project.dataset.d_ausd_v_ta_apn_ve`.
    - **Language**: BigQuery SQL (Stored Procedure)

4. **Configuration (Optional)**:
    - If externalized configuration is needed (e.g., for `BERT_DIR_ROOT`), define and populate a BigQuery configuration table or use BigQuery constants within procedures.
    - **Language**: BigQuery DDL/DML

5. **External Orchestration (Optional)**:
    - If required, create an Airflow DAG (using Python) in Cloud Composer or a Cloud Workflow definition (using YAML) to schedule and invoke the `project.dataset.r_ausd_vertrag_control` BigQuery Stored Procedure.
    - **Language**: Python (Airflow) or YAML (Cloud Workflows)