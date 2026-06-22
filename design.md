# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh

## 1. Purpose & Scope
This KornShell script, `k_ausd_v_ta_apn_ve.ksh`, serves as a control script for `r_ausd_vertrag.ksh`. Its primary purpose is to orchestrate a data processing job. Specifically, it manages the execution of an SQL script (`d_ausd_v_ta_apn_ve.sql`), handles job registration, ignores already active jobs, and deactivates old active jobs. It reads input parameters, validates them, and after executing the SQL logic, records the number of processed records.

The scope of this migration is to re-implement this orchestration logic and the underlying SQL processing on the BigQuery platform. This includes translating parameter handling, job control, error logging, and the core data manipulation logic to BigQuery SQL and its ecosystem.

## 2. Source Inventory
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh`
  - **Technology:** KornShell
  - **Category:** shell
  - **Tool:** KornShell
  - **Complexity Tier:** medium
  - **Automation Bucket:** semi_auto
  - **Purpose:** ETL (orchestration)
  - **Migration Flags:** []
  - **Complexity Signals:** (none provided by analysis)
  - **Code Summary:** This script handles parameter parsing, error checking, sources several utility shell scripts for common functions (error handling, date operations, parameter parsing, SQLPlus interaction), sets a target table `ta_apn_ve`, then calls a wrapper function `starteSQLSkript` to execute the main SQL logic found in `d_ausd_v_ta_apn_ve.sql`. It also captures a record count from a temporary file.

## 3. Target Architecture
The target platform is BigQuery. The migration will involve:
- **BigQuery Stored Procedures:** To encapsulate the orchestration logic, parameter handling, error handling, and job control functions currently implemented in the KornShell script. The main script `k_ausd_v_ta_apn_ve.ksh` will be converted into a BigQuery stored procedure, for instance, `project.dataset.control_r_ausd_vertrag`.
- **BigQuery SQL:** The embedded SQL logic within `d_ausd_v_ta_apn_ve.sql` (inferred from the shell script) will be translated into a separate BigQuery SQL script or a BigQuery stored procedure/table function, for instance, `project.dataset.d_ausd_v_ta_apn_ve`.
- **Logging Tables:** Dedicated BigQuery tables for error logging (e.g., `project.dataset.error_log`), job status updates (e.g., `project.dataset.job_log`), and record counts (e.g., `project.dataset.record_count_log`) will replace shell-based logging mechanisms and temporary files.
- **Orchestration:** An external orchestration tool (e.g., Cloud Composer/Airflow, Cloud Workflows) might be used to trigger the main BigQuery stored procedure and manage its scheduling, potentially replacing higher-level job schedulers that call this KornShell script.

## 4. Data Flow & Lineage
The lineage information from `lineage_edges` for this specific file was empty, indicating no explicit `INVOKES`, `READS`, `WRITES`, or `DEPENDS_ON` records were found in the database. However, based on the script content, the inferred data flow and execution order are:

1.  **Environment Initialization:** The script first sources `$HOME/.dw_init` and several utility shell scripts:
    -   `f_alis_msgerr.ksh` (error handling)
    -   `h_alis_date.ksh` (date utilities)
    -   `h_alis_parameter.ksh` (parameter parsing utilities, including `pruefeParameterGesetzt`)
    -   `h_alis_sqlplus.ksh` (SQLPlus interaction utilities, including `starteSQLSkript`)
2.  **Parameter Parsing:** Command-line parameters `p_JobKennung` and `p_EintragsNr` are parsed using `getopts`.
3.  **Parameter Validation:** The `pruefeParameterGesetzt` function is called to validate the parsed parameters.
4.  **Error Handling (Shell):** If parameters are invalid, `DWMSG_MeldeFehler` is called, and the script exits.
5.  **SQL Script Execution:** The `starteSQLSkript` function is called, which executes the main SQL script `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_apn_ve.sql` with `p_EintragsNr` and `p_JobKennung`. This function likely handles the interaction with a database (e.g., Oracle via SQL*Plus) and may write the count of processed records to a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_apn_ve_$$.tmp`).
6.  **Record Count Retrieval:** The script reads the record count from the temporary file into the `v_records` variable.
7.  **Exit:** The script completes its execution.

**Target BigQuery Data Flow:**
- **Orchestration SP:** `project.dataset.control_r_ausd_vertrag`
  - Takes `p_JobKennung` and `p_EintragsNr` as input parameters.
  - Performs parameter validation.
  - Calls another BigQuery stored procedure `project.dataset.starteSQLSkript` (or directly embeds the logic) to execute the core data transformation.
  - Logs errors to `project.dataset.error_log`.
  - Logs job status to `project.dataset.job_log`.
  - Determines record count (e.g., `SELECT COUNT(*) FROM project.dataset.ta_apn_ve WHERE eintrags_nr = p_EintragsNr`) directly within BigQuery.
  - Logs record counts to `project.dataset.record_count_log`.
- **Data Transformation SP/SQL:** `project.dataset.d_ausd_v_ta_apn_ve` (or similar)
  - This procedure would contain the actual `SELECT`, `INSERT`, `UPDATE`, `DELETE` statements that were originally in `d_ausd_v_ta_apn_ve.sql`.
  - It would operate on BigQuery tables, likely including `project.dataset.ta_apn_ve` as a target or source.

## 5. Transformation Logic
The transformation logic for the KornShell script primarily revolves around orchestration, parameter handling, and calling an external SQL script. The actual data transformation is delegated to `d_ausd_v_ta_apn_ve.sql`.

**Original KornShell Logic:**
- **Parameter Reading:** `getopts` for `j` (JobKennung) and `f` (EintragsNr).
- **Parameter Validation:** `pruefeParameterGesetzt` function checks if `p_JobKennung` and `p_EintragsNr` are set.
- **Error Reporting:** `DWMSG_MeldeFehler` and `echo "FEHLER..."` for error output.
- **Environment Sourcing:** `. $HOME/.dw_init` and sourcing several helper `.ksh` scripts.
- **SQL Execution:** `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung` calls an Oracle SQL script using an internal wrapper, passing job parameters.
- **Record Count:** `eval "v_records=\`cat $tmpFile\`"` reads a record count from a temporary file.

**Target BigQuery Logic (Pseudocode for `control_r_ausd_vertrag` SP):**

```sql
-- BigQuery Stored Procedure: control_r_ausd_vertrag
CREATE OR REPLACE PROCEDURE `project.dataset.control_r_ausd_vertrag`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_apn_ve'; -- Hardcoded table name
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation (replaces pruefeParameterGesetzt)
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'EintragsNr';
  END IF;

  -- Error handling (replaces DWMSG_MeldeFehler and shell exit)
  IF ErrNr != 0 THEN
    -- Log to a BigQuery error table
    INSERT INTO `project.dataset.error_log`
      (error_source, error_type, error_number, error_argument, created_at)
    VALUES
      ('control_r_ausd_vertrag', 'E', ErrNr, ErrArg, CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = CONCAT('FEHLER: 0 E ', CAST(ErrNr AS STRING), ' ', ErrArg);
  END IF;

  -- Main SQL execution wrapper replacement (replaces starteSQLSkript)
  -- This call would represent the execution of the converted d_ausd_v_ta_apn_ve.sql logic
  -- Assuming d_ausd_v_ta_apn_ve.sql is also converted into a BQ Stored Procedure
  CALL `project.dataset.d_ausd_v_ta_apn_ve_sp`( -- Example: new SP for the SQL logic
    p_EintragsNr,
    p_JobKennung
  );

  -- Log job completion
  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintrags_nr, tab_name, status, created_at)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, 'ENDE', CURRENT_TIMESTAMP());

  -- Replace temp-file read with direct query result for record count
  -- This assumes ta_apn_ve is the target table where records are added/updated
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.ta_apn_ve` -- Target table in BQ
    WHERE eintrags_nr = p_EintragsNr -- Example: filter by the entry number
  );

  -- Persist record count if needed
  INSERT INTO `project.dataset.record_count_log`
    (job_kennung, eintrags_nr, tab_name, record_count, created_at)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());
END;
```

**Note:** The detailed transformation logic for `d_ausd_v_ta_apn_ve.sql` is not available and would require separate analysis and conversion from its source (likely Oracle SQL) to BigQuery SQL. The above pseudocode assumes this SQL script is also refactored into a BigQuery Stored Procedure.

## 6. External Dependencies
The current script has several external dependencies, primarily other shell scripts and potentially a database connection (likely Oracle, given the `SQL*Plus` context implied by `h_alis_sqlplus.ksh`).

- **External System:** Other KornShell scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
  - **Replacement Strategy:**
    - Error handling (`f_alis_msgerr.ksh`): Replaced by BigQuery's native error handling (`RAISE USING MESSAGE`) and dedicated error logging tables (`project.dataset.error_log`).
    - Date utilities (`h_alis_date.ksh`): Replaced by BigQuery SQL date and time functions (e.g., `CURRENT_TIMESTAMP()`, `DATE_ADD`, `FORMAT_DATE`).
    - Parameter parsing (`h_alis_parameter.ksh`): Replaced by BigQuery Stored Procedure input parameters and BigQuery's `IF` statements for validation.
    - SQL execution (`h_alis_sqlplus.ksh`): Replaced by directly calling BigQuery Stored Procedures for the SQL logic or embedding the SQL statements within the main BigQuery stored procedure.
- **External System:** Database (implied Oracle via `SQL*Plus` interaction).
  - **Replacement Strategy:** All database interactions will be directly against BigQuery tables and views using BigQuery SQL.
- **External System:** Temporary file `$DW_DIR_UTL/bert_k_ausd_v_ta_apn_ve_$$.tmp`.
  - **Replacement Strategy:** Replaced by BigQuery `DECLARE` variables within a stored procedure or temporary BigQuery tables if more complex temporary data storage is needed. The specific record count can be obtained directly via `SELECT COUNT(*)` from the target BigQuery table.
- **External System:** Job table (implied by "Eintrag in die Job-Tabelle" and "JobKennung").
  - **Replacement Strategy:** A dedicated BigQuery logging table (e.g., `project.dataset.job_log`) will be created to store job status and metadata.

## 7. Unresolved / Risks
- **SQL Script `d_ausd_v_ta_apn_ve.sql`:** The content of this SQL script was not available for analysis. This is the most significant unresolved item. Its migration (syntax conversion from source RDBMS, likely Oracle, to BigQuery SQL) is critical and represents the core business logic transformation. It will need separate analysis and conversion.
- **Exact Logic of `starteSQLSkript`:** The `starteSQLSkript` function is a shell wrapper. Its full logic (e.g., how it handles active jobs, job registration, error handling around SQL*Plus execution) needs to be understood to fully replicate its behavior in BigQuery. The pseudocode provides a basic replacement but might need refinement if `starteSQLSkript` has complex retry, logging, or error propagation mechanisms.
- **`DW_DIR_UTL` and `BERT_DIR_ROOT`:** These are environment variables. Their values and how they are set in the legacy environment are important. In BigQuery, these paths will be replaced by dataset and table names.
- **`r_ausd_vertrag.ksh`:** The current script is a "Kontrollscript zu r_ausd_vertrag.ksh". The larger context and scheduling of `r_ausd_vertrag.ksh` are not detailed here, and migrating it might introduce further dependencies or orchestration requirements.
- **File System Operations:** The script uses temporary files and relies on shell environment sourcing. These file system operations are not directly transferable to a serverless BigQuery environment and require re-architecting into BigQuery-native constructs (variables, tables, Cloud Storage if external file processing is truly needed).

## 8. Build Plan
The migration will involve building the following components:

1.  **BigQuery DDL for Logging Tables:**
    -   `project.dataset.error_log` (table for error messages)
    -   `project.dataset.job_log` (table for job status/metadata)
    -   `project.dataset.record_count_log` (table for processed record counts)
    -   **Language:** BigQuery DDL
2.  **BigQuery Stored Procedure for Core SQL Logic (`d_ausd_v_ta_apn_ve.sql`):**
    -   Develop a BigQuery stored procedure (e.g., `project.dataset.d_ausd_v_ta_apn_ve_sp`) that encapsulates the logic from the original `d_ausd_v_ta_apn_ve.sql` script. This requires a separate analysis and conversion effort for the SQL content itself.
    -   **Language:** BigQuery SQL
3.  **BigQuery Stored Procedure for Orchestration (`k_ausd_v_ta_apn_ve.ksh`):**
    -   Create `project.dataset.control_r_ausd_vertrag` BigQuery stored procedure.
    -   This procedure will implement the parameter parsing, validation, error handling, calls to the `d_ausd_v_ta_apn_ve_sp` (or embedded SQL logic), and logging as outlined in the "Transformation Logic" section.
    -   **Language:** BigQuery SQL
4.  **Orchestration (Optional):**
    -   If this job is part of a larger workflow, an Airflow DAG or Cloud Workflow definition might be needed to schedule and trigger the `control_r_ausd_vertrag` BigQuery stored procedure.
    -   **Language:** Python (for Airflow) or YAML (for Cloud Workflows)