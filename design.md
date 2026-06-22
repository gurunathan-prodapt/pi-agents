# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh

## 1. Purpose & Scope
This job serves as a wrapper or orchestration script (`Rahmenskript`) for updating the `ta_c_bfc` table, which is identified as the "Bindefristcache" (binding period cache). Its primary responsibilities include setting up the execution environment, parsing command-line parameters, initializing job and error logging, invoking a core processing script (`k_ausd_v_ta_c_bfc.ksh`), and finally, marking the job's success or handling any encountered errors. The script does not contain direct data transformation logic; it delegates this to the core script it calls.

## 2. Source Inventory
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh`
  - **Technology:** Korn Shell (KSH) Script
  - **Role:** ETL Orchestrator / Wrapper
  - **Complexity Tier:** Not available from `file_complexity` analysis. Based on its wrapper nature, it's assessed as **Simple**.
  - **Automation Bucket:** Not available from `automation_rate` analysis. Given the clear mapping to BigQuery stored procedures, it's assessed as **B1 (Automated)**.
  - **Summary:** This script orchestrates the execution of a core script responsible for updating the `ta_c_bfc` table. It manages environment setup, parameter passing, and job-level logging and error handling.

## 3. Target Architecture
The migration will convert the KSH wrapper script into a BigQuery Stored Procedure. This procedure will encapsulate the orchestration logic, parameter handling, and error management.
- **Main Component:** `dataset.BERT_V_TA_C_BFC` (BigQuery Stored Procedure).
- **Logging/Auditing:** Dedicated BigQuery log tables (e.g., `dataset.dw_job_log`) will replace the filesystem-based logging. Utility functions for determining entry numbers (`DWMSG_ErmittleNr`), log filenames (`DWMSG_Logdateiname`), and status updates (`DWMSG_SetzeStatusOK`) will be migrated into separate BigQuery Stored Procedures or functions, or integrated directly into the main procedure if simple enough.
- **Orchestration:** The execution of the main BigQuery Stored Procedure will be managed by a BigQuery-native scheduler or an external orchestrator like Cloud Composer (Airflow), which can handle parameter passing and scheduling.
- **Core Logic:** The core processing script `k_ausd_v_ta_c_bfc.ksh` is assumed to be migrated to a separate BigQuery Stored Procedure (e.g., `dataset.k_ausd_v_ta_c_bfc`) or a series of SQL statements/views that perform the actual data transformation on the `ta_c_bfc` table.

## 4. Data Flow & Lineage
The original script `r_ausd_v_ta_c_bfc.ksh` has the following simplified flow:
1. **Environment Initialization:** Sources `$HOME/.dw_init` and various utility KSH scripts for error handling and date functions.
2. **Parameter Processing:** Parses command-line arguments.
3. **Logging Setup:** Initializes job-specific logging, including a unique entry number and log file name.
4. **Core Script Invocation:** Executes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh` with parameters, redirecting its output to a log file.
5. **Status Update:** Records success or failure in the logging system.

In BigQuery, this flow will translate to:
1. **External Trigger:** A scheduler (e.g., Cloud Composer, Cloud Scheduler) invokes the `dataset.BERT_V_TA_C_BFC` BigQuery Stored Procedure, passing any necessary parameters.
2. **Parameter Handling:** The stored procedure directly receives parameters.
3. **Logging Procedures:** Calls to `DWMSG_` functions will be replaced by calls to BigQuery Stored Procedures (e.g., `dataset.dwmsg_ermittle_nr`, `dataset.dwmsg_logdateiname`, `dataset.dwmsg_setze_status_ok`) which insert records into the `dataset.dw_job_log` table.
4. **Core Logic Execution:** The BigQuery Stored Procedure `dataset.BERT_V_TA_C_BFC` will call another BigQuery Stored Procedure, `dataset.k_ausd_v_ta_c_bfc`, which contains the actual data update logic for the `ta_c_bfc` table.
5. **Error Handling:** BigQuery's `EXCEPTION WHEN ERROR THEN` block will capture and log any execution errors, mirroring the `trap` mechanism.

## 5. Transformation Logic
The migration involves mapping KSH wrapper constructs to BigQuery SQL Stored Procedure elements:

| KSH Construct (r_ausd_v_ta_c_bfc.ksh)      | BigQuery SQL Equivalent (Pseudocode)                                | Notes                                                                                                    |
|--------------------------------------------|---------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `#!/bin/ksh`                               | `CREATE OR REPLACE PROCEDURE ...`                                   | Script type declaration replaced by stored procedure definition.                                         |
| `ProgName="Name"`, `ProgVersion="V1.0.0"`  | `DECLARE ProgName STRING DEFAULT 'Name';` `DECLARE ProgVersion STRING DEFAULT 'V1.0.0';` | Environment variables become BigQuery `DECLARE` variables or procedure parameters.                         |
| `usage()` function, `cat <<EOF ... EOF`    | `IF p_h IS NOT NULL THEN SELECT ...; LEAVE; END IF;`                 | Usage message becomes a `SELECT` statement within a conditional block, causing the procedure to exit. |
| `. $HOME/.dw_init` (sourcing env)          | Replaced by procedure parameters, constants, or configuration tables. | Environment setup is externalized or explicitly defined within the procedure.                            |
| `. ${BERT_DIR_ROOT}/...f_alis_msgerr.ksh` (sourcing utils) | Replaced by dedicated BigQuery Stored Procedures or functions for logging/error handling. | Shared utility scripts become shared BigQuery routines.                                                 |
| `set -eu`                                  | `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;`                      | Strict error handling is implicitly managed by BigQuery's error model and explicit `EXCEPTION` blocks. |
| `getopts` for parameter parsing            | Stored Procedure input parameters (e.g., `IN p_h STRING`).          | Command-line arguments become explicit procedure parameters.                                           |
| `if [ ! $ErrNr -eq 0 ]`                    | `IF ErrNr != 0 THEN ... END IF;`                                    | Conditional logic translates directly to BigQuery's `IF` statements.                                    |
| `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`    | `CALL dataset.dwmsg_meldefehler(...)`, etc.                         | Custom functions are migrated to BigQuery Stored Procedures interacting with log tables.                 |
| `Name_Kernskript="..."` variable           | `DECLARE Name_Kernskript STRING DEFAULT 'dataset.k_ausd_v_ta_c_bfc';` | Variable storing core script path becomes a string variable in BQSQL, referencing the core BQ procedure. |
| `$(date +%d%m%Y)`                          | `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`                             | Shell command for date generation becomes BigQuery's `FORMAT_DATE` function.                            |
| `trap` for error handling                  | `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;`                      | Shell traps are replaced by BigQuery's robust error handling mechanisms within a `BEGIN...END` block.  |
| `print "Message" | tee -a $LogDatei`       | `INSERT INTO dataset.dw_job_log (...) VALUES (...);`                | Console output and file logging are redirected to inserts into the BigQuery log table.                  |
| `${Name_Kernskript} ... >> $LogDatei 2>&1` | `CALL dataset.k_ausd_v_ta_c_bfc(JobKennung, DW_EintragsNr);`        | Execution of a sub-script becomes a call to another BigQuery Stored Procedure.                           |
| `exit 0` / `exit $ErrNr`                   | `LEAVE;` (for early exit) or natural end of procedure.              | Procedure completes successfully or `RAISE`s an error within an `EXCEPTION` block.                       |

## 6. External Dependencies
The original script exhibits the following external dependencies:

- **Environment Initialization File (`$HOME/.dw_init`):** This file configures the shell environment.
  - **Replacement in BigQuery:** These settings will be transformed into BigQuery procedure parameters, constants, or retrieved from a dedicated BigQuery configuration table.
- **Utility KSH Scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These provide shared functions for error handling, parameter management, and date utilities.
  - **Replacement in BigQuery:** These will be reimplemented as separate BigQuery Stored Procedures or user-defined functions (UDFs), or their logic will be directly embedded into the main procedure if trivial. The logging-related functions (like `DWMSG_...`) will interact with BigQuery log tables.
- **Core Processing Script (`k_ausd_v_ta_c_bfc.ksh`):** This script is responsible for the actual data manipulation on `ta_c_bfc`.
  - **Replacement in BigQuery:** This will be migrated to a dedicated BigQuery Stored Procedure (`dataset.k_ausd_v_ta_c_bfc`) or a series of SQL statements/views that update the target table. Its migration is critical and should be handled separately, providing the core transformation logic.
- **Filesystem for Logging:** The script writes logs to files.
  - **Replacement in BigQuery:** Logging will be done by inserting records into a BigQuery audit/log table (`dataset.dw_job_log`).

There were no explicit `external_systems` (like Oracle, SFTP, S3) identified in the initial lineage analysis for this specific job, so no direct replacements for these are needed for *this wrapper script*. Any such dependencies would originate from the core script (`k_ausd_v_ta_c_bfc.ksh`).

## 7. Unresolved / Risks
- **Core Script `k_ausd_v_ta_c_bfc.ksh`:** The actual data transformation logic resides in the called script. Its content, complexity, and dependencies are currently unknown and represent the largest unresolved item. A separate analysis and migration design for this core script are essential.
- **Detailed `DWMSG_` Implementation:** The exact implementation details of the `DWMSG_` functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`) are not fully known from the wrapper script. Their translation to BigQuery procedures needs to match their original functionality precisely for consistent logging and error reporting. This may require analyzing the source code of these utility scripts.
- **Parameter `-s` and `-l` usage:** The original script accepts `-s` and `-l` but doesn't explicitly use them. While the pseudocode includes them, their intended purpose and whether `k_ausd_v_ta_c_bfc.ksh` uses them should be clarified during the core script's analysis.
- **Lack of `file_analysis` data:** The absence of analysis data for this file (`r_ausd_v_ta_c_bfc.ksh`) made it difficult to determine its automation bucket and complexity tier automatically. Manual assessment was performed.

## 8. Build Plan
The build plan focuses on creating the BigQuery components.

1. **Create Logging/Audit Table:**
   - **Language:** BigQuery SQL (DDL)
   - **File:** `bq_ddl/dw_job_log.sql`
   - **Content:**
     ```sql
     CREATE TABLE IF NOT EXISTS `dataset.dw_job_log` (
       job_kennung STRING,
       eintrags_nr INT64,
       log_level STRING,
       err_nr INT64,
       err_arg STRING,
       log_text STRING,
       stichtag STRING,
       created_at TIMESTAMP
     );
     ```

2. **Create Utility Stored Procedures/Functions (DWMSG_ equivalents):**
   - **Language:** BigQuery SQL (Stored Procedures)
   - **Files:**
     - `bq_sprocs/dwmsg_ermittle_nr.sql` (to generate unique job entry numbers, likely from a sequence or metadata table)
     - `bq_sprocs/dwmsg_logdateiname.sql` (to determine log names, potentially can be merged into main procedure if logic is simple)
     - `bq_sprocs/dwmsg_setze_status_ok.sql` (to update job status in `dw_job_log`)
   - **Content:** (Placeholders, actual logic from original `DWMSG_` scripts needed)
     ```sql
     -- Example for dwmsg_ermittle_nr
     CREATE OR REPLACE PROCEDURE `dataset.dwmsg_ermittle_nr`(OUT p_eintrags_nr INT64)
     BEGIN
       -- Logic to generate or fetch a unique entry number
       SET p_eintrags_nr = (SELECT COALESCE(MAX(eintrags_nr), 0) + 1 FROM `dataset.dw_job_log`);
     END;
     ```

3. **Develop Core Processing Stored Procedure:**
   - **Language:** BigQuery SQL (Stored Procedure)
   - **File:** `bq_sprocs/k_ausd_v_ta_c_bfc.sql`
   - **Content:** (To be developed based on the analysis of the original `k_ausd_v_ta_c_bfc.ksh` script, which is currently a risk/unresolved item). This procedure will contain the logic to update the `ta_c_bfc` table.

4. **Create Main Orchestration Stored Procedure:**
   - **Language:** BigQuery SQL (Stored Procedure)
   - **File:** `bq_sprocs/bert_v_ta_c_bfc.sql`
   - **Content:** (Based on the provided BigQuery SQL Pseudocode from CM MCP tool)
     ```sql
     -- See full pseudocode in MCP output above. This will be the content.
     CREATE OR REPLACE PROCEDURE `dataset.BERT_V_TA_C_BFC`(
       IN p_h STRING,
       IN p_s STRING,
       IN p_l STRING
     )
     BEGIN
       DECLARE ProgName STRING DEFAULT 'Bindefristcache';
       DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
       DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_C_BFC';
       DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
       DECLARE ErrNr INT64 DEFAULT 0;
       DECLARE ErrArg STRING DEFAULT '';
       DECLARE DW_EintragsNr INT64 DEFAULT 0;
       DECLARE LogDatei STRING DEFAULT '';
       DECLARE Name_Kernskript STRING DEFAULT 'dataset.k_ausd_v_ta_c_bfc';
       DECLARE v_status STRING DEFAULT 'INIT';

       IF p_h IS NOT NULL AND p_h = 'h' THEN
         SELECT
           ProgName AS Programm,
           ProgVersion AS Version,
           'Aufruf: Parameter' AS Aufruf,
           '-h zeigt diese Seite an' AS Hilfe;
         LEAVE;
       END IF;

       IF p_s IS NULL OR p_l IS NULL THEN
         SET ErrNr = 193;
         SET ErrArg = IF(p_s IS NULL, 's', 'l');
       END IF;

       IF ErrNr != 0 THEN
         INSERT INTO `dataset.dw_job_log`
         (job_kennung, eintrags_nr, log_level, err_nr, err_arg, log_text, created_at)
         VALUES
         (JobKennung, DW_EintragsNr, 'E', ErrNr, ErrArg, 'Parameterfehler', CURRENT_TIMESTAMP());

         SELECT 'usage' AS action, ProgName AS programm, ProgVersion AS version;
         LEAVE;
       END IF;

       BEGIN
         CALL `dataset.dwmsg_ermittle_nr`(DW_EintragsNr);
         CALL `dataset.dwmsg_logdateiname`(LogDatei, JobKennung, DW_EintragsNr); -- Assuming this returns/sets LogDatei

         INSERT INTO `dataset.dw_job_log`
         (job_kennung, eintrags_nr, log_level, log_text, created_at)
         VALUES
         (JobKennung, DW_EintragsNr, 'I', CONCAT('Jobstart: ', CURRENT_USER()), CURRENT_TIMESTAMP());

         INSERT INTO `dataset.dw_job_log`
         (job_kennung, eintrags_nr, log_level, log_text, stichtag, created_at)
         VALUES
         (JobKennung, DW_EintragsNr, 'I', 'SetzeStichtagInfo', v_sysdate, CURRENT_TIMESTAMP());

         SELECT
           ' ----------------- Job -----------------------' AS line
         UNION ALL SELECT CONCAT(' Job-Nr    : \'', CAST(DW_EintragsNr AS STRING), '\'')
         UNION ALL SELECT CONCAT(' JobKennung: \'', JobKennung, '\'')
         UNION ALL SELECT CONCAT(' Logdatei  : \'', LogDatei, '\'')
         UNION ALL SELECT ' ---------------------------------------------';

         CALL `dataset.k_ausd_v_ta_c_bfc`(JobKennung, DW_EintragsNr); -- Call to the migrated core script

         INSERT INTO `dataset.dw_job_log`
         (job_kennung, eintrags_nr, log_level, log_text, created_at)
         VALUES
         (JobKennung, DW_EintragsNr, 'I', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

         CALL `dataset.dwmsg_setze_status_ok`(DW_EintragsNr);

         SET v_status = 'OK';
       EXCEPTION WHEN ERROR THEN
         INSERT INTO `dataset.dw_job_log`
         (job_kennung, eintrags_nr, log_level, log_text, created_at)
         VALUES
         (JobKennung, DW_EintragsNr, 'E', 'AppError: Abbruch', CURRENT_TIMESTAMP());

         SET v_status = 'ERROR';
         RAISE USING MESSAGE = CONCAT('Execution failed for job ', JobKennung, ': ', @@error.message);
       END;

       SELECT v_status AS job_status;
     END;
     ```

5. **Orchestration Configuration:**
   - **Language:** YAML (for Cloud Composer/Airflow DAG) or other scheduler configuration.
   - **File:** `orchestration/r_ausd_v_ta_c_bfc_dag.py` (if Airflow)
   - **Content:** An Airflow DAG that schedules and invokes the `dataset.BERT_V_TA_C_BFC` BigQuery Stored Procedure, passing required parameters.

This build plan is dependent on the separate analysis and migration of the core script `k_ausd_v_ta_c_bfc.ksh` and the `DWMSG_` utility scripts.