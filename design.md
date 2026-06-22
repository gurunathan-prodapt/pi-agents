# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh

## 1. Purpose & Scope
This job, `r_ausd_v_ta_p_discount_rr.ksh`, is a KornShell wrapper script designed for the `ta_p_discount_rr` data reconciliation process. Its primary purpose is orchestration: it handles environment setup, parses command-line parameters, establishes error trapping, manages logging, and invokes a core processing script (`k_ausd_v_ta_p_discount_rr.ksh`). The job's overall scope is to facilitate the execution and monitoring of the contract data reconciliation against the `ta_p_discount_rr` table.

## 2. Source Inventory
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** B2: Semi-automated (Re-platforming to BigQuery/Airflow)

## 3. Target Architecture
The target platform for this job is Google Cloud Platform, specifically:
*   **BigQuery:** For data storage, transformation logic (BigQuery SQL stored procedures for the wrapper logic and core data reconciliation).
*   **Cloud Composer (Airflow):** For job orchestration, scheduling, and overall workflow management, replacing the shell script's role as an orchestrator.

The shell script's wrapper functionality will be translated into a BigQuery Stored Procedure, which will then call the migrated core reconciliation logic. Audit and log tables will be implemented within BigQuery to replace the file-based logging.

## 4. Data Flow & Lineage
The `r_ausd_v_ta_p_discount_rr.ksh` script acts as an orchestrator.
1.  It begins by sourcing several utility shell scripts for environment initialization, error handling, parameter management, and date functions (`$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`).
2.  It parses command-line parameters (e.g., `-s`, `-l`).
3.  It sets up error traps (`INT`, `ERR`) to ensure robust error handling and logging.
4.  It generates job metadata, including a unique entry number and log file name.
5.  **Crucially, it invokes another KornShell script, `k_ausd_v_ta_p_discount_rr.ksh`**, passing job-specific parameters. This `k_ausd_v_ta_p_discount_rr.ksh` is identified as the "core script" where the actual reconciliation logic resides.
6.  The output of the core script is redirected to a designated log file.
7.  Upon completion of the core script, the wrapper updates the job status and exits.
8.  The overall process eventually impacts the `ta_p_discount_rr` table (identified as an output target).

**Simplified Flow:**
`r_ausd_v_ta_p_discount_rr.ksh` (Wrapper Script)
  ↳ Sources: `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`
  ↳ Invokes: `k_ausd_v_ta_p_discount_rr.ksh` (Core Reconciliation Script)
    ↳ Writes to: `ta_p_discount_rr` (Table)
  ↳ Produces: Log files, updates job status.

## 5. Transformation Logic
The `r_ausd_v_ta_p_discount_rr.ksh` script's logic can be directly translated to a BigQuery Stored Procedure.

**Key logic components and their BigQuery equivalents:**
*   **Environment Setup:** Replaced by setting session variables or configuration values within the BigQuery Stored Procedure, or by managing environment variables within the Cloud Composer environment that triggers the BigQuery job.
*   **Parameter Parsing:** The `getopts` logic for parameters `-s` and `-l` will be converted to stored procedure input arguments.
*   **Job Metadata and Logging:** Functions like `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK` will be replaced by `INSERT` and `UPDATE` statements to a dedicated BigQuery audit/log table.
*   **Error Handling (`trap`):** Shell traps are not directly supported in BigQuery SQL. Equivalent error handling will be implemented using BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks within the stored procedure.
*   **Core Script Invocation:** The call to `k_ausd_v_ta_p_discount_rr.ksh` will be replaced by a `CALL` statement to a separate BigQuery Stored Procedure that encapsulates the migrated logic of the core script.

**BigQuery Stored Procedure Pseudocode for the Wrapper:**
The BigQuery Stored Procedure would define input parameters corresponding to the shell script's arguments. It would use `DECLARE` statements for variables like `job_kennung`, `sysdate`, `eintragsnr`, and `log_file`. Parameter validation would use `IF` conditions. Audit logging would involve `INSERT` statements into a `job_audit_log` table. The core processing would be a `CALL` to another stored procedure (`core_discount_rr_process`). Error handling would be managed by `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, updating the audit log upon failure and raising an error.

## 6. External Dependencies
*   **Source Shell Environment:** The execution relies on a KornShell environment and standard Unix utilities (`date`, `tee`, `print`, `getopts`). These will be replaced by the BigQuery execution environment and Cloud Composer (Airflow) capabilities.
*   **Sourced Utility Scripts:** The functions provided by `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` (e.g., logging, error reporting, parameter validation, date utilities) will be re-implemented as part of the BigQuery Stored Procedure's logic or as helper functions/views/UDFs within BigQuery.
*   **Core Reconciliation Script (`k_ausd_v_ta_p_discount_rr.ksh`):** This is the primary internal dependency. Its contents are unknown for this specific design and require separate analysis and migration to BigQuery SQL.
*   **External Systems:** No external systems (like Oracle, SFTP, S3) are directly referenced in this wrapper script according to the job assembly. The data source for `ta_p_discount_rr` is implicitly handled by the core reconciliation script.

## 7. Unresolved / Risks
*   **Core Logic Unknown:** The most significant unresolved item is the actual data reconciliation logic within `k_ausd_v_ta_p_discount_rr.ksh`. Its complexity and specific operations will dictate the strategy for its BigQuery migration (e.g., pure SQL, UDFs, Python scripts).
*   **Parameter Usage:** The `s` and `l` parameters are parsed but not used within the wrapper script. Their intended use should be clarified, as they might be passed to the core script or used in ways not visible in the provided wrapper code.
*   **Environment Variables:** The resolution of `BERT_DIR_ROOT` and other environment variables set by `.dw_init` needs to be mapped to a BigQuery-compatible configuration mechanism.
*   **Timestamp Format:** The `v_sysdate=$(date +%d%m%Y)` format needs to be considered when migrating date handling.
*   **Error Numbering:** The meaning of `ErrNr=193` and `ErrNr=192` should be documented or mapped to specific BigQuery error codes or custom error handling conventions.
*   **`tee -a $LogDatei` behavior:** Replicating the exact concurrent logging behavior of `tee -a` with BigQuery audit tables should be carefully considered, especially in a distributed environment.

## 8. Build Plan
1.  **Define BigQuery Audit Log Table:** Create the DDL for a BigQuery table to store job execution metadata, status, errors, and logging information, replacing the shell script's log file output and `DWMSG_*` functions.
2.  **Migrate Wrapper Logic to BigQuery Stored Procedure:** Develop a BigQuery Stored Procedure (e.g., `project.dataset.vertragsdatenabgleich`) that replicates the parameter parsing, environment setup (via configurations/session variables), audit logging, and error handling of `r_ausd_v_ta_p_discount_rr.ksh`.
3.  **Analyze and Migrate Core Reconciliation Logic:** Conduct a separate analysis of `k_ausd_v_ta_p_discount_rr.ksh`.
    *   **Convert to BigQuery Stored Procedure:** Translate the core reconciliation logic into BigQuery SQL, creating a dedicated stored procedure.
    *   **Identify Python UDFs (if necessary):** If the core script contains complex, row-level transformations not efficiently handled by SQL, identify and convert these into BigQuery Python UDFs.
4.  **Integrate Core Logic Call:** Modify the wrapper BigQuery Stored Procedure to call the newly created BigQuery Stored Procedure for the core reconciliation logic.
5.  **Develop Cloud Composer (Airflow) DAG:** Create an Airflow DAG in Python to schedule and orchestrate the execution of the BigQuery Stored Procedure. This DAG will handle parameter passing and overall workflow management in the target GCP environment.