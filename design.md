# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh

## 1. Purpose & Scope
This job, `r_drop_temp_table.ksh`, serves as a wrapper script in the legacy environment. Its primary purpose is to orchestrate the deletion of temporary intermediate tables that were not properly dropped by a downstream BERT process. It handles parameter parsing, environment initialization, logging, and error handling, before invoking a core cleanup script, `${BERT_DIR_ROOT}/aufbereitung/bin/k_drop_temp_table.ksh`. The script ensures proper operational logging and status reporting.

The scope of this migration covers the conversion of this KornShell script into a BigQuery-native solution, aiming for full automation within the Google Cloud Platform ecosystem.

## 2. Source Inventory
The job consists of a single KornShell script:
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh`
- **Technology:** KornShell (KSH)
- **Complexity Tier:** Not available (no rows in `file_complexity` table for this file)
- **Automation Bucket:** Semi-Auto (B2)

The script itself is a wrapper, delegating the actual table dropping logic to another script (`k_drop_temp_table.ksh`) which is assumed to contain SQL-compatible DDL/DML.

## 3. Target Architecture
The target architecture in BigQuery will involve:
- A BigQuery Stored Procedure (`project.dataset.k_drop_temp_table_wrapper`) to replace the `r_drop_temp_table.ksh` wrapper script.
- A BigQuery Stored Procedure (`project.dataset.k_drop_temp_table_core`) to encapsulate the core temporary table dropping logic, replacing `k_drop_temp_table.ksh`.
- Dedicated BigQuery tables for job auditing (`project.dataset.job_audit_log`) and status (`project.dataset.job_status_log`), replacing file-based logging.
- Orchestration via a cloud-native scheduler (e.g., Cloud Composer/Airflow or Cloud Workflows) to invoke the BigQuery stored procedure.

## 4. Data Flow & Lineage
The original script's lineage indicates it is invoked by an UC4 job:
- **Source:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_DROP_TEMP_TABLE.xml` (UC4 Job Definition)
- **Edge Type:** INVOKES
- **Target:** `SCRIPT:R_DROP_TEMP_TABLE.KSH`

In BigQuery, this flow will be:
- An Airflow DAG (or similar orchestrator) replaces the UC4 job.
- The Airflow DAG will call the `project.dataset.k_drop_temp_table_wrapper` BigQuery Stored Procedure, passing required parameters.
- `k_drop_temp_table_wrapper` will handle parameter validation, default value assignment, and audit logging.
- `k_drop_temp_table_wrapper` will then `CALL` the `project.dataset.k_drop_temp_table_core` BigQuery Stored Procedure.
- `k_drop_temp_table_core` will execute the actual DDL/DML for dropping temporary tables.
- Both procedures will interact with `project.dataset.job_audit_log` and `project.dataset.job_status_log` for logging and status updates.

There are no direct data sources (READS) or targets (WRITES) identifiable within this wrapper script itself, as its function is orchestration. The data flow originates from the core cleanup script. The parameters `p_stichtag` and `p_wiederanlaufWert` act as control flow inputs.

## 5. Transformation Logic
The transformation from KornShell to BigQuery SQL Stored Procedures involves:

**`r_drop_temp_table.ksh` (Wrapper) -> `project.dataset.k_drop_temp_table_wrapper` (BigQuery Stored Procedure):**

- **Parameter Handling:**
    - KornShell `getopts` for `-s` (Stichtag) and `-l` (Wiederanlaufwert) will be replaced by `IN` parameters in the stored procedure.
    - `p_stichtag` (reference date, DDMMYYYY) and `p_wiederanlaufWert` (restart threshold) will be direct procedure arguments.
    - Defaulting logic for `p_wiederanlaufWert` to `0` and `p_stichtag` to `v_sysdate` (current date) will be implemented using `IFNULL` and `CURRENT_DATE()` / `FORMAT_DATE()`.
- **Environment Initialization:**
    - `. $HOME/.dw_init` and sourcing other utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) will be replaced by direct SQL logic within the procedure or by pre-defined BigQuery UDFs/procedures if complex reusable logic is required.
- **Error Handling:**
    - KornShell `set -e` and `trap` mechanisms will be replaced by `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks in BigQuery SQL.
    - `DWMSG_MeldeFehler` and `DWMSG_Fehlerbehandlung` will be replaced by `INSERT` statements into the `job_audit_log` table and `RAISE` statements to propagate errors.
- **Logging:**
    - File-based logging (`>> $LogDatei 2>&1`, `tee -a $LogDatei`) will be replaced by `INSERT` statements into the `project.dataset.job_audit_log` table.
    - Job metadata (`JobKennung`, `DW_EintragsNr`, `LogDatei`) will be stored as columns in the audit log table.
    - `DWMSG_SetzeStatusOK` will be replaced by an `INSERT` statement into `project.dataset.job_status_log` with status 'OK'.
- **Core Script Invocation:**
    - The line `${Name_Kernskript} -j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert} >> $LogDatei 2>&1` will be replaced by a `CALL` statement to the `project.dataset.k_drop_temp_table_core` stored procedure, passing the relevant arguments.

**`${BERT_DIR_ROOT}/aufbereitung/bin/k_drop_temp_table.ksh` (Core Script) -> `project.dataset.k_drop_temp_table_core` (BigQuery Stored Procedure):**

- The actual DDL/DML for dropping temporary tables will be directly translated into BigQuery SQL statements within this stored procedure. This may involve `DROP TABLE`, `DELETE FROM`, or `TRUNCATE TABLE` statements.
- The specific logic of `k_drop_temp_table.ksh` is not available in the provided context, but it is assumed to be SQL-centric. For example:
    ```sql
    DELETE FROM `project.dataset.temp_table`
    WHERE stichtag = p_stichtag
      AND dwh_vertrag_id >= p_wiederanlaufWert;
    ```
- This procedure will also perform audit logging via `INSERT` statements into `job_audit_log`.

## 6. External Dependencies
- **UC4 Scheduler:** The legacy job is scheduled and invoked by an UC4 job. This will be replaced by a Cloud Composer (Airflow) DAG or Cloud Workflows in Google Cloud. The DAG will be responsible for triggering the `project.dataset.k_drop_temp_table_wrapper` BigQuery stored procedure.
- **Legacy Environment Variables & Utilities:**
    - `$HOME/.dw_init`
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    These will be absorbed and re-implemented as native BigQuery SQL logic within the stored procedures or as BigQuery UDFs where applicable (e.g., date formatting, parameter validation). Generic error handling and logging will transition to BigQuery's built-in error handling and custom audit tables.
- **Core Cleanup Script (`k_drop_temp_table.ksh`):** This is an internal dependency. Its contents are assumed to be SQL-compatible and will be migrated into the `project.dataset.k_drop_temp_table_core` stored procedure. If it contains non-SQL operations (e.g., filesystem manipulations), those will need to be re-evaluated for a cloud-native Python/Cloud Run equivalent.

No external databases or systems (like Oracle, SFTP, S3) were explicitly identified as direct dependencies for this specific wrapper script.

## 7. Unresolved / Risks
- **`file_complexity` Information Missing:** The complexity tier and migration flags for `r_drop_temp_table.ksh` were not available. While the `shellscript_to_bqsql_design` tool provided a comprehensive design, a formal complexity assessment would offer additional insights into potential migration challenges.
- **Contents of `k_drop_temp_table.ksh`:** The actual DDL/DML logic of the core cleanup script (`k_drop_temp_table.ksh`) is not known. The current design assumes it's migratable to BigQuery SQL. If it contains complex non-SQL logic (e.g., OS commands, calls to other external systems), this part would represent a significant redesign effort and might require a Python-based Cloud Function or a specific Cloud Run job, orchestrated by the BigQuery wrapper. This is the main unknown and risk for the "semi-auto" classification.
- **`DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, `DWMSG_*` Functions:** These are custom shell functions. While the BigQuery pseudocode provides equivalents, the exact behavior and any subtle nuances of these functions from the original KornShell environment need to be thoroughly understood to ensure a faithful translation.
- **Parameter `p_wiederanlaufWert` handling:** The description states that if `p_wiederanlaufWert` is set, "only Vertraege zu DWH_VERTRAG_ID > Wiederanlaufwert in die FOS-Tabelle geschrieben (die Eintraege bzgl. Werten >= diesem Wert werden geloescht)". This implies a DELETE operation based on `DWH_VERTRAG_ID`. The core cleanup script must correctly implement this logic.

## 8. Build Plan
1. **Define BigQuery Dataset:** Create the `project.dataset` in BigQuery to house the new assets.
2. **Create Audit & Status Tables:**
    - `project.dataset.job_audit_log` (Schema: `job_kennung STRING`, `job_entry_nr INT64`, `log_level STRING`, `message STRING`, `stichtag STRING`, `restart_value INT64`, `created_at TIMESTAMP`)
    - `project.dataset.job_status_log` (Schema: `job_kennung STRING`, `job_entry_nr INT64`, `status STRING`, `stichtag STRING`, `created_at TIMESTAMP`)
3. **Develop `k_drop_temp_table_core` Stored Procedure:**
    - **Language:** BigQuery SQL
    - **Content:** Translate the DDL/DML from the original `k_drop_temp_table.ksh` into a BigQuery Stored Procedure named `project.dataset.k_drop_temp_table_core`. This is the most critical step and requires understanding the original core script's logic. Include logging to `job_audit_log`.
4. **Develop `k_drop_temp_table_wrapper` Stored Procedure:**
    - **Language:** BigQuery SQL
    - **Content:** Implement the wrapper logic as per the BigQuery Pseudocode provided in the design. This includes parameter parsing, defaulting, error handling with `BEGIN...EXCEPTION`, and calling `project.dataset.k_drop_temp_table_core`. Include logging to both `job_audit_log` and `job_status_log`.
5. **Develop Orchestration DAG:**
    - **Language:** Python (for Airflow/Cloud Composer)
    - **Content:** Create an Airflow DAG that triggers `project.dataset.k_drop_temp_table_wrapper` with the appropriate parameters. This will replace the UC4 scheduling.
6. **Testing:** Unit test each stored procedure and integration test the entire workflow via the Airflow DAG.