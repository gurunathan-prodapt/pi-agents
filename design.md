# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh

## 1. Purpose & Scope
This document outlines the migration strategy for the `k_ausd_bp_ta_rn_vertrag.ksh` job, which is responsible for orchestrating a data transformation process. The job involves parameter validation, date checking, execution of an Oracle SQL script (`d_ausd_bp_ta_rn_vertrag.sql`), and recording the number of processed records. The primary goal is to migrate this job to Google Cloud's BigQuery platform, preserving its existing functionality and data flow.

The KornShell script acts as a control script, handling environment setup, argument parsing, error handling, and the sequential execution of a SQL script. The SQL script performs a crucial data aggregation and reshaping task, truncating a target table (`SOF$TA_RN_VERTRAG`) and populating it with data from a source table (`SOF$TA_RN_EINZELN`) based on contract ID.

## 2. Source Inventory

### **File: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh**
- **Technology:** KornShell
- **Purpose:** ETL orchestration, parameter validation, script execution control.
- **Complexity Tier:** Medium
- **Automation Bucket:** Semi-Auto
- **Summary:** This script acts as a control script for a data processing job. It handles parameter parsing, validates input dates, sources utility scripts for error handling and SQL*Plus execution, and orchestrates the execution of a SQL script (`d_ausd_bp_ta_rn_vertrag.sql`). It also reads a temporary file for record counts after the SQL execution.

### **File: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_rn_vertrag.sql**
- **Technology:** Oracle SQL
- **Purpose:** Data transformation and loading.
- **Complexity Tier:** Medium
- **Automation Bucket:** Semi-Auto
- **Summary:** This SQL script truncates the `SOF$TA_RN_VERTRAG` table and then populates it by aggregating and reshaping data from the `SOF$TA_RN_EINZELN` table, using `MAX` functions and grouping by contract ID. It also defines a date variable from `DWTK_MELDUNGEN`.

## 3. Target Architecture
The migrated job will leverage BigQuery's capabilities for data storage and processing.

- **Orchestration:** The KornShell logic will be migrated to a BigQuery Stored Procedure, which will encapsulate parameter validation, date calculations, and the execution of the main data transformation logic. Alternatively, a Cloud Composer (Airflow) DAG could be used for more complex orchestration needs, but a BigQuery Stored Procedure is suitable for this case.
- **Data Storage:**
    - Source tables like `SOF$TA_RN_EINZELN` and `DWTK_MELDUNGEN` will be ingested into BigQuery as standard tables (e.g., `project.dataset.sof_ta_rn_einzeln`, `project.dataset.dwtk_meldungen`).
    - The target table `SOF$TA_RN_VERTRAG` will also be a BigQuery table (e.g., `project.dataset.sof_ta_rn_vertrag`).
    - Temporary files and intermediate data will be handled within BigQuery using temporary tables or BigQuery scripting variables.
- **Data Transformation:** The Oracle SQL transformation logic will be directly translated into BigQuery Standard SQL, executed within the BigQuery Stored Procedure.
- **Logging and Monitoring:** Standard BigQuery logging and Cloud Monitoring will replace the custom shell-based logging (`DWMSG_MeldeFehler`) and temporary file record counts. A dedicated BigQuery logging table (e.g., `project.dataset.job_log`) will store job execution details.

## 4. Data Flow & Lineage

The original job involves the following flow:
1. **`r_ausd_bp_ta_rn_vertrag.ksh` (wrapper script)** `INVOKES` **`k_ausd_bp_ta_rn_vertrag.ksh` (main script)**
2. **`k_ausd_bp_ta_rn_vertrag.ksh`:**
    - Sourcing utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    - Parameter parsing and validation (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
    - Date derivation from `gestern.ksh`.
    - `EXECUTES_SQL` **`d_ausd_bp_ta_rn_vertrag.sql`** via `starteSQLSkript` function.
    - Reads record count from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_vertrag.tmp`).
    - (Commented out: `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`)
3. **`d_ausd_bp_ta_rn_vertrag.sql`:**
    - `READS` from `isbert_schema.dwtk_meldungen` to derive `v_datum`.
    - `TRUNCATES` `sof$ta_rn_vertrag`.
    - `READS` from `sof$ta_rn_einzeln`.
    - `WRITES` to `sof$ta_rn_vertrag`.

**Migrated Data Flow:**
1. A scheduling mechanism (e.g., Cloud Scheduler, Cloud Composer) triggers the BigQuery Stored Procedure.
2. **BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_rn_vertrag`:**
    - Receives parameters (e.g., `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
    - Performs parameter validation.
    - Calculates `v_datum_heute` and `v_datum_gestern` using BigQuery date functions.
    - Executes the BigQuery Standard SQL equivalent of `d_ausd_bp_ta_rn_vertrag.sql`.
    - Captures the row count from the `INSERT` statement or a subsequent `COUNT(*)` query into a BigQuery variable.
    - Logs execution details to a BigQuery logging table.

## 5. Transformation Logic

### `k_ausd_bp_ta_rn_vertrag.ksh` (KornShell to BigQuery Stored Procedure)
- **Parameter Parsing:** `getopts` will be replaced by direct input parameters to the BigQuery Stored Procedure.
- **Parameter Validation:** Shell `if [ ! $ErrNr -eq 0 ]` logic will be translated to BigQuery `IF` statements and `RAISE USING MESSAGE` for error handling.
- **Date Check:** `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'` will be replaced by `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` and null checks in BigQuery SQL.
- **Environment Sourcing:** `. $HOME/.dw_init` and other sourced utility scripts will be replaced by:
    - Direct BigQuery SQL functions for date manipulation (e.g., `CURRENT_DATE()`, `DATE_SUB`).
    - BigQuery Stored Procedure variables for configuration or context.
    - Logging into a BigQuery table instead of custom shell error reporting.
- **SQL Script Execution:** The `starteSQLSkript` function will be replaced by direct execution of the migrated BigQuery SQL within the stored procedure.
- **Temporary File for Record Count:** Reading from `$tmpFile` will be replaced by assigning the result of a `COUNT(*)` operation to a BigQuery variable.
- **Commented-out Code:** The commented job management functions (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`) and file-based post-processing (`sed`, `sort`, `join`) are not part of the active migration scope but would be reimplemented using BigQuery DDL/DML and SQL transformations if activated.

### `d_ausd_bp_ta_rn_vertrag.sql` (Oracle SQL to BigQuery Standard SQL)
- **Variable Definition:** `DEFINE v_carmen = "@pcrs1"` and `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';` to determine `v_datum` will be handled by BigQuery variables. `v_datum` can be derived using `COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')` within a `DECLARE` statement.
- **`TRUNCATE TABLE`:** The `TRUNCATE TABLE sof$ta_rn_vertrag REUSE STORAGE` command will translate directly to `TRUNCATE TABLE \`project.dataset.sof_ta_rn_vertrag\`;`
- **`INSERT` with `MAX()` Aggregation:** The main `INSERT INTO ... SELECT MAX(...) FROM ... GROUP BY ...` statement is directly translatable to BigQuery Standard SQL. The `/*+ full(rp) parallel(rp,4) */` Oracle hints will be removed as BigQuery's execution engine handles parallelism automatically.
    ```sql
    TRUNCATE TABLE `project.dataset.sof_ta_rn_vertrag`;

    INSERT INTO `project.dataset.sof_ta_rn_vertrag` (...)
    SELECT
      cntrct_id,
      MAX(TN_multi_single) AS TN_multi_single,
      -- ... (all other MAX aggregations)
      MAX(MS_RN_2_valid_to) AS MS_RN_2_valid_to
    FROM `project.dataset.sof_ta_rn_einzeln`
    GROUP BY cntrct_id;
    ```
- **`COMMIT;`:** Explicit `COMMIT` statements are not needed in BigQuery's transactional model for DML operations.
- **`start ../trace.sql.cfg` and `spool`:** These Oracle-specific tracing and spooling commands will be replaced by BigQuery's built-in logging and potentially by external logging mechanisms like Cloud Logging if detailed execution traces are required.

## 6. External Dependencies
The current job has the following external dependencies and their proposed replacements:

- **Oracle Database:** The primary data source and target for the SQL script is an Oracle database.
    - **Replacement:** All relevant Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_rn_einzeln`, `sof$ta_rn_vertrag`) will be migrated to BigQuery. Data ingestion can be done via various methods:
        - **Batch:** Dataflow, Datastream, or BigQuery Data Transfer Service for initial load and ongoing replication.
        - **Real-time:** Change Data Capture (CDC) solutions into BigQuery.
- **File System Utilities:** The KornShell script uses various shell utilities and external scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`, temporary files).
    - **Replacement:**
        - Environment initialization (`.dw_init`) will be handled by BigQuery Stored Procedure parameters or runtime configuration.
        - Error handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`) will use BigQuery's `RAISE` and Cloud Logging.
        - Date functions (`h_alis_date.ksh`, `gestern.ksh`) will be replaced by BigQuery's native date and time functions (`CURRENT_DATE()`, `DATE_SUB`, `FORMAT_DATE`, `SAFE.PARSE_DATE`).
        - Parameter parsing (`h_alis_parameter.ksh`) will be handled by BigQuery Stored Procedure parameters.
        - SQL*Plus execution (`h_alis_sqlplus.ksh`, `starteSQLSkript`) will be replaced by direct BigQuery SQL execution.
        - Temporary files for record counts will be replaced by BigQuery variables.
- **FOS Job Management (commented out):** `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`.
    - **Replacement:** If these become active requirements, they would need to be re-evaluated and potentially implemented using Cloud Functions, Cloud Workflows, or Cloud Composer to interact with BigQuery metadata or other job management systems.

## 7. Unresolved / Risks
- **`r_ausd_bp_ta_rn_vertrag.ksh` (wrapper script):** The lineage indicates `r_ausd_bp_ta_rn_vertrag.ksh` invokes `k_ausd_bp_ta_rn_vertrag.ksh`. This suggests a potential higher-level orchestration not fully covered by the current job's scope. The migration of this wrapper script should also be considered to maintain the complete job flow.
- **Oracle-specific Functions/Packages in SQL:** While the provided SQL is standard DML, complex Oracle functions, built-in packages (e.g., `DWPA_UTIL_SKRIPT.runstatement`), or custom stored procedures not directly translatable to BigQuery SQL would require manual rewrite or alternative implementations (e.g., UDFs, Python external functions in BigQuery). The `runstatement` call within the SQL script might need careful handling, potentially becoming a direct `TRUNCATE TABLE` DDL statement in BigQuery.
- **Data Type Mismatches:** Implicit data type conversions in Oracle might behave differently in BigQuery. Careful schema mapping and explicit casting in BigQuery SQL may be necessary, especially for columns involved in `MAX()` aggregations and date fields.
- **Performance Tuning:** The `/*+ full(rp) parallel(rp,4) */` Oracle hints suggest performance considerations. While BigQuery handles parallelism automatically, query optimization and partitioning strategies will need to be applied to the BigQuery tables to ensure optimal performance.

## 8. Build Plan

The migration will follow these steps:

1.  **Data Ingestion:**
    *   Migrate source Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_rn_einzeln`) and the target table (`sof$ta_rn_vertrag`) to BigQuery. Ensure appropriate data types, partitioning, and clustering for performance.
    *   **Language:** SQL (for DDL), Dataflow/Datastream/BQ Transfer Service (for ingestion).
2.  **SQL Script Migration:**
    *   Translate `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_rn_vertrag.sql` into BigQuery Standard SQL. This includes converting variable definitions, `TRUNCATE TABLE`, and the `INSERT ... SELECT MAX(...) GROUP BY` statement.
    *   **Language:** BigQuery Standard SQL.
3.  **KornShell Script Migration (Orchestration):**
    *   Create a BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_rn_vertrag` that encapsulates the logic from `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh`.
    *   Implement parameter validation, date calculations, and error handling within the stored procedure using BigQuery scripting.
    *   Integrate the migrated BigQuery SQL from step 2 into this stored procedure.
    *   Implement logging to a `job_log` BigQuery table.
    *   **Language:** BigQuery Standard SQL (for Stored Procedure).
4.  **Scheduling:**
    *   Set up a Cloud Scheduler job to trigger the BigQuery Stored Procedure at the required frequency. For more complex dependencies or integration with other workflows, consider using Cloud Composer (Airflow).
    *   **Language:** YAML/JSON (for Cloud Scheduler) or Python (for Cloud Composer DAG).
5.  **Testing and Validation:**
    *   Perform thorough unit and integration testing to ensure the migrated job produces identical results to the legacy system.
    *   **Language:** SQL, Python.
6.  **Decommissioning:**
    *   Once validated, decommission the legacy KornShell and Oracle SQL scripts.
    *   **Language:** N/A.