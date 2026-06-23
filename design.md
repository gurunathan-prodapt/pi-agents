# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh

## 1. Purpose & Scope
This job, initiated by the KornShell script `r_ausd_bp_ta_rn_da_vda_tk.ksh`, is responsible for the initial provisioning of selected basic product data for the BERT (Business Event Reporting Tool) system. Its core function is to extract contract cache data from the Data Warehouse (DWH) based on a specified snapshot date and prepare it for "Forderungsscoring" (claims scoring). The process involves orchestrating a control script which then executes a SQL script to populate a target table with filtered contract data.

## 2. Source Inventory
The assembled job consists of three primary source files:

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh`**
    *   **Technology:** KornShell Script
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-automatic (B2)
    *   **Purpose:** This is the top-level orchestration script. It handles command-line parameter parsing (`-s` for Stichtag/reference date, `-l` for Wiederanlaufwert/restart value), environmental setup, basic date determination, logging, and error handling. It then invokes the `k_ausd_bp_ta_rn_da_vda_tk.ksh` script with the collected parameters.
*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh`**
    *   **Technology:** KornShell Script
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-automatic (B2)
    *   **Purpose:** This is a control script invoked by the main orchestrator. It further processes parameters, performs date checks, sets up SQL*Plus routines, and crucially executes the core SQL script `d_ausd_bp_ta_rn_da_vda_tk.sql`. It also contains commented-out sections for file-based data reformatting and joining which are currently inactive.
*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_rn_da_vda_tk.sql`**
    *   **Technology:** Oracle SQL
    *   **Complexity Tier:** Complex
    *   **Automation Bucket:** Manual (B3)
    *   **Purpose:** This is the core data processing script. It determines a date variable from `isbert_schema.dwtk_meldungen`, truncates the target table `sof$ta_rn_da_vda_tk`, and then inserts data by selecting from `sof$ta_rn_einzeln`. The selection filters for rows where `DA_RN_msisdn`, `VDA_RN_msisdn`, or `TK_RN_msisdn` are not null.

## 3. Target Architecture
The migration targets Google BigQuery for data processing and storage. The current shell-script-orchestrated workflow will be transitioned to BigQuery stored procedures and potentially BigQuery Scripting.

*   **Orchestration Layer:** The KornShell scripts (`r_ausd_bp_ta_rn_da_vda_tk.ksh` and `k_ausd_bp_ta_rn_da_vda_tk.ksh`) will be refactored into a BigQuery stored procedure (e.g., `project.dataset.sp_bereitstellung_basisprodukte_bert`). This stored procedure will handle parameter validation, defaulting logic, job audit logging, and invocation of subsequent data processing procedures.
*   **Data Processing Layer:** The Oracle SQL script (`d_ausd_bp_ta_rn_da_vda_tk.sql`) will be directly converted into BigQuery SQL within a separate BigQuery stored procedure (e.g., `project.dataset.sp_ausd_bp_ta_rn_da_vda_tk`).
*   **Logging and Auditing:** The custom shell-based logging (`DWMSG_*` functions) will be replaced by inserts into dedicated BigQuery audit/log tables (e.g., `project.dataset.job_audit_log`).
*   **Data Storage:** All source tables (e.g., `isbert_schema.dwtk_meldungen`, `sof$ta_rn_einzeln`) and target tables (e.g., `sof$ta_rn_da_vda_tk`) will be migrated to BigQuery as native tables, preserving their schema and data types where appropriate.

## 4. Data Flow & Lineage
The proposed data flow in the BigQuery target architecture is as follows:

1.  **Orchestration Procedure Call:** A top-level BigQuery stored procedure, `project.dataset.sp_bereitstellung_basisprodukte_bert`, is invoked, accepting `p_stichtag` (reference date) and `p_wiederanlaufWert` (restart value) as parameters.
2.  **Parameter Handling & Audit:** `sp_bereitstellung_basisprodukte_bert` validates and defaults input parameters. It logs job start information into a `job_audit_log` table.
3.  **Core Data Processing Procedure Call:** `sp_bereitstellung_basisprodukte_bert` then calls another BigQuery stored procedure, `project.dataset.sp_ausd_bp_ta_rn_da_vda_tk`, passing necessary job parameters (job key, reference date, entry number, restart threshold).
4.  **SQL Execution:** Inside `sp_ausd_bp_ta_rn_da_vda_tk`:
    *   It first queries `project.dataset.dwtk_meldungen` (migrated from `isbert_schema.dwtk_meldungen`) to determine a `v_datum` value based on `MAX(timecreated)` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   The target table `project.dataset.sof_ta_rn_da_vda_tk` (migrated from `sof$ta_rn_da_vda_tk`) is truncated.
    *   Data is then inserted into `project.dataset.sof_ta_rn_da_vda_tk` by selecting from `project.dataset.sof_ta_rn_einzeln` (migrated from `sof$ta_rn_einzeln`). The selection criterion is `DA_RN_msisdn IS NOT NULL OR VDA_RN_msisdn IS NOT NULL OR TK_RN_msisdn IS NOT NULL`.
5.  **Audit & Completion:** Upon successful completion or error, `sp_bereitstellung_basisprodukte_bert` updates the `job_audit_log` table with the final job status and relevant messages.

**Execution Order:**
1. `project.dataset.sp_bereitstellung_basisprodukte_bert` (orchestrator)
2. `project.dataset.sp_ausd_bp_ta_rn_da_vda_tk` (core data processing)
3. Reads from `project.dataset.dwtk_meldungen` and `project.dataset.sof_ta_rn_einzeln`
4. Writes to `project.dataset.sof_ta_rn_da_vda_tk`
5. Updates `project.dataset.job_audit_log`

## 5. Transformation Logic

### Orchestration Script (`r_ausd_bp_ta_rn_da_vda_tk.ksh`) to BigQuery Stored Procedure (`sp_bereitstellung_basisprodukte_bert`):

*   **Parameter Handling:**
    *   `p_stichtag`: Will be a `STRING` parameter in the BigQuery stored procedure. Default logic (if null/empty, use current system date) will be implemented using `IFNULL(NULLIF(p_stichtag, ''), v_sysdate)`.
    *   `p_wiederanlaufWert`: Will be a `STRING` parameter. Default logic (if null/empty, use '0') will be `IFNULL(NULLIF(p_wiederanlaufWert, ''), '0')`.
*   **Date Determination:** `DWDate_Gib_Zeitraum` will be replaced by `CURRENT_DATE()` and `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` in BigQuery SQL.
*   **Parameter Validation:** `pruefeParameterGesetzt` will be replaced by explicit `IF ... THEN SIGNAL` statements using BigQuery Scripting error handling.
*   **Logging:** The `DWMSG_*` functions will be replaced by `INSERT` statements into a BigQuery audit table (`job_audit_log`) at various stages (start, success, error).
*   **Error Handling:** Shell `trap` mechanisms will be replaced by BigQuery Scripting `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks for robust error capture and logging.
*   **Sub-script Invocation:** The call to `k_ausd_bp_ta_rn_da_vda_tk.ksh` will be replaced by a `CALL` statement to the `sp_ausd_bp_ta_rn_da_vda_tk` BigQuery stored procedure.

### Core Script (`k_ausd_bp_ta_rn_da_vda_tk.ksh`) Logic to BigQuery Stored Procedure (`sp_ausd_bp_ta_rn_da_vda_tk`):

*   The environmental setup, parameter parsing, and utility script sourcing within `k_ausd_bp_ta_rn_da_vda_tk.ksh` will be absorbed into the overarching BigQuery stored procedure design.
*   The `starteSQLSkript` function call, which executes the Oracle SQL, will be replaced by the direct execution of the BigQuery SQL equivalent within the `sp_ausd_bp_ta_rn_da_vda_tk` procedure.
*   Commented-out file processing and `join` commands (`sed`, `sort`, `join`) indicate potential for future expansion. If these features become active, they would need migration to BigQuery SQL, Python (e.g., Dataflow), or other appropriate BigQuery ecosystem tools, interacting with Cloud Storage for file-like operations.

### SQL Script (`d_ausd_bp_ta_rn_da_vda_tk.sql`) to BigQuery SQL within `sp_ausd_bp_ta_rn_da_vda_tk`:

*   **Variable Definition:**
    *   `DEFINE v_carmen = "@pcrs1"` is an Oracle SQL*Plus specific definition and will be removed if not directly used in the query. If it represents a connection string or schema, it needs to be mapped to appropriate BigQuery project/dataset references.
    *   `COLUMN s_datum new_value v_datum noprint` and the subsequent `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';` will be converted to a `DECLARE v_datum STRING DEFAULT (...)` statement using `COALESCE` for `NVL` and `FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated)))` for the date formatting.
*   **Settings:** Oracle SQL*Plus specific commands (`start`, `spool`, `WHENEVER SQLERROR CONTINUE/EXIT FAILURE`, `set timing on`) will be removed. Error handling will be managed by BigQuery Scripting.
*   **Truncate Table:** The Oracle PL/SQL block `begin isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_rn_da_vda_tk REUSE STORAGE'); end; /` will be replaced by `TRUNCATE TABLE project.dataset.sof_ta_rn_da_vda_tk;`.
*   **Insert Statement:**
    *   The `INSERT INTO ... SELECT` statement will be directly translated to BigQuery SQL.
    *   Table names like `sof$ta_rn_da_vda_tk` and `sof$ta_rn_einzeln` will be mapped to BigQuery equivalents, e.g., `project.dataset.sof_ta_rn_da_vda_tk`.
    *   The Oracle hint `/*+ full(rp) parallel(rp,4) */` will be removed as BigQuery's execution engine handles parallelism automatically.
    *   The `WHERE` clause `DA_RN_msisdn is not null or VDA_RN_msisdn is not null or TK_RN_msisdn is not null` remains logically the same.
    *   `COMMIT;` is implicitly handled by BigQuery's auto-commit behavior for DML statements.

## 6. External Dependencies
The current job primarily interacts with Oracle database tables.

*   **Oracle Database:**
    *   `isbert_schema.dwtk_meldungen`: This table is read to determine a date variable. In BigQuery, this will be a migrated BigQuery table, e.g., `project.dataset.dwtk_meldungen`.
    *   `sof$ta_rn_einzeln`: This table is read as the source for the main data selection. In BigQuery, this will be a migrated BigQuery table, e.g., `project.dataset.sof_ta_rn_einzeln`.
    *   `sof$ta_rn_da_vda_tk`: This is the target table that is truncated and reloaded. In BigQuery, this will be a migrated BigQuery table, e.g., `project.dataset.sof_ta_rn_da_vda_tk`.
    *   `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`: This is an Oracle procedural utility. Its specific function (truncating a table) will be replaced by direct BigQuery `TRUNCATE TABLE` DDL. Any other functionality of this utility would need individual assessment and migration.
*   **Shell Utilities:** The KornShell scripts source various utility scripts like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`. These functions (error messaging, parameter handling, date utilities) will be replaced by native BigQuery Scripting constructs or custom BigQuery SQL functions/procedures as identified in the transformation logic.

## 7. Unresolved / Risks
*   **Case Sensitivity:** Oracle's default case insensitivity for identifiers might need careful handling during BigQuery migration, especially if tables/columns were created with mixed case and later referenced without quotes. BigQuery is generally case-sensitive for table/column names unless specifically quoted.
*   **Oracle `REUSE STORAGE`:** The `REUSE STORAGE` clause in Oracle's `TRUNCATE TABLE` is for performance optimization of storage allocation. BigQuery's storage management is automatic, so this specific optimization concept does not directly apply and will be omitted.
*   **Parameterized SQL Execution:** The `starteSQLSkript` in `k_ausd_bp_ta_rn_da_vda_tk.ksh` indicates dynamic SQL execution with parameters. This needs to be carefully mapped to BigQuery stored procedure parameters to prevent SQL injection vulnerabilities and ensure type safety.
*   **Commented-out File Processing:** The commented `sed`, `sort`, `join` commands in `k_ausd_bp_ta_rn_da_vda_tk.ksh` suggest historical or potential future file-based processing. If these are reactivated, they would introduce a dependency on external files that would need to be handled via Cloud Storage and potentially Dataflow or other data processing services.
*   **Oracle `trace.sql.cfg` and `spool`:** These are Oracle-specific tracing and output mechanisms that will be removed. BigQuery provides its own logging and monitoring capabilities.

## 8. Build Plan
The migration will be executed in a structured approach, prioritizing the creation of BigQuery equivalents for the data objects and then the procedural logic.

1.  **BigQuery Table Creation (DDL):**
    *   Create BigQuery tables for `dwtk_meldungen`, `sof_ta_rn_einzeln`, and `sof_ta_rn_da_vda_tk` within the designated BigQuery project and dataset (e.g., `project.dataset`). Ensure correct data type mapping from Oracle to BigQuery.
        *   Language: BigQuery DDL
    *   Create the `job_audit_log` table for logging and auditing purposes.
        *   Language: BigQuery DDL

2.  **BigQuery Stored Procedure for Core Data Processing:**
    *   Convert `d_ausd_bp_ta_rn_da_vda_tk.sql` into a BigQuery stored procedure, `sp_ausd_bp_ta_rn_da_vda_tk`.
        *   Language: BigQuery SQL

3.  **BigQuery Stored Procedure for Orchestration:**
    *   Convert `r_ausd_bp_ta_rn_da_vda_tk.ksh` and `k_ausd_bp_ta_rn_da_vda_tk.ksh` logic into a BigQuery stored procedure, `sp_bereitstellung_basisprodukte_bert`. This procedure will incorporate parameter handling, logging, error handling, and the call to `sp_ausd_bp_ta_rn_da_vda_tk`.
        *   Language: BigQuery SQL

4.  **Data Ingestion (Initial Load):**
    *   Perform an initial historical load of data from the Oracle source tables (`dwtk_meldungen`, `sof$ta_rn_einzeln`) into their respective BigQuery target tables.
        *   Language: Dataflow, BigQuery Data Transfer Service, or other ETL tool.

5.  **Scheduling:**
    *   Set up a scheduler (e.g., Cloud Composer/Airflow, Cloud Workflows, or Dataform) to invoke `sp_bereitstellung_basisprodukte_bert` at the required frequency.
        *   Language: Python (for Airflow DAGs), YAML (for Workflows), or Dataform SQLX.