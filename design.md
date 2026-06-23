# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh

## 1. Purpose & Scope
This job, `k_ausd_v_ta_cntrct_templ.ksh`, is a control script primarily responsible for orchestrating the execution of an SQL script, `d_ausd_v_ta_cntrct_templ.sql`. Its main purpose is to manage job execution, including:
- Ignoring active jobs to prevent concurrent processing.
- Invoking the core SQL data processing script.
- Registering job activity or status in a job-tracking mechanism (implied by references to 'Job-Tabelle').
- Deactivating older active jobs.
The script processes contract template data, likely extracting from source systems and loading into a target system, with a focus on historical validity periods and production status.

## 2. Source Inventory
The job is composed of a main KornShell script and an invoked SQL script.

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh`
- **Technology:** KornShell (shell)
- **Complexity Tier:** Medium
- **Migration Bucket:** Semi-automatic (B2)
- **Purpose:** Orchestration, parameter parsing, error handling, SQL script execution.

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_templ.sql`
- **Technology:** Oracle SQL (implied by syntax like `NVL`, `TO_CHAR`, `prompt`, `WHENEVER SQLERROR`, `COMMIT`, and `isbert_schema.DWPA_UTIL_SKRIPT`).
- **Complexity Tier:** Not explicitly analyzed by the tool, but contains complex DML operations (INSERT, SELECT) and schema-specific details.
- **Migration Bucket:** Not explicitly analyzed, likely B2 or B3 (semi-automatic to manual) depending on the complexity of SQL conversion.
- **Purpose:** Data extraction, transformation, and loading of contract template data.

## 3. Target Architecture
The target platform is Google BigQuery. The migration will involve:
- **Orchestration:** The KornShell script's control logic will be re-implemented as a BigQuery Stored Procedure or integrated into an orchestration tool like Cloud Composer (Airflow DAG). Parameter handling (`p_JobKennung`, `p_EintragsNr`) will translate to stored procedure arguments.
- **Data Processing:** The Oracle SQL script will be converted to BigQuery SQL. This will involve translating Oracle-specific functions and syntax, schema references, and DML operations.
- **Tables:** Source tables (e.g., `isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_template`, `cds$ta_care_description`) and target tables (e.g., `sof$ta_cntrct_templ`) will be migrated to BigQuery tables within appropriate datasets (e.g., `project.dataset.dwtk_meldungen`, `project.dataset.ta_cntrct_template`, `project.dataset.sof_ta_cntrct_templ`).
- **Temporary Data:** The temporary file usage for record counting will be replaced by BigQuery `DECLARE` variables or `CREATE TEMP TABLE` constructs within the stored procedure.
- **Logging & Error Handling:** The custom shell error framework will be replaced with BigQuery scripting's `RAISE` and `EXCEPTION WHEN ERROR THEN` constructs.

## 4. Data Flow & Lineage
The overall data flow for this job is as follows:

1.  **`r_ausd_v_ta_cntrct_templ.ksh` (invoker script, not analyzed in detail, but identified as invoking the seed)**
    - Invokes: `k_ausd_v_ta_cntrct_templ.ksh`

2.  **`k_ausd_v_ta_cntrct_templ.ksh` (seed script)**
    - Invokes helper scripts: `. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` (for utility functions).
    - Executes SQL script: `d_ausd_v_ta_cntrct_templ.sql`
    - Writes: A temporary file for record count (`$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_templ_$$.tmp`).

3.  **`d_ausd_v_ta_cntrct_templ.sql` (core data processing script)**
    - Reads from:
        - `TABLE:DWTK_MELDUNGEN` (from `isbert_schema.dwtk_meldungen`) - used to determine `v_datum` (snapshot date).
        - `TABLE:CDS$TA_CNTRCT_TEMPLATE` (from `cds$ta_cntrct_template`)
        - `TABLE:CDS$TA_CARE_DESCRIPTION` (from `cds$ta_care_description`)
    - Writes to:
        - `TABLE:SOF$TA_CNTRCT_TEMPL` (into `sof$ta_cntrct_templ`) via `INSERT` statement after truncating.
        - `TABLE:VIA` (via `MERGE`) - this appears to be an internal or temporary table used in the merge operation.
    - Uses Package: `PACKAGE:DWPA_UTIL_SKRIPT.runstatement` (to execute `TRUNCATE TABLE`).

**Execution Order:**
- The KSH script (`k_ausd_v_ta_cntrct_templ.ksh`) is the entry point, likely called by `r_ausd_v_ta_cntrct_templ.ksh`.
- It performs setup, parameter validation.
- It then executes the SQL script (`d_ausd_v_ta_cntrct_templ.sql`), which performs the actual data manipulation.
- Finally, the KSH script captures the record count from a temporary file.

## 5. Transformation Logic
**`k_ausd_v_ta_cntrct_templ.ksh` (Orchestration Logic):**
- **Parameter Handling:** Parses `-j` (Jobkennung) and `-f` (EintragsNr) using `getopts`.
- **Environment Setup:** Sources `$HOME/.dw_init` and several utility KSH scripts for error handling, date functions, parameter parsing, and SQL*Plus routines.
- **Validation:** Uses `pruefeParameterGesetzt` to validate required parameters. Exits with an error if validation fails.
- **SQL Execution:** Calls `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) to execute `d_ausd_v_ta_cntrct_templ.sql` with `p_EintragsNr` and `p_JobKennung` as arguments.
- **Record Count:** Reads a record count from a temporary file `"$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_templ_$$.tmp"` into `v_records`.

**`d_ausd_v_ta_cntrct_templ.sql` (Data Transformation Logic):**
- **Date Determination:** Retrieves a snapshot date (`v_datum`) from `isbert_schema.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
- **Temporary Table Handling:** Truncates `sof$ta_cntrct_templ` using `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
- **Data Insertion:** Inserts data into `sof$ta_cntrct_templ` by selecting from `cds$ta_cntrct_template` and `cds$ta_care_description`.
    - **Join Condition:** `ct.cds_description_id = cd.cds_description_id`.
    - **Filtering Conditions:**
        - `ct.insert_at <= TO_DATE('&v_datum','YYYYMMDD')`
        - `(ct.modified_at IS NULL OR ct.modified_at > TO_DATE('&v_datum','YYYYMMDD'))`
        - `ct.valid_from <= TO_DATE('&v_datum','YYYYMMDD')`
        - `(ct.valid_to IS NULL OR ct.valid_to > TO_DATE('&v_datum','YYYYMMDD'))`
        - `ct.is_production = 1`
        - `cd.language = 1`
    - The `&v_carmen` definition suggests a database link or schema alias, likely for the `cds` tables.
- **MERGE operation:** The comment `merge via` indicates a MERGE statement, but the full SQL was not provided in the source file output, only the `WRITES_TABLE:VIA` edge. This part needs further investigation if the full SQL is available in the original source system.
- **Commit:** Explicit `COMMIT` statement.

## 6. External Dependencies
- **Database Connection:** The scripts interact with an Oracle database (implied by SQL*Plus routines and syntax). The SQL script uses a DB-link `v_carmen` (`@pcrs1`). This will be replaced by direct BigQuery table access.
- **External Systems:** The `lineage_assembled_jobs` indicated no explicit `external_systems` for this job, but the SQL script implicitly depends on the `isbert_schema` and `cds` schema within Oracle.
- **KornShell Utilities:** Relies on several sourced KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). These utility functions for error handling, date manipulation, and SQL execution will need to be re-implemented natively in BigQuery Scripting or Python for Cloud Composer.
- **Job Control:** The script interacts with a "Job-Tabelle" and has logic for ignoring/deactivating jobs. This implies an external job control or scheduling system (e.g., UC4 from other lineage data, though not directly linked to *this* file). This functionality needs to be recreated in BigQuery using control tables or handled by the orchestrator.

## 7. Unresolved / Risks
- **`unresolved_targets`:** The `lineage_assembled_jobs` record showed no `unresolved_targets`.
- **Full `MERGE` statement:** The complete `MERGE` statement involving `TABLE:VIA` from `d_ausd_v_ta_cntrct_templ.sql` was not available. This needs to be fully analyzed for accurate BigQuery conversion.
- **`DWPA_UTIL_SKRIPT` package:** The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` call needs to be understood. If it performs more than just a `TRUNCATE TABLE`, that logic needs to be migrated.
- **Job Control Logic:** The specific implementation of "aktive Jobs werden ignoriert" and "alte aktive Jobs werden einfach dekativiert" within `starteSQLSkript` or the SQL script needs full understanding to replicate accurately in BigQuery and the orchestration layer.
- **Environment Variables:** The values for `$HOME`, `BERT_DIR_ROOT`, and `DW_DIR_UTL` are not hardcoded. These must be externalized as BigQuery environment variables, Airflow variables, or parameters in the target environment.
- **Schema Mapping:** The exact mapping of `isbert_schema`, `cds`, and `sof` schemas to BigQuery datasets needs to be defined.

## 8. Build Plan
1.  **Schema Migration:**
    - Create BigQuery datasets for `isbert`, `cds`, and `sof` schemas (or consolidate if appropriate for the target architecture).
    - Define BigQuery table DDLs for `dwtk_meldungen`, `ta_cntrct_template`, `ta_care_description`, `sof_ta_cntrct_templ`, and `VIA` based on their Oracle definitions.

2.  **SQL Script Conversion (`d_ausd_v_ta_cntrct_templ.sql`):**
    - Translate Oracle SQL to BigQuery SQL, specifically:
        - `NVL` to `COALESCE`.
        - `TO_CHAR` and `TO_DATE` functions.
        - `@pcrs1` DB-link to direct BigQuery table references.
        - `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` to `TRUNCATE TABLE` or equivalent DDL.
        - Ensure the `MERGE` statement (once fully understood) is converted correctly to BigQuery `MERGE`.
    - Create a BigQuery SQL script or a BigQuery Stored Procedure (`d_ausd_v_ta_cntrct_templ_bq.sql` or `proc_d_ausd_v_ta_cntrct_templ`) for this logic.

3.  **KornShell Control Script Conversion (`k_ausd_v_ta_cntrct_templ.ksh`):**
    - Create a BigQuery Stored Procedure (`proc_k_ausd_v_ta_cntrct_templ`) that encapsulates the control logic:
        - Input parameters for `p_JobKennung` and `p_EintragsNr`.
        - Parameter validation using `IF` and `RAISE`.
        - Calls the migrated BigQuery SQL script/stored procedure (`proc_d_ausd_v_ta_cntrct_templ`).
        - Replaces temporary file interaction for record counting with BigQuery `DECLARE` variables and `SELECT COUNT(*)` assignments.
        - Implement job control logic (ignoring active jobs, deactivating old jobs) using BigQuery DML on a job control table.
    - Alternatively, create a Python script for Cloud Composer that orchestrates the BigQuery SQL, handling parameters and job control.

4.  **Orchestration Layer Integration:**
    - If `r_ausd_v_ta_cntrct_templ.ksh` is part of a larger workflow, integrate the new BigQuery stored procedure/Python script into the target orchestration tool (e.g., an Airflow DAG in Cloud Composer).

5.  **Testing:**
    - Unit test the converted BigQuery SQL and stored procedures.
    - Integration test the end-to-end job in the BigQuery environment.
    - Performance test and optimize BigQuery queries.