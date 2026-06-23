# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh

## 1. Purpose & Scope
This migration targets an assembled job identified by `run_id: 5af228f1-3847-4cc6-9310-ed82ed19407c` and `seed_name: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh`. The job's primary purpose, as indicated in its `purpose_note`, is to provide a "snapshot extract of the base table for the BERT Report".

The `r_ausd_austausch.ksh` script acts as an orchestration layer, handling parameter parsing, date determination, and calling a core processing script `k_ausd_austausch.ksh`. This core script, in turn, executes a complex Oracle SQL script `d_ausd_austausch.sql` which performs the actual data extraction, transformation, and loading into reporting tables.

The scope of this migration involves re-implementing the entire workflow, including parameter handling, date calculations, data transformations, and table updates, onto the Google Cloud BigQuery platform.

## 2. Source Inventory
The job consists of three primary source files, all residing in the legacy KornShell/Oracle environment:

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh`**
    *   **Technology:** KornShell Script
    *   **Purpose:** Orchestration, parameter handling, date defaulting, job logging, invocation of `k_ausd_austausch.ksh`.
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Complexity Signals:** None recorded.
    *   **Notes:** This script serves as the entry point, defining the job's overall flow and error handling.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_austausch.ksh`**
    *   **Technology:** KornShell Script
    *   **Purpose:** Control script, parameter validation, date format checks, execution of `gestern.ksh`, execution of `d_ausd_austausch.sql` via an SQL*Plus utility.
    *   **Complexity Tier:** (Inherited from SQL script)
    *   **Automation Bucket:** (Inherited from SQL script)
    *   **Complexity Signals:** None recorded.
    *   **Notes:** This script links the orchestration with the core data processing.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`**
    *   **Technology:** KornShell Script
    *   **Purpose:** Calculates "today's" date and "yesterday's" date in `YYYYMMDD` format, handling month/year rollovers and basic leap year logic.
    *   **Complexity Tier:** (Low, logic is self-contained)
    *   **Automation Bucket:** (Likely Auto, for simple date functions)
    *   **Complexity Signals:** None recorded.
    *   **Notes:** This is a utility script that provides date values used by `k_ausd_austausch.ksh`.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_austausch.sql`**
    *   **Technology:** Oracle SQL / PL/SQL
    *   **Purpose:** Core data transformation logic for the BERT report. It updates several `rpt$...` tables using a "new table and swap" pattern, performing complex `INSERT ... SELECT` operations with numerous joins, `CASE` statements, and Oracle-specific functions (`decode`, `nvl`, `substr`, `to_date`). It also manages index creation/dropping and statistics collection.
    *   **Complexity Tier:** (Likely Complex/Very Complex due to Oracle-specific syntax and complex logic)
    *   **Automation Bucket:** (Likely Semi-Auto/Manual for complex SQL)
    *   **Complexity Signals:** Extensive Oracle SQL, DDL operations, swap pattern, complex CASE/DECODE logic, multiple joins.
    *   **Notes:** This is the most critical component in terms of data transformation.

## 3. Target Architecture
The target platform is Google BigQuery. The migration will leverage BigQuery's capabilities for data storage, transformation, and orchestration.

*   **Data Storage:** All source Oracle tables (`sof$ta_p_rech_empf`, `sof$ta_p_vertrag`, `sof$ta_p_basisprod`, `sof$ta_p_gesch_part`, `sof$ta_p_dn_nutzer`, `sof$ta_p_evn_empf`, `sof$ta_p_discount`, `sof$ta_p_discount_rr`, `sof$ta_p_d1_vpn`) will be migrated to BigQuery as standard tables within a designated dataset (e.g., `project.source_dataset`). The target reporting tables (`rpt$ta_s_d1_rech_empf`, `rpt$ta_s_d1_vertrag`, `rpt$ta_s_d1_rech_kunde`, `rpt$ta_s_d1_discount`, `rpt$ta_s_d1_discount_rr`, `rpt$ta_s_d1_vpn`) will also reside in BigQuery (e.g., `project.reporting_dataset`).

*   **Transformation Logic:** The logic from `d_ausd_austausch.sql` will be re-implemented as one or more BigQuery SQL stored procedures. The complex `CASE` statements, `DECODE`, `NVL`, and `SUBSTR` functions will be translated to their BigQuery equivalents (`CASE`, `IFNULL`, `SUBSTR`). The "new table and swap" pattern will be replaced with a `MERGE` statement where feasible, or a sequence of `CREATE TABLE AS SELECT` with appropriate table renames/drops, potentially orchestrated by an external tool.

*   **Orchestration:** The shell scripts (`r_ausd_austausch.ksh`, `k_ausd_austausch.ksh`) will be replaced by a main BigQuery stored procedure (e.g., `BERT_AUSTAUSCH_KSH_SP`) that orchestrates the overall flow.
    *   Parameter handling (`-s`, `-l`) will be managed via stored procedure input parameters.
    *   The date calculation logic from `gestern.ksh` will be incorporated directly into the BigQuery stored procedure using BigQuery's date functions (`CURRENT_DATE()`, `FORMAT_DATE`).
    *   Job logging and status updates will be handled by inserting/updating records in dedicated BigQuery job control and log tables.
    *   The execution of the main data transformation logic (from `d_ausd_austausch.sql`) will be encapsulated within a separate BigQuery stored procedure (e.g., `D_AUSD_AUSTAUSCH_SP`) called by the orchestration procedure.

*   **Temporary Tables:** The temporary tables (`sof$ta_rechdef`, `sof$ta_kd_kto`) used in `d_ausd_austausch.sql` will be implemented as temporary tables or Common Table Expressions (CTEs) within the BigQuery transformation stored procedure.

## 4. Data Flow & Lineage
The redesigned data flow in BigQuery will be as follows:

1.  **Main Orchestration Stored Procedure (`BERT_AUSTAUSCH_KSH_SP`):**
    *   Receives `p_stichtag` (reference date) and `p_wiederanlaufWert` (restart value) as input parameters.
    *   Determines `v_sysdate`, `v_stichtag`, `v_wiederanlaufWert` based on input and default logic (replaces `gestern.ksh` and date logic in `r_ausd_austausch.ksh`).
    *   Validates parameters.
    *   Records job start in `job_control` and `job_log` BigQuery tables.
    *   **Calls Data Transformation Stored Procedure (`D_AUSD_AUSTAUSCH_SP`).**
    *   Records job completion (success/failure) in `job_control` and `job_log` BigQuery tables.

2.  **Data Transformation Stored Procedure (`D_AUSD_AUSTAUSCH_SP`):**
    *   Receives necessary parameters (e.g., `v_stichtag`, `v_wiederanlaufWert`, job identifiers) from the orchestration procedure.
    *   **Phase 1: Update `rpt$ta_s_d1_rech_empf`**
        *   Triggers a `MERGE` statement or `CREATE TABLE AS SELECT ... FROM project.source_dataset.sof_ta_p_rech_empf` into a temporary `rpt_ta_s_d1_rech_empf_new` table.
        *   Updates the main `rpt$ta_s_d1_rech_empf` table (e.g., `MERGE` or `DROP/RENAME`).
    *   **Phase 2: Update `rpt$ta_s_d1_vertrag`**
        *   Triggers a `MERGE` statement or `CREATE TABLE AS SELECT ... FROM project.source_dataset.sof_ta_p_vertrag JOIN ... UNION ALL ...` into a temporary `rpt_ta_s_d1_vertrag_new` table.
        *   Updates the main `rpt$ta_s_d1_vertrag` table.
    *   **Phase 3: Update `rpt$ta_s_d1_rech_kunde`**
        *   Populates temporary tables/CTEs (`sof_ta_rechdef`, `sof_ta_kd_kto`) from `rpt$ta_s_d1_rech_empf` and `rpt$ta_s_d1_vertrag`.
        *   Triggers a `MERGE` statement or `CREATE TABLE AS SELECT ... FROM sof_ta_rechdef JOIN sof_ta_kd_kto` into `rpt_ta_s_d1_rech_kunde_new`.
        *   Updates the main `rpt$ta_s_d1_rech_kunde` table.
    *   **Phase 4: Update `rpt$ta_s_d1_discount`**
        *   Triggers a `MERGE` statement or `CREATE TABLE AS SELECT ... FROM project.source_dataset.sof_ta_p_discount` into `rpt_ta_s_d1_discount_new`.
        *   Updates the main `rpt$ta_s_d1_discount` table.
    *   **Phase 5: Update `rpt$ta_s_d1_discount_rr`**
        *   Triggers a `MERGE` statement or `CREATE TABLE AS SELECT ... FROM project.source_dataset.sof_ta_p_discount_rr` into `rpt_ta_s_d1_discount_rr_new`.
        *   Updates the main `rpt$ta_s_d1_discount_rr` table.
    *   **Phase 6: Update `rpt$ta_s_d1_vpn`**
        *   Triggers a `MERGE` statement or `CREATE TABLE AS SELECT ... FROM project.source_dataset.sof_ta_p_d1_vpn` into `rpt_ta_s_d1_vpn_new`.
        *   Updates the main `rpt$ta_s_d1_vpn` table.
    *   All `CREATE INDEX`, `ALTER INDEX`, `DROP INDEX`, `ANALYZE TABLE` commands will be replaced by BigQuery's implicit indexing for performance or omitted if not necessary.

## 5. Transformation Logic
The core transformation logic will be directly translated from Oracle SQL to BigQuery SQL, addressing specific function and syntax differences:

*   **Date Functions:**
    *   Oracle `date '+ %d %m %Y'` in `gestern.ksh` -> BigQuery `FORMAT_DATE('%d %m %Y', CURRENT_DATE())`.
    *   Oracle `to_date('11.11.1111','dd.mm.yyyy')` -> BigQuery `PARSE_DATE('%d.%m.%Y', '11.11.1111')` or `DATE '1111-11-11'`.
    *   The complex date calculation logic in `gestern.ksh` will be simplified using BigQuery's `DATE_SUB`, `LAST_DAY`, and other date arithmetic functions.
*   **Conditional Logic:**
    *   Oracle `DECODE` function -> BigQuery `CASE` statement.
    *   Oracle `NVL` function -> BigQuery `IFNULL` or `COALESCE`.
*   **String Manipulation:**
    *   Oracle `SUBSTR` -> BigQuery `SUBSTR` (syntax is compatible).
    *   Oracle `TO_CHAR` -> BigQuery `CAST(... AS STRING)`.
*   **Joins:** Oracle implicit joins (comma-separated tables in `FROM` clause with join conditions in `WHERE`) will be explicitly converted to BigQuery `JOIN` syntax (`INNER JOIN`, `LEFT JOIN` for `(+)`).
*   **Table Management (Swap Pattern):** The `RENAME table_old to table_new`, `TRUNCATE table_new`, `RENAME table to table_old`, `RENAME table_new to table` pattern will be handled in BigQuery.
    *   For the main table updates, `MERGE INTO target_table USING source_data ON join_conditions WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...` is the preferred BigQuery idiom for upserts and can handle incremental updates efficiently based on the `p_wiederanlaufWert` logic if applicable.
    *   Alternatively, for a full refresh as implied by `TRUNCATE`, a `CREATE OR REPLACE TABLE target_table AS SELECT ...` followed by `ALTER TABLE target_table RENAME TO target_table_new` and then a final `ALTER TABLE target_table_new RENAME TO target_table` can simulate the swap, or simply `CREATE OR REPLACE TABLE` if a brief downtime is acceptable. Given the complexity, a `MERGE` operation is often more robust.
*   **Index Management:** BigQuery does not have user-defined indexes in the same way Oracle does. Query performance is managed through partitioning, clustering, and appropriate query design. The `CREATE INDEX`, `DROP INDEX`, `ALTER INDEX`, and `ANALYZE TABLE` statements will be removed or replaced with BigQuery-specific optimizations (e.g., defining clustering and partitioning keys on the BigQuery tables).
*   **Restart Logic (`p_wiederanlaufWert`):** The logic `DWH_VERTRAG_ID > Wiederanlaufwert` and subsequent deletion will be directly translated into the `WHERE` clauses of `DELETE` and `INSERT` (or `MERGE`) statements within BigQuery SQL, ensuring idempotency and restartability.
*   **MultiSIM and other complex `CASE` logic:** These extensive `CASE` statements will be directly translated to BigQuery `CASE` expressions.
*   **Temporary Tables (`sof$ta_rechdef`, `sof$ta_kd_kto`):** These will be refactored into Common Table Expressions (CTEs) within the main transformation query for better readability and performance in BigQuery, or if necessary, as actual temporary tables in BigQuery.

## 6. External Dependencies
The original system has the following external/implicit dependencies:

*   **Oracle Database:** All source and target tables (`sof$ta_p_...`, `rpt$ta_s_d1_...`) reside in an Oracle database.
    *   **Replacement:** These will be migrated to BigQuery tables. Data ingestion from Oracle to BigQuery can be achieved using tools like Database Migration Service, batch exports, or Change Data Capture (CDC) solutions.
*   **Shell Environment (`$HOME/.dw_init`):** This script sources a `.dw_init` file, implying environment variable setup.
    *   **Replacement:** Relevant environment variables will be passed as parameters to BigQuery stored procedures or managed as configuration within the BigQuery environment (e.g., dataset names, project IDs).
*   **Custom Shell Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`):** These provide logging, parameter parsing, date utilities, and SQL*Plus interaction.
    *   **Replacement:**
        *   **Logging/Error Handling:** Replaced by dedicated BigQuery logging tables (`job_control`, `job_log`) and BigQuery's `RAISE ERROR` or `SIGNAL SQLSTATE` within stored procedures for error conditions.
        *   **Parameter Parsing:** Replaced by BigQuery stored procedure parameters.
        *   **Date Utilities (`h_alis_date.ksh`, `gestern.ksh`):** Replaced by BigQuery's native date and time functions.
        *   **SQL*Plus Interaction (`h_alis_sqlplus.ksh`):** Replaced by direct BigQuery SQL execution within stored procedures.
*   **Temporary Files (`tmpFile`):** Used to communicate record counts between SQL and shell.
    *   **Replacement:** This data can be directly returned from a BigQuery stored procedure or logged directly to a BigQuery log table.
*   **Log File Output (`${BERT_DIR_ROOT}/../../LOG/DATENSTAND.log`):**
    *   **Replacement:** Log entries will be written to a BigQuery log table. The `DATENSTAND.log` specific output can be managed by writing to a Cloud Storage bucket if a file output is strictly necessary, but typically this would be a BigQuery table entry.

## 7. Unresolved / Risks
*   **Oracle `isbert_schema.dwpa_util_skript.runstatement`:** The SQL script executes DDL operations via a utility package. This package's exact implementation details are not available. It's assumed to be a wrapper around standard DDL. The direct translation will be BigQuery DDL commands (`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`) or `MERGE` operations, but verification of the utility's specific behavior for locking/transactions is needed.
*   **Performance of complex `CASE` statements and joins:** The `d_ausd_austausch.sql` contains very large `SELECT` statements with many joins and intricate `CASE` logic. While BigQuery is highly performant, careful review and optimization (e.g., suitable partitioning/clustering on BigQuery tables, optimizing join order) will be crucial after initial translation.
*   **`(+)` Oracle Outer Join Syntax:** This needs careful conversion to BigQuery `LEFT JOIN` to ensure equivalent behavior, especially given the complexity of multiple `(+)` in the original query.
*   **Data Types:** Implicit data type conversions in Oracle might need explicit casting in BigQuery. A detailed schema mapping exercise is required.
*   **`WHENEVER SQLERROR CONTINUE/EXIT FAILURE`:** BigQuery stored procedures use `BEGIN...EXCEPTION...END` blocks for error handling. The exact error handling logic of these Oracle directives needs to be fully understood and mapped to BigQuery.
*   **`parallel (table, N)` hints:** These Oracle-specific hints for parallel execution will be removed as BigQuery automatically manages parallelism.
*   **`TRUNCATE REUSE STORAGE`:** BigQuery `TRUNCATE TABLE` does not have a `REUSE STORAGE` equivalent in the same way Oracle does. This is generally not an issue as BigQuery manages storage automatically.
*   **Unused Code:** There are commented-out sections (`--AL??`) and blocks within `d_ausd_austausch.sql` that appear to be deprecated or for specific testing (`INSERT INTO SOF$TA_K_BERT_DATENSTAND`). These should be omitted from the BigQuery migration unless explicitly required.

## 8. Build Plan
The migration build plan will proceed in several logical steps:

1.  **Schema Migration:**
    *   Migrate all source Oracle tables (`sof$ta_p_...`) to BigQuery. Define appropriate data types, partitioning, and clustering keys.
    *   Migrate all target reporting tables (`rpt$ta_s_d1_...`) to BigQuery, including temporary tables (`sof$ta_rechdef`, `sof$ta_kd_kto`) that will be replaced by CTEs or temporary tables in BigQuery.
    *   Define BigQuery tables for job control and logging (e.g., `job_control`, `job_log`).
    *   **Language:** DDL (BigQuery SQL)

2.  **Date Utility Translation:**
    *   Translate the logic from `gestern.ksh` into BigQuery SQL functions or directly into the main orchestration stored procedure.
    *   **Language:** BigQuery SQL

3.  **Data Transformation Procedure (`D_AUSD_AUSTAUSCH_SP`):**
    *   Translate the `d_ausd_austausch.sql` script into a BigQuery SQL stored procedure (`D_AUSD_AUSTAUSCH_SP`).
    *   Convert all Oracle-specific functions (`DECODE`, `NVL`, `(+)` joins) to BigQuery equivalents.
    *   Refactor the temporary table population (`sof$ta_rechdef`, `sof$ta_kd_kto`) into CTEs.
    *   Re-implement the table update logic (inserting into `_new` tables and then swapping) using BigQuery's `MERGE` statement for incremental/upsert logic, or `CREATE OR REPLACE TABLE` statements combined with `DROP` and `RENAME` commands if a full refresh is always intended and atomicity can be managed through orchestration.
    *   Remove all Oracle index and analyze commands.
    *   **Language:** BigQuery SQL

4.  **Orchestration Procedure (`BERT_AUSTAUSCH_KSH_SP`):**
    *   Create a BigQuery SQL stored procedure (`BERT_AUSTAUSCH_KSH_SP`) to replace `r_ausd_austausch.ksh` and `k_ausd_austausch.ksh`.
    *   Implement parameter parsing and validation using stored procedure input parameters.
    *   Integrate the translated date calculation logic.
    *   Implement logging and error handling by inserting/updating records in BigQuery `job_control` and `job_log` tables.
    *   Call the `D_AUSD_AUSTAUSCH_SP` procedure with the necessary parameters.
    *   **Language:** BigQuery SQL

5.  **External Orchestration (Optional/Advanced):**
    *   If complex scheduling, dependency management, or integration with other systems is required, deploy the BigQuery stored procedures within a Cloud Composer (Apache Airflow) DAG or Cloud Workflows.
    *   **Language:** Python (for Airflow DAG) or YAML (for Cloud Workflows)