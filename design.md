# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh

## 1. Purpose & Scope
This job, `k_ausd_v_ta_bp_ref.ksh`, serves as a control script for data processing related to `ta_bp_ref` (Business Partner Reference) entities. Its primary purpose is to orchestrate the execution of a SQL script (`d_ausd_v_ta_bp_ref.sql`) which extracts, filters, and loads data, while managing job states. The script handles environment initialization, parses input parameters, performs error checking, and logs job entries. It also ensures that active jobs are handled appropriately (ignoring current active jobs and deactivating old ones).

## 2. Source Inventory
This job comprises two main components:

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh`**
    *   **Technology:** Shell Script (KornShell)
    *   **Role:** Orchestration, Parameter Handling, Error Management, SQL Execution Wrapper
    *   **Complexity Tier:** Medium
    *   **Migration Bucket:** Semi-Automatic
    *   **Summary:** A KSH script that initializes the environment, parses parameters, validates them, and executes a SQL*Plus script to process data. It manages job entries and reports record counts.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_bp_ref.sql`**
    *   **Technology:** Oracle SQL*Plus
    *   **Role:** Data Transformation (TRUNCATE & INSERT...SELECT)
    *   **Complexity Tier:** Simple (inferred, as it's a direct DML operation)
    *   **Migration Bucket:** Automatic (inferred, for SQL conversion)
    *   **Summary:** An Oracle SQL script that determines a processing date, truncates the `sof$ta_bp_ref` table, and then inserts filtered data from `cds$ta_bp_ref` into `sof$ta_bp_ref`. It potentially interacts with an external Oracle database via a DB-Link.

## 3. Target Architecture
The migrated job will leverage Google Cloud Platform services, primarily BigQuery for data storage and transformation, and potentially Cloud Composer or Workflows for orchestration.

*   **BigQuery Stored Procedure:** The KornShell script (`k_ausd_v_ta_bp_ref.ksh`) will be refactored into a BigQuery Stored Procedure (e.g., `project.dataset.k_ausd_v_ta_bp_ref_sp`). This SP will handle parameter passing, validation, error handling, and orchestrate the data transformation logic.
*   **BigQuery Tables:**
    *   **Source Tables:** `isbert_schema.dwtk_meldungen` and `cds$ta_bp_ref` will be migrated to BigQuery as `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_bp_ref` respectively.
    *   **Target Table:** `sof$ta_bp_ref` will be migrated to BigQuery as `project.dataset.sof_ta_bp_ref`.
    *   **Logging/Audit Tables:** New BigQuery tables like `project.dataset.job_error_log` and `project.dataset.job_control` will be created to manage job status, errors, and record counts, replacing the shell script's internal logging and temporary file usage.
*   **Orchestration (Optional External):** If the original invocation (`r_ausd_v_ta_bp_ref.ksh`) has complex scheduling or dependency requirements not directly handled by BigQuery SPs, Cloud Composer (Apache Airflow) or Google Cloud Workflows can be used to schedule and trigger the BigQuery Stored Procedure.

## 4. Data Flow & Lineage
The migrated job will follow this data flow:

1.  **Invocation:** An external scheduler (e.g., a Cloud Composer DAG replacing `r_ausd_v_ta_bp_ref.ksh`) will invoke the BigQuery Stored Procedure `project.dataset.k_ausd_v_ta_bp_ref_sp`, passing `p_JobKennung` and `p_EintragsNr` as parameters.
2.  **Parameter Validation & Environment Setup:** The BigQuery Stored Procedure will validate the input parameters and initialize any necessary variables.
3.  **Date Determination:** The SP will query the `project.dataset.dwtk_meldungen` table to determine the `v_datum` (processing date), mimicking the `SELECT NVL(TO_CHAR(MAX(m.timecreated),...),...) FROM isbert_schema.dwtk_meldungen` logic.
4.  **Target Table Preparation:** The SP will execute a `TRUNCATE TABLE project.dataset.sof_ta_bp_ref` statement, replacing the `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` call.
5.  **Data Transformation:** The core `INSERT INTO ... SELECT FROM` logic from `d_ausd_v_ta_bp_ref.sql` will be executed within the BigQuery Stored Procedure. Data will be read from `project.dataset.cds_ta_bp_ref` (and potentially other tables if `cds$ta_bp_ref` has implicit dependencies) and inserted into `project.dataset.sof_ta_bp_ref`, applying the original `WHERE` clause filters.
6.  **Record Count & Logging:** After the data transformation, the SP will query `project.dataset.sof_ta_bp_ref` to get the count of processed records (`v_records`) and update the `project.dataset.job_control` and `project.dataset.job_error_log` tables with job status, record counts, and any encountered errors.
7.  **Completion:** The SP will return a completion status or relevant output.

## 5. Transformation Logic

### 5.1. Orchestration Logic (from `k_ausd_v_ta_bp_ref.ksh`)

*   **Parameter Handling:** The shell script's `getopts` parameter parsing (`-j p_JobKennung`, `-f p_EintragsNr`) will be directly translated to input parameters of the BigQuery Stored Procedure.
*   **Environment Variables:** Shell environment variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) and sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) will be replaced by:
    *   BigQuery Stored Procedure variables and parameters for configuration paths or values.
    *   Built-in BigQuery functions for date operations.
    *   Custom logging and error handling logic within the SP for `f_alis_msgerr.ksh`.
    *   The `h_alis_sqlplus.ksh` wrapper functionality will be subsumed by directly executing BigQuery SQL.
*   **Error Checking & Exit:** The `if [ ! $ErrNr -eq 0 ]` conditional logic, `pruefeParameterGesetzt`, `DWMSG_MeldeFehler`, and `exit $ErrNr` will be translated to BigQuery `IF` statements, `RAISE` (for errors), and inserts into `job_error_log` table.
*   **Temporary File:** The usage of `$DW_DIR_UTL/bert_k_ausd_v_ta_bp_ref_$$.tmp` to store `v_records` will be replaced by directly assigning the `COUNT(*)` result to a BigQuery variable within the Stored Procedure or logging it to the `job_control` table.
*   **SQL Script Execution:** The `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung` call will be replaced by embedding the migrated SQL transformation logic (from `d_ausd_v_ta_bp_ref.sql`) directly within the BigQuery Stored Procedure.

### 5.2. Data Transformation Logic (from `d_ausd_v_ta_bp_ref.sql`)

*   **Variable Definitions:**
    *   `DEFINE v_carmen = "@pcrs1"`: The DB-Link will be removed. Source tables will be directly referenced with their BigQuery project and dataset names (e.g., `project.dataset.cds_ta_bp_ref`).
    *   `COLUMN s_datum new_value v_datum noprint`: This logic to determine `v_datum` from `isbert_schema.dwtk_meldungen` will be converted to a BigQuery `SELECT MAX(timecreated)` statement, with appropriate `FORMAT_DATE` or `CAST` functions to match the `YYYYMMDD` format.
*   **Tracing and Settings (`START ../trace.sql.cfg`, `SPOOL ./tmp/trace_d_ausd_v_ta_bp_ref`, `WHENEVER SQLERROR`, `SET TIMING ON`, `SET SERVEROUTPUT ON`):**
    *   Tracing and spooling will be replaced by BigQuery's native logging capabilities (Cloud Logging) and standard BigQuery query execution details.
    *   Error handling will be integrated into the BigQuery Stored Procedure's `EXCEPTION` blocks or `IF` conditions for status checks.
*   **Table Truncation:** `begin isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bp_ref'); end;` will be directly translated to `TRUNCATE TABLE project.dataset.sof_ta_bp_ref;` within the BigQuery Stored Procedure.
*   **Core INSERT...SELECT Statement:** The main data movement logic will be translated to BigQuery SQL:
    ```sql
    INSERT INTO `project.dataset.sof_ta_bp_ref` (cntrct_cp2_id, bp_id)
    SELECT
        br.cntrct_cp2_id,
        br.bp_id
    FROM
        `project.dataset.cds_ta_bp_ref` AS br
    WHERE
        br.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
        AND (br.modified_at IS NULL OR br.modified_at > PARSE_DATE('%Y%m%d', v_datum))
        AND br.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
        AND (br.valid_to IS NULL OR br.valid_to > PARSE_DATE('%Y%m%d', v_datum))
        AND br.is_production = 1
        AND br.bp_ref_ty = 4;
    ```
    *Note: Data types and function names (`TO_DATE`, `NVL`) will be adapted to BigQuery equivalents (`PARSE_DATE`, `IFNULL`).*
*   **`commit;`:** Explicit `COMMIT` statements are not required in BigQuery DML, as transactions are typically auto-committed per statement.

## 6. External Dependencies

*   **Oracle Database (accessed via DB-Link `pcrs1`):** The dependency on `cds$ta_bp_ref` and `isbert_schema.dwtk_meldungen` tables in the Oracle database will be removed. These tables must be migrated to BigQuery datasets and tables (`project.dataset.cds_ta_bp_ref`, `project.dataset.dwtk_meldungen`) prior to the job migration.
*   **Oracle Stored Procedure (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`):** The functionality of this procedure (specifically `TRUNCATE TABLE`) will be replaced by native BigQuery DDL statements directly within the BigQuery Stored Procedure.
*   **KornShell Environment & Utility Scripts:** The sourcing of various KSH helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) will be eliminated. Their functionalities will be re-implemented using BigQuery scripting features, built-in functions, and dedicated logging tables.
*   **Temporary Filesystem (`$DW_DIR_UTL`):** The use of a temporary file for record counts will be replaced by BigQuery variables or direct inserts into BigQuery logging/audit tables.
*   **Upstream Invoker (`r_ausd_v_ta_bp_ref.ksh`):** The calling script will need to be updated or replaced by a Cloud Composer DAG or Cloud Workflow that invokes the new BigQuery Stored Procedure.

## 7. Unresolved / Risks

*   **Complete Schema Details:** The exact schemas (column names, data types) of `cds$ta_bp_ref`, `sof$ta_bp_ref`, and `isbert_schema.dwtk_meldungen` are assumed. A detailed schema mapping is required for precise BigQuery DDL generation.
*   **Utility Script Logic:** The full logic within the sourced KSH utility scripts (e.g., `pruefeParameterGesetzt`, `DWMSG_MeldeFehler`) is not fully known. Their exact behavior, especially complex error handling or date calculations, needs to be thoroughly understood and accurately translated into BigQuery scripting.
*   **Performance Tuning:** The initial BigQuery SQL translation may require performance tuning, especially for large datasets, by optimizing `WHERE` clauses, partitioning, and clustering keys in BigQuery tables.
*   **Data Consistency:** Ensure that the migration of `cds$ta_bp_ref` and `isbert_schema.dwtk_meldungen` maintains data consistency and integrity with BigQuery data types and storage.
*   **Security and Access Control:** Proper IAM roles and permissions must be configured for the BigQuery Stored Procedure and any external orchestrators to access the relevant BigQuery datasets and tables.
*   **Testing:** Comprehensive unit and integration testing will be crucial to ensure the migrated BigQuery Stored Procedure replicates the original job's functionality and data output accurately.

## 8. Build Plan

The migration will be executed in the following order:

1.  **BigQuery DDL for Logging Tables:**
    *   Create `project.dataset.job_error_log` table (BigQuery SQL DDL).
    *   Create `project.dataset.job_control` table (BigQuery SQL DDL).
2.  **BigQuery DDL for Data Tables:**
    *   Define and create `project.dataset.dwtk_meldungen` table (BigQuery SQL DDL), mirroring the Oracle `isbert_schema.dwtk_meldungen` schema.
    *   Define and create `project.dataset.cds_ta_bp_ref` table (BigQuery SQL DDL), mirroring the Oracle `cds$ta_bp_ref` schema.
    *   Define and create `project.dataset.sof_ta_bp_ref` table (BigQuery SQL DDL), mirroring the Oracle `sof$ta_bp_ref` schema.
3.  **Data Migration:**
    *   Perform one-time or continuous data migration from Oracle `isbert_schema.dwtk_meldungen` to BigQuery `project.dataset.dwtk_meldungen`.
    *   Perform one-time or continuous data migration from Oracle `cds$ta_bp_ref` to BigQuery `project.dataset.cds_ta_bp_ref`.
4.  **Develop BigQuery Stored Procedure:**
    *   Write the BigQuery Stored Procedure `project.dataset.k_ausd_v_ta_bp_ref_sp` in BigQuery SQL, encapsulating:
        *   Parameter definitions (`p_JobKennung`, `p_EintragsNr`).
        *   Validation logic.
        *   Date determination query.
        *   `TRUNCATE TABLE project.dataset.sof_ta_bp_ref;`
        *   The migrated `INSERT INTO ... SELECT FROM` statement.
        *   Error handling and logging to `job_error_log` and `job_control`.
        *   Record count retrieval and logging.
5.  **Develop/Update Orchestration:**
    *   If `r_ausd_v_ta_bp_ref.ksh` is part of a larger job stream, create or update a Cloud Composer DAG or Cloud Workflow to invoke `project.dataset.k_ausd_v_ta_bp_ref_sp` with the necessary parameters.
6.  **Testing:**
    *   Thoroughly test the BigQuery Stored Procedure for functional correctness and performance.
    *   Validate data output against the legacy system.