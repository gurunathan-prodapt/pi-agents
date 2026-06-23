# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh

## 1. Purpose & Scope
This job, `k_ausd_v_ta_cntrct_crs.ksh`, serves as a control script for the `r_ausd_vertrag.ksh` process. Its primary function is to orchestrate the execution of an underlying SQL script, `d_ausd_v_ta_cntrct_crs.sql`, for data preparation related to contract information (`ta_cntrct_crs`). The script handles job identification, parameter parsing, error management, and integrates with a job tracking mechanism. The overall purpose is to extract, filter, and transform contract data from a source Oracle system into a target staging table. This migration aims to replicate the existing ETL workflow on Google Cloud Platform, specifically utilizing BigQuery for data storage and transformation.

## 2. Source Inventory

### 2.1 `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh`
*   **Technology:** KornShell Script
*   **Category:** shell
*   **Tool (Detected):** KornShell
*   **Complexity Tier:** (Unknown - `file_complexity` returned no rows)
*   **Automation Bucket:** `semi_auto`
*   **Purpose:** Orchestration, parameter handling, error logging, and invocation of the SQL data processing script.

### 2.2 `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs.sql`
*   **Technology:** Oracle SQL*Plus Script
*   **Category:** SQL
*   **Tool (Detected):** (Implicitly handled by `shellscript_to_bqsql_design` and then `hql_sql_to_bqsql_design`)
*   **Complexity Tier:** (Unknown - `file_complexity` returned no rows)
*   **Automation Bucket:** (Part of `semi_auto` for the overall job)
*   **Purpose:** Data extraction, filtering, and insertion from `cds$ta_cntrct` into `sof$ta_cntrct_crs`, based on a determined `v_datum` and specific contract conditions.

## 3. Target Architecture

The target architecture will leverage BigQuery for data warehousing and transformations, with Cloud Composer (or Cloud Workflows) for orchestrating the overall job flow.

*   **BigQuery Datasets:**
    *   `project.isbert_schema`: To host migrated `dwtk_meldungen` table and potentially the `DWPA_UTIL_SKRIPT` functionality as a stored procedure.
    *   `project.staging`: To host the target table `sof_ta_cntrct_crs`.
    *   `project.source_cds`: To host the migrated `cds_ta_cntrct` table.
    *   `project.job_control`: To host `job_table`, `error_log`, `job_result_log`.

*   **BigQuery Stored Procedures:**
    *   `project.job_control.r_ausd_vertrag_control`: This will be the migrated KornShell script, responsible for parameter handling, job tracking, and invoking the core data processing stored procedure.
    *   `project.staging.d_ausd_v_ta_cntrct_crs`: This will be the migrated SQL script, handling the data transformation logic.

*   **BigQuery Tables:**
    *   `project.job_control.job_table`: To track job status and metadata.
    *   `project.job_control.error_log`: For logging errors.
    *   `project.job_control.job_result_log`: To store job execution results like record counts.
    *   `project.isbert_schema.dwtk_meldungen`: Migrated `dwtk_meldungen` table.
    *   `project.source_cds.cds_ta_cntrct`: Migrated `cds$ta_cntrct` from the Oracle Carmen DB.
    *   `project.staging.sof_ta_cntrct_crs`: Target table for the processed contract data.

*   **Orchestration:**
    *   Cloud Composer DAG (or Cloud Workflow) to trigger the `project.job_control.r_ausd_vertrag_control` stored procedure with appropriate parameters.

## 4. Data Flow & Lineage

1.  **Orchestration (External Trigger):** An external scheduler (e.g., Cloud Composer) invokes the `project.job_control.r_ausd_vertrag_control` BigQuery Stored Procedure, passing `p_JobKennung` and `p_EintragsNr` as parameters.
2.  **Job Control & Validation (BigQuery Stored Procedure):**
    *   `project.job_control.r_ausd_vertrag_control` validates input parameters.
    *   It updates/inserts records into `project.job_control.job_table` to manage job status.
    *   It determines the `v_datum` by querying `project.isbert_schema.dwtk_meldungen`.
    *   It then calls the `project.staging.d_ausd_v_ta_cntrct_crs` BigQuery Stored Procedure.
3.  **Data Transformation (BigQuery Stored Procedure):**
    *   `project.staging.d_ausd_v_ta_cntrct_crs` first `TRUNCATE`s the `project.staging.sof_ta_cntrct_crs` table.
    *   It then inserts data into `project.staging.sof_ta_cntrct_crs` by selecting from `project.source_cds.cds_ta_cntrct`.
    *   The selection criteria include filtering on `cntrct_st`, `redundant_owner_id`, `is_production`, `cntrct_ty`, and several date columns against the `v_datum` obtained earlier.
4.  **Result Logging (BigQuery Stored Procedure):**
    *   After the data transformation, `project.job_control.r_ausd_vertrag_control` queries `project.staging.sof_ta_cntrct_crs` to get the count of processed records.
    *   This record count is then logged into `project.job_control.job_result_log`.
    *   Error messages are logged to `project.job_control.error_log` if parameter validation fails or other issues arise.

**Original Lineage:**
*   `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh` `EXECUTES_SQL` `SCRIPT:D_AUSD_V_TA_CNTRCT_CRS.SQL`
*   `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh` `INVOKES` `OTHER:K_AUSD_V_TA_CNTRCT_CRS.KSH`
*   `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs.sql`
    *   `READS_TABLE` `DWTK_MELDUNGEN`
    *   `READS_TABLE` `CDS$TA_CNTRCT`
    *   `WRITES_TABLE` `SOF$TA_CNTRCT_CRS`
    *   `WRITES_TABLE` `VIA` (Further investigation needed for this target)
    *   `USES_PACKAGE` `DWPA_UTIL_SKRIPT`

## 5. Transformation Logic

### 5.1 KornShell Script (`k_ausd_v_ta_cntrct_crs.ksh`) to BigQuery Stored Procedure (`r_ausd_vertrag_control`)
*   **Environment Initialization (`. $HOME/.dw_init`):** This will be replaced by BigQuery stored procedure parameters, configuration variables within the procedure, or potentially by environment variables configured in Cloud Composer/Workflows.
*   **Parameter Parsing (`getopts`):** Replaced by `IN` parameters of the BigQuery stored procedure.
*   **Utility Scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):** These functionalities will be re-implemented directly within the BigQuery stored procedure using BigQuery's procedural language features (e.g., `IF`, `DECLARE`, `SET`) or by calling other helper stored procedures. Error logging will be directed to `project.job_control.error_log`.
*   **Job Status Management:** The logic for ignoring active jobs and deactivating old jobs will be translated into DML operations on `project.job_control.job_table`.
*   **SQL Script Execution (`starteSQLSkript`):** This will be replaced by a `CALL` statement to the `project.staging.d_ausd_v_ta_cntrct_crs` BigQuery stored procedure.
*   **Temporary File Handling (`tmpFile`, `cat`, `eval`):** The record count from the SQL execution will be obtained directly from the BigQuery DML operation or a subsequent `COUNT(*)` query on the target table and then stored in `project.job_control.job_result_log`.

### 5.2 Oracle SQL Script (`d_ausd_v_ta_cntrct_crs.sql`) to BigQuery Stored Procedure (`d_ausd_v_ta_cntrct_crs`)
*   **SQL*Plus Commands (`DEFINE`, `COLUMN`, `prompt`, `START`, `SPOOL`, `WHENEVER SQLERROR`, `SET TIMING`, `SET SERVEROUTPUT`, `COMMIT`):** These client-side or specific RDBMS commands will be removed. Transactional integrity will be managed by BigQuery's implicit transaction handling or explicit transaction blocks if necessary (though not common for simple `TRUNCATE`/`INSERT` patterns). Error handling will be integrated into the BigQuery procedural language.
*   **Date Determination:** The `SELECT NVL(TO_CHAR(MAX(m.timecreated),\'YYYYMMDD\'),\'19000101\')` query will be translated to use BigQuery date functions: `DECLARE v_datum DATE DEFAULT (SELECT IFNULL(MAX(DATE(m.timecreated)), DATE '1900-01-01') FROM project.isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE');`
*   **Truncate Table:** The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_crs REUSE STORAGE');` call will be replaced by a direct `TRUNCATE TABLE project.staging.sof_ta_cntrct_crs;` statement.
*   **Insert Statement:** The Oracle `INSERT INTO ... SELECT` statement will be converted to BigQuery SQL syntax.
    *   The `/*+ PARALLEL(c,4) */` hint will be removed as BigQuery automatically handles parallelization.
    *   Table names will be fully qualified with project and dataset (e.g., `project.source_cds.cds_ta_cntrct`).
    *   Date conversions like `TO_DATE('&v_datum','YYYYMMDD')` will be replaced with BigQuery's `PARSE_DATE` or by using the `v_datum` `DATE` variable directly, e.g., `DATE(c.insert_at) <= v_datum`.
    *   `NVL` will be replaced with `IFNULL` if necessary (though in the provided SQL, `NVL` is used for `TO_CHAR` output, which will be handled by the `DATE` cast and `IFNULL` for the `MAX`).
*   **DB Link (`&v_carmen`):** The reference to `@pcrs1` for `cds$ta_cntrct` signifies a cross-database query in Oracle. In BigQuery, this will mean that `cds$ta_cntrct` must be explicitly loaded into BigQuery (`project.source_cds.cds_ta_cntrct`) prior to this job's execution.

## 6. External Dependencies

*   **Oracle Database (Carmen DB):** The source `cds$ta_cntrct` table resides in an external Oracle database, accessed via a DB link (`@pcrs1`).
    *   **Replacement:** This data will need to be extracted from the Oracle Carmen DB and ingested into BigQuery, likely through a batch load process (e.g., using Dataflow, Fivetran, or a custom ingestion pipeline) into `project.source_cds.cds_ta_cntrct`.
*   **`isbert_schema.dwtk_meldungen` (Oracle Table):** This table is used to determine the `v_datum`.
    *   **Replacement:** This table will be migrated to BigQuery as `project.isbert_schema.dwtk_meldungen`.
*   **`DWPA_UTIL_SKRIPT` (Oracle Package):** This package is used for the `TRUNCATE TABLE` command.
    *   **Replacement:** The specific functionality used (truncating a table) will be directly translated into a BigQuery `TRUNCATE TABLE` DDL statement. If the package has other complex logic, it would need to be re-implemented as BigQuery stored procedures or functions.
*   **Utility KornShell Scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):** These provide environmental setup, error handling, date functions, parameter parsing, and SQL*Plus routines.
    *   **Replacement:** Their functionalities will be integrated into the BigQuery stored procedures using BigQuery's native procedural capabilities.
*   **`../trace.sql.cfg`:** An external configuration file for SQL tracing.
    *   **Replacement:** This tracing mechanism will be replaced by BigQuery's built-in logging and monitoring features, or by custom logging within the BigQuery stored procedures.

## 7. Unresolved / Risks

*   **Missing File Complexity Data:** The `file_complexity` table returned no rows, so the actual complexity tier and migration flags for both the KSH and SQL scripts are unknown. This could hide specific migration challenges.
*   **`VIA` Table:** The `lineage_edges` indicated `d_ausd_v_ta_cntrct_crs.sql` `WRITES_TABLE` `VIA`, but the SQL script itself only explicitly `INSERT`s into `sof$ta_cntrct_crs`. The nature and usage of this `VIA` target table need further investigation. It could be a view, or an implicit write, or handled by the `DWPA_UTIL_SKRIPT` package.
*   **Comprehensive `DWPA_UTIL_SKRIPT` Functionality:** Only the `runstatement` call for `TRUNCATE` was observed. If `DWPA_UTIL_SKRIPT` has other critical functionalities, these need to be fully identified and migrated.
*   **`r_ausd_v_ta_cntrct_crs.ksh` Invocation:** The seed script `k_ausd_v_ta_cntrct_crs.ksh` is invoked by `r_ausd_v_ta_cntrct_crs.ksh`. This upstream dependency should be considered for a complete migration chain, ensuring the caller is also migrated or can correctly invoke the new BigQuery job.
*   **Oracle-specific SQL Features:** While the `hql_sql_to_bqsql_design` tool covers common SQL features, highly specific Oracle functions or advanced PL/SQL constructs (beyond what was visible in `d_ausd_v_ta_cntrct_crs.sql`) might require manual intervention or specific BigQuery equivalents.

## 8. Build Plan

1.  **Migrate Source Data:**
    *   Ingest `isbert_schema.dwtk_meldungen` into `project.isbert_schema.dwtk_meldungen` in BigQuery.
    *   Ingest `cds$ta_cntrct` from the Oracle Carmen DB into `project.source_cds.cds_ta_cntrct` in BigQuery.
    *   **(Output:** BigQuery DDL for tables, Dataflow/Fivetran/custom ingestion scripts in Python/Java).

2.  **Create BigQuery Control Tables:**
    *   Generate DDL for `project.job_control.job_table`, `project.job_control.error_log`, and `project.job_control.job_result_log`.
    *   **(Output:** BigQuery DDL files).

3.  **Create Target Staging Table:**
    *   Generate DDL for `project.staging.sof_ta_cntrct_crs`.
    *   **(Output:** BigQuery DDL file).

4.  **Develop BigQuery SQL Stored Procedure for Data Transformation:**
    *   Translate `d_ausd_v_ta_cntrct_crs.sql` into `project.staging.d_ausd_v_ta_cntrct_crs` BigQuery Stored Procedure, incorporating the identified BigQuery conversion rules.
    *   **(Output:** BigQuery SQL script for stored procedure `project.staging.d_ausd_v_ta_cntrct_crs`).

5.  **Develop BigQuery SQL Stored Procedure for Orchestration:**
    *   Translate `k_ausd_v_ta_cntrct_crs.ksh` into `project.job_control.r_ausd_vertrag_control` BigQuery Stored Procedure, implementing parameter validation, job tracking, and calling the data transformation procedure.
    *   **(Output:** BigQuery SQL script for stored procedure `project.job_control.r_ausd_vertrag_control`).

6.  **Develop Orchestration DAG (Cloud Composer/Workflows):**
    *   Create a Cloud Composer DAG (Python) or Cloud Workflow definition (YAML) to schedule and invoke the `project.job_control.r_ausd_vertrag_control` BigQuery Stored Procedure.
    *   **(Output:** Python DAG file or YAML Workflow definition).

7.  **Testing:**
    *   Unit tests for individual BigQuery stored procedures.
    *   Integration tests for the entire workflow orchestrated by Cloud Composer/Workflows.
    *   Data validation to ensure output matches legacy system.
    *   **(Output:** Test scripts/framework).