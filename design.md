# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh

## 1. Purpose & Scope
This migration targets a KornShell-orchestrated Oracle SQL job responsible for reconciling and updating contract discount data into the `sof$ta_p_discount` table. The job involves a wrapper script (`r_ausd_v_ta_p_discount.ksh`), a core execution script (`k_ausd_v_ta_p_discount.ksh`), and an Oracle SQL script (`d_ausd_v_ta_p_discount.sql`). The primary business purpose is to ensure the `sof$ta_p_discount` table contains current and reconciled discount information derived from `sof$ta_disc_zusgf` and `sof$ta_cntrct_crs` source tables, with a processing date determined from a control table `dwtk_meldungen`. The scope of this migration is to re-platform this entire ETL workflow from its current KornShell/Oracle environment to Google Cloud's BigQuery platform.

## 2. Source Inventory

| File Name                                                                 | Technology           | Role                       | Complexity Tier (Inferred) | Automation Bucket (Inferred) |
| :------------------------------------------------------------------------ | :------------------- | :------------------------- | :------------------------- | :--------------------------- |
| `r_ausd_v_ta_p_discount.ksh`                                            | KornShell            | Wrapper / Orchestration    | Medium                     | Semi-Auto (B2)               |
| `k_ausd_v_ta_p_discount.ksh`                                            | KornShell            | Core Orchestration         | Medium                     | Semi-Auto (B2)               |
| `d_ausd_v_ta_p_discount.sql`                                            | Oracle SQL/PLSQL     | Data Transformation        | Medium                     | Semi-Auto (B2)               |
| `f_alis_msgerr.ksh` (utility)                                           | KornShell            | Error Handling / Logging   | Simple                     | Redesign (B4)                |
| `h_alis_parameter.ksh` (utility)                                        | KornShell            | Parameter Processing       | Simple                     | Redesign (B4)                |
| `h_alis_date.ksh` (utility)                                             | KornShell            | Date Utilities             | Simple                     | Redesign (B4)                |
| `h_alis_sqlplus.ksh` (utility)                                          | KornShell            | SQL*Plus Execution Wrapper | Simple                     | Redesign (B4)                |

*(Complexity tiers and automation buckets are inferred as database metadata was not available for these attributes.)*

## 3. Target Architecture
The migrated solution will reside within Google BigQuery, leveraging its native SQL capabilities and stored procedures for orchestration and data transformation.
- **Orchestration:** The KornShell scripts will be re-engineered into a BigQuery Stored Procedure or an Airflow DAG orchestrated by Cloud Composer. The current analysis suggests a BigQuery Stored Procedure is a direct equivalent.
- **Data Transformation:** The Oracle SQL will be converted to BigQuery Standard SQL, executed within the BigQuery Stored Procedure.
- **Data Storage:** All source tables (`sof$ta_disc_zusgf`, `sof$ta_cntrct_crs`, `dwtk_meldungen`) and the target table (`sof$ta_p_discount`) will be migrated to BigQuery tables.
- **Logging & Monitoring:** The existing shell-based logging and error handling framework will be replaced by BigQuery logging tables and potentially integrated with Cloud Logging and Monitoring.
- **Parameter Handling:** Shell script parameters will be translated to BigQuery Stored Procedure input parameters.

## 4. Data Flow & Lineage
The data flow of the migrated job will be as follows:

1.  **Trigger:** An external scheduler (e.g., Cloud Composer/Airflow) invokes the main BigQuery Stored Procedure.
2.  **Parameter Processing:** The Stored Procedure receives `JobKennung` and `EintragsNr` as input.
3.  **Logging & Error Handling:** Initial job entry is recorded in a BigQuery logging table. Error conditions are handled via `EXCEPTION WHEN ERROR` blocks and logged.
4.  **Date Derivation:** The procedure queries `project.dataset.dwtk_meldungen` (migrated BigQuery table) to determine a processing date (`v_datum`).
5.  **Target Table Preparation:** The procedure executes `TRUNCATE TABLE project.dataset.sof_ta_p_discount` (migrated BigQuery table).
6.  **Data Transformation:** The procedure performs an `INSERT INTO ... SELECT` operation:
    *   Reads from `project.dataset.sof_ta_disc_zusgf` (migrated BigQuery table).
    *   Reads from `project.dataset.sof_ta_cntrct_crs` (migrated BigQuery table).
    *   Joins these two tables on `cntrct_id` and `cntrct_obj_version`.
    *   Inserts the selected, reconciled data into `project.dataset.sof_ta_p_discount`.
7.  **Record Count & Audit:** The number of inserted rows (`@@row_count`) is captured and recorded in an audit table.
8.  **Final Status Update:** The job's final status (OK or FAILED) is updated in the BigQuery logging table.

## 5. Transformation Logic
The core transformation logic resides within `d_ausd_v_ta_p_discount.sql` and will be directly translated to BigQuery Standard SQL.

**Original Oracle SQL Logic:**
```sql
-- Determine processing date
SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
  FROM isbert_schema.dwtk_meldungen m
 WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

-- Truncate target table
begin
  isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_p_discount');
end;
/

-- Insert reconciled data
INSERT  INTO sof$ta_p_discount(
        cntrct_id,
        disc_vector_ty,
        cntrct_obj_version,
        rabatt_alle,
        contract_number)
SELECT  /*+ parallel(da,4) parallel(c,4) */
        da.cntrct_id,
        da.disc_vector_ty,
        da.cntrct_obj_version,
        da.rabatt_alle,
        c.contract_number
FROM
        sof$ta_disc_zusgf da,
        sof$ta_cntrct_crs c
WHERE
        da.cntrct_id            = c.cntrct_id
AND     da.cntrct_obj_version   = c.obj_version;
```

**Migrated BigQuery Standard SQL Logic (within a Stored Procedure):**
```sql
DECLARE v_datum STRING;
DECLARE v_records INT64;

-- Derive v_datum from latest BERT_DROP_TEMP_TABLE entry
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(timecreated))), '19000101')
  FROM `project.dataset.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Truncate target table
TRUNCATE TABLE `project.dataset.sof_ta_p_discount`;

-- Insert reconciled rows
INSERT INTO `project.dataset.sof_ta_p_discount`
  (cntrct_id, disc_vector_ty, cntrct_obj_version, rabatt_alle, contract_number)
SELECT
  da.cntrct_id,
  da.disc_vector_ty,
  da.cntrct_obj_version,
  da.rabatt_alle,
  c.contract_number
FROM `project.dataset.sof_ta_disc_zusgf` AS da
JOIN `project.dataset.sof_ta_cntrct_crs` AS c
  ON da.cntrct_id = c.cntrct_id
 AND da.cntrct_obj_version = c.obj_version;

SET v_records = @@row_count;
```
The Oracle-specific `/*+ parallel(...) */` hints will be removed as BigQuery handles parallelism automatically. The `DEFINE` variable and `COLUMN new_value` for `v_datum` will be replaced by a `DECLARE` statement and direct assignment within the BigQuery Stored Procedure. The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` PL/SQL call for `TRUNCATE` will be replaced by a direct `TRUNCATE TABLE` statement in BigQuery.

## 6. External Dependencies
The original job has the following external dependencies:

*   **Oracle Database:** All source tables (`sof$ta_disc_zusgf`, `sof$ta_cntrct_crs`, `dwtk_meldungen`) and the target table (`sof$ta_p_discount`) reside in an Oracle database.
    *   **Migration Plan:** These Oracle tables must be migrated to BigQuery tables. This typically involves a data ingestion pipeline (e.g., Data Migration Service, Striim, custom ETL) to replicate data from Oracle to BigQuery, establishing one-time or continuous synchronization.
*   **Oracle DB-Link (`@pcrs1`):** The `d_ausd_v_ta_p_discount.sql` script references `DEFINE v_carmen = "@pcrs1"`, indicating a database link to `pcrs1`.
    *   **Migration Plan:** The concept of DB-Links does not directly apply in BigQuery. All data required for the transformation must either be physically present in BigQuery tables or accessible via BigQuery's federated query capabilities (e.g., querying Cloud SQL for PostgreSQL or other supported external data sources). Assuming `pcrs1` refers to another Oracle instance, that data source would also need to be ingested into BigQuery.
*   **KornShell Utility Framework:** The job relies on a suite of KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`). These provide error handling, parameter parsing, date calculations, and SQL*Plus execution.
    *   **Migration Plan:** This entire framework needs to be redesigned for BigQuery.
        *   Error handling and logging (`f_alis_msgerr.ksh`) will be replaced by BigQuery logging tables and BigQuery's native error handling (`EXCEPTION WHEN ERROR`) within stored procedures, potentially integrated with Cloud Logging.
        *   Parameter handling (`h_alis_parameter.ksh`) will be handled by BigQuery Stored Procedure input parameters and internal validation logic.
        *   Date utilities (`h_alis_date.ksh`) will be replaced by BigQuery's rich set of date and time functions.
        *   SQL*Plus execution (`h_alis_sqlplus.ksh`) is no longer relevant as the SQL will run natively in BigQuery.

## 7. Unresolved / Risks
*   **Missing `file_complexity` and `automation_rate` data:** The migration planning was conducted without direct insight into the complexity tier or automation bucket from the database. Assessments were made based on code analysis. This poses a minor risk if the inferred complexity significantly deviates from the actual.
*   **`DW_DIR_UTL` and temporary file handling:** The creation and reading of `$DW_DIR_UTL/bert_k_ausd_v_ta_p_discount_$$.tmp` for record counts is a file-system operation not native to BigQuery.
    *   **Resolution:** This will be replaced by capturing `@@row_count` directly after the `INSERT` statement within the BigQuery Stored Procedure and storing it in an audit table.
*   **SQL*Plus `spool` and `trace.sql.cfg`:** The original script uses SQL*Plus `spool` and a `trace.sql.cfg` file for output and tracing.
    *   **Resolution:** This will be replaced by BigQuery audit logging, BigQuery's query history, and potentially dedicated BigQuery logging tables.
*   **Environment variables (`$HOME/.dw_init`, `BERT_DIR_ROOT`):** The shell scripts rely heavily on environment variables for configuration and path resolution.
    *   **Resolution:** These will need to be parameterized in the BigQuery Stored Procedure or managed via environment variables/secrets in the orchestration layer (e.g., Cloud Composer).
*   **Oracle-specific `NVL` and `TO_CHAR` functions:** While straightforward for migration, careful testing is needed to ensure BigQuery equivalents (e.g., `COALESCE`, `FORMAT_DATE`) yield identical results.
*   **`DWPA_UTIL_SKRIPT.runstatement`:** This is an Oracle PL/SQL utility.
    *   **Resolution:** It will be replaced by direct BigQuery Standard SQL statements (e.g., `TRUNCATE TABLE`).

## 8. Build Plan
The build plan will involve creating BigQuery resources and converting the existing logic.

1.  **Schema Migration:**
    *   Migrate the schemas of `sof$ta_p_discount`, `sof$ta_disc_zusgf`, `sof$ta_cntrct_crs`, and `dwtk_meldungen` to BigQuery.
    *   Define BigQuery tables (`project.dataset.sof_ta_p_discount`, `project.dataset.sof_ta_disc_zusgf`, `project.dataset.sof_ta_cntrct_crs`, `project.dataset.dwtk_meldungen`).
    *   Create BigQuery tables for logging and auditing: `project.dataset.job_log` and `project.dataset.job_error_log`.
2.  **Data Ingestion:**
    *   Establish a data ingestion pipeline to migrate historical and ongoing data from Oracle to the new BigQuery tables.
3.  **BigQuery Stored Procedure Development:**
    *   Develop a BigQuery Stored Procedure named `project.dataset.r_ausd_v_ta_p_discount` that encapsulates the logic from `r_ausd_v_ta_p_discount.ksh`, `k_ausd_v_ta_p_discount.ksh`, and `d_ausd_v_ta_p_discount.sql`.
    *   Implement parameter handling (for `JobKennung` and `EintragsNr`).
    *   Translate the Oracle SQL to BigQuery Standard SQL, replacing `TRUNCATE` calls and date derivation.
    *   Integrate BigQuery error handling (`EXCEPTION WHEN ERROR`).
    *   Replace file-based logging and record counting with inserts into `project.dataset.job_log` and `project.dataset.job_error_log`.
4.  **Orchestration Integration:**
    *   Create an Airflow DAG (in Cloud Composer) or define a Cloud Scheduler job to invoke the BigQuery Stored Procedure with necessary parameters.
    *   Configure retry mechanisms and alerts.
5.  **Testing:**
    *   Develop unit and integration tests to verify data accuracy and job functionality.
    *   Compare output data with the legacy system for reconciliation.
6.  **Deployment:**
    *   Deploy BigQuery tables, stored procedures, and orchestration components to the target environment.

**Generated Build Artifacts:**
*   `r_ausd_v_ta_p_discount.sql` (BigQuery Stored Procedure)
*   `create_table_sof_ta_p_discount.sql` (BigQuery DDL)
*   `create_table_sof_ta_disc_zusgf.sql` (BigQuery DDL for source)
*   `create_table_sof_ta_cntrct_crs.sql` (BigQuery DDL for source)
*   `create_table_dwtk_meldungen.sql` (BigQuery DDL for control table)
*   `create_table_job_log.sql` (BigQuery DDL for logging)
*   `create_table_job_error_log.sql` (BigQuery DDL for error logging)
*   `airflow_dag_r_ausd_v_ta_p_discount.py` (Python, if using Cloud Composer for orchestration)