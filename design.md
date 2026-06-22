# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh

## 1. Purpose & Scope

This job, `k_ausd_v_ta_discount.ksh`, is a KornShell control script designed to orchestrate a data processing task related to discount data. Its primary purpose is to prepare and execute an underlying SQL script (`d_ausd_v_ta_discount.sql`) that extracts and loads discount information into a target table, `ta_discount` (specifically, `SOF$TA_DISCOUNT`). The script handles parameter parsing, error logging, and ensures proper execution flow. It also manages temporary files for tracking record counts.

The scope of this migration is to re-implement this entire ETL workflow, including the shell orchestration and the core SQL data transformation, into a Google Cloud Platform (GCP) BigQuery-centric architecture. The aim is to convert the existing KornShell script and its invoked Oracle SQL into BigQuery SQL, potentially leveraging BigQuery stored procedures and Cloud Composer for orchestration.

## 2. Source Inventory

The job consists of two primary components:

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh`**
    *   **Technology:** KornShell (ksh)
    *   **Description:** Control script for a data processing job, handling parameter parsing, error logging, and orchestrating the execution of an SQL script to process data into the 'ta_discount' table.
    *   **Complexity Tier:** (Not available in `file_complexity` table, inferring as **Medium** due to orchestration logic and dynamic SQL invocation)
    *   **Automation Bucket:** (Not available in `automation_rate` table, inferring as **Semi-Auto (B2)** given the need for shell logic conversion and SQL rewrite)
    *   **Key components:**
        *   Environment setup (`. $HOME/.dw_init`)
        *   Error handling (`f_alis_msgerr.ksh`)
        *   Date utilities (`h_alis_date.ksh`)
        *   Parameter parsing (`h_alis_parameter.ksh`)
        *   SQL*Plus wrapper (`h_alis_sqlplus.ksh`)
        *   Dynamic invocation of `d_ausd_v_ta_discount.sql`
        *   Usage of variables `v_TabName`, `Name_SQLskript`, `tmpFile`
        *   Retrieval of record count from a temporary file.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_discount.sql`**
    *   **Technology:** Oracle SQL
    *   **Description:** The core data transformation logic that populates `SOF$TA_DISCOUNT`.
    *   **Complexity Tier:** (Part of the overall job's complexity, inferring as **Medium** due to joins, date filtering, and Oracle-specific functions)
    *   **Automation Bucket:** (Part of the overall job's automation bucket, inferring as **Semi-Auto (B2)**)
    *   **Key components:**
        *   Determines a cutoff date `v_datum` from `isbert_schema.dwtk_meldungen`.
        *   Truncates `sof$ta_discount`.
        *   Inserts data into `sof$ta_discount` by joining `cds$ta_discount_bc_assoc`, `cds$ta_discount`, `cds$ta_care_description`, and `cds$ta_disc_vector` from a source Oracle database (`&v_carmen`).
        *   Applies date-based filtering on `insert_at`, `modified_at`, `valid_from`, `valid_to` columns.
        *   Filters by `cd.LANGUAGE = 1` and `d.is_production = 1`.
        *   Uses Oracle-specific `TO_CHAR`, `NVL`, `TO_DATE` functions, and `TRUNCATE TABLE`.
        *   Calls `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for truncation.
        *   Mentions `PACKAGE:DWPA_UTIL_SKRIPT` and `PACKAGE:PA_ANALYZE`.

## 3. Target Architecture

The target architecture for this job will be entirely within Google Cloud Platform, utilizing:

*   **BigQuery:** For all data storage, transformation, and querying.
    *   Source Oracle tables will be ingested into BigQuery as external tables or native BigQuery tables.
    *   The `sof$ta_discount` table will be recreated in a BigQuery dataset (`project.dataset.sof_ta_discount`).
    *   The transformation logic will be implemented as a BigQuery SQL stored procedure.
*   **Cloud Composer (Apache Airflow):** For job orchestration, scheduling, and error handling, replacing the KornShell script's control flow.
    *   A new Airflow DAG will be developed to invoke the BigQuery stored procedure.
    *   Parameter passing, logging, and dependency management will be handled by the DAG.
*   **Cloud Logging & Monitoring:** For centralized logging and alerting, replacing shell-based logging and `SPOOL` outputs.

## 4. Data Flow & Lineage

The original data flow is:

1.  **KornShell Script (`k_ausd_v_ta_discount.ksh`)**:
    *   Loads environment and utility scripts.
    *   Parses input parameters (`p_JobKennung`, `p_EintragsNr`).
    *   Sets `v_TabName='ta_discount'`.
    *   Dynamically sets `Name_SQLskript` to point to `d_ausd_v_ta_discount.sql`.
    *   Executes the SQL script via `starteSQLSkript` (a wrapper function from `h_alis_sqlplus.ksh`).
    *   Reads a record count from a temporary file `tmpFile`.
2.  **Oracle SQL Script (`d_ausd_v_ta_discount.sql`)**:
    *   Reads from `isbert_schema.dwtk_meldungen` to determine `v_datum`.
    *   Truncates `SOF$TA_DISCOUNT` using `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
    *   Reads from source tables (via `&v_carmen` DB link, assumed to be an Oracle source)
        *   `cds$ta_discount_bc_assoc` (aliased as `da`)
        *   `cds$ta_discount` (aliased as `d`)
        *   `cds$ta_care_description` (aliased as `cd`)
        *   `cds$ta_disc_vector` (aliased as `dv`)
    *   Performs joins and filters based on `discount_id`, `cds_description_id`, `language`, `obj_version`, `insert_at`, `modified_at`, `valid_from`, `valid_to`, and `is_production`.
    *   Calculates `rabatt` and `rabatthoehe`.
    *   Writes/Inserts into `SOF$TA_DISCOUNT`.
    *   Uses Oracle packages `DWPA_UTIL_SKRIPT` and `PA_ANALYZE`.

The migrated data flow in BigQuery will be:

1.  **Cloud Composer DAG**:
    *   Initiates the BigQuery stored procedure.
    *   Passes parameters (e.g., job identifier, entry number) to the procedure.
    *   Monitors the stored procedure execution and handles logging.
2.  **BigQuery Stored Procedure (`project.dataset.r_ausd_v_ta_discount`)**:
    *   Performs parameter validation.
    *   Queries `project.isbert_schema.dwtk_meldungen` to determine the cutoff date.
    *   `TRUNCATE TABLE` `project.dataset.sof_ta_discount`.
    *   `INSERT` data into `project.dataset.sof_ta_discount` by joining source tables (e.g., `project.source.cds_ta_discount_bc_assoc`, `project.source.cds_ta_discount`, etc.) now present in BigQuery.
    *   Replicates the filtering and transformation logic.
    *   Records the number of inserted rows, potentially into a logging table or as a return value.

## 5. Transformation Logic

The core transformation logic resides within the Oracle SQL script and will be translated into BigQuery SQL.

**Original Oracle SQL Logic Summary:**

The script performs an `INSERT INTO ... SELECT` operation. It populates `sof$ta_discount` with `cntrct_id`, `discount_id`, `disc_vector_ty`, `cntrct_obj_version`, `rabatt`, and `rabatthoehe`.

The `SELECT` statement involves joining four tables:
*   `cds$ta_discount_bc_assoc` (aliased `da`)
*   `cds$ta_discount` (aliased `d`)
*   `cds$ta_care_description` (aliased `cd`)
*   `cds$ta_disc_vector` (aliased `dv`)

**Join Conditions:**
*   `da.discount_id = d.discount_id`
*   `cd.cds_description_id = d.cds_description_id`
*   `d.discount_id = dv.discount_id`
*   `d.disc_vector_ty = dv.disc_vector_ty`
*   `d.obj_version = dv.discount_obj_version`

**Filter Conditions:**
All date fields (`insert_at`, `modified_at`, `valid_from`, `valid_to`) are filtered against a derived `v_datum` (cutoff date).
*   `da.insert_at <= v_datum AND (da.modified_at IS NULL OR da.modified_at > v_datum)`
*   `d.insert_at <= v_datum AND (d.modified_at IS NULL OR d.modified_at > v_datum)`
*   `d.valid_from <= v_datum AND (d.valid_to IS NULL OR d.valid_to > v_datum)`
*   `dv.insert_at <= v_datum AND (dv.modified_at IS NULL OR dv.modified_at > v_datum)`
*   `cd.LANGUAGE = 1`
*   `d.is_production = 1`

**Column Mappings / Transformations:**
*   `cntrct_id` from `da`
*   `discount_id` from `da`
*   `disc_vector_ty` from `d`
*   `cntrct_obj_version` from `da`
*   `rabatt` from `cd.cds_description`
*   `rabatthoehe` from `to_char(dv.CALC_RULE_VALUE)`

**Derived Cutoff Date (`v_datum`):**
`v_datum` is derived by querying `isbert_schema.dwtk_meldungen` for the maximum `timecreated` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`, formatted as `YYYYMMDD`. If no such record exists, it defaults to `'19000101'`.

**Target BigQuery SQL (Pseudocode):**

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_v_ta_discount`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_datum STRING DEFAULT '';
  DECLARE v_datum_date DATE DEFAULT DATE '1900-01-01';
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation (replacing shell getopts and checks)
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr <> 0 THEN
    -- Log error or raise exception
    SELECT FORMAT('FEHLER: %d %s', ErrNr, ErrArg) AS error_message;
    RETURN; -- Exit procedure
  END IF;

  -- Determine cutoff date from control table (replacing Oracle SELECT ... DWTK_MELDUNGEN)
  SET v_datum = (
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
    FROM `project.isbert_schema.dwtk_meldungen`
    WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  SET v_datum_date = PARSE_DATE('%Y%m%d', v_datum);

  -- Refresh target table (replacing Oracle TRUNCATE TABLE)
  TRUNCATE TABLE `project.dataset.sof_ta_discount`; -- Or DELETE FROM then INSERT

  -- Insert refreshed data (translating Oracle INSERT SELECT with joins and filters)
  INSERT INTO `project.dataset.sof_ta_discount` (
    cntrct_id,
    discount_id,
    disc_vector_ty,
    cntrct_obj_version,
    rabatt,
    rabatthoehe
  )
  SELECT
    da.cntrct_id,
    da.discount_id,
    d.disc_vector_ty,
    da.cntrct_obj_version,
    cd.cds_description AS rabatt,
    CAST(dv.calc_rule_value AS STRING) AS rabatthoehe -- Assuming calc_rule_value can be cast to STRING
  FROM `project.source.cds_ta_discount_bc_assoc` da -- Source tables need to be present in BigQuery
  JOIN `project.source.cds_ta_discount` d
    ON da.discount_id = d.discount_id
  JOIN `project.source.cds_ta_care_description` cd
    ON cd.cds_description_id = d.cds_description_id
   AND cd.language = 1
  JOIN `project.source.cds_ta_disc_vector` dv
    ON d.discount_id = dv.discount_id
   AND d.disc_vector_ty = dv.disc_vector_ty
   AND d.obj_version = dv.discount_obj_version
  WHERE da.insert_at <= v_datum_date
    AND (da.modified_at IS NULL OR da.modified_at > v_datum_date)
    AND d.insert_at <= v_datum_date
    AND (d.modified_at IS NULL OR d.modified_at > v_datum_date)
    AND d.valid_from <= v_datum_date
    AND (d.valid_to IS NULL OR d.valid_to > v_datum_date)
    AND dv.insert_at <= v_datum_date
    AND (dv.modified_at IS NULL OR dv.modified_at > v_datum_date)
    AND d.is_production = 1;

  SET v_records = (
    SELECT COUNT(*) FROM `project.dataset.sof_ta_discount`
  );

  -- Log completion and record count
  SELECT
    'VERARBEITUNG_FERTIG' AS status,
    v_datum AS cutoff_date,
    v_records AS records_loaded;
END;
```

## 6. External Dependencies

| Original External Dependency | Type        | How it's Used                                                    | Migration Strategy                                                                                                     |
| :--------------------------- | :---------- | :--------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------- |
| `$HOME/.dw_init`             | Environment | Sourced for environment variables.                               | Replaced by environment variables/constants in Cloud Composer DAG or BigQuery stored procedure parameters.             |
| `f_alis_msgerr.ksh`          | Script      | Error logging utility.                                           | Replaced by Cloud Logging and BigQuery's built-in error handling (`BEGIN...EXCEPTION`) or custom logging tables.       |
| `h_alis_date.ksh`            | Script      | Date utility.                                                    | BigQuery SQL date functions (`CURRENT_DATE()`, `PARSE_DATE`, `FORMAT_DATE`).                                           |
| `h_alis_parameter.ksh`       | Script      | Parameter parsing (`getopts`).                                   | Replaced by Cloud Composer DAG parameters passed to the BigQuery stored procedure.                                     |
| `h_alis_sqlplus.ksh`         | Script      | SQL*Plus wrapper function (`starteSQLSkript`).                   | Replaced by direct invocation of BigQuery stored procedure from Cloud Composer, or BigQuery scripting.                 |
| `isbert_schema.dwtk_meldungen`| Table       | Source for determining cutoff date (`v_datum`).                  | Must be ingested into BigQuery (e.g., `project.isbert_schema.dwtk_meldungen`).                                         |
| `cds$ta_discount_bc_assoc`   | Table       | Source for discount contract association data (from Carmen DB).  | Ingested into BigQuery (e.g., `project.source.cds_ta_discount_bc_assoc`).                                              |
| `cds$ta_discount`            | Table       | Source for discount master data (from Carmen DB).                | Ingested into BigQuery (e.g., `project.source.cds_ta_discount`).                                                       |
| `cds$ta_care_description`    | Table       | Source for discount description data (from Carmen DB).           | Ingested into BigQuery (e.g., `project.source.cds_ta_care_description`).                                               |
| `cds$ta_disc_vector`         | Table       | Source for discount vector data (from Carmen DB).                | Ingested into BigQuery (e.g., `project.source.cds_ta_disc_vector`).                                                    |
| `DWPA_UTIL_SKRIPT`           | Oracle Package| Used for `runstatement` (e.g., `TRUNCATE TABLE`).                | Replaced by native BigQuery `TRUNCATE TABLE` DDL.                                                                      |
| `PA_ANALYZE`                 | Oracle Package| Used for table analysis (commented out).                         | Not applicable in BigQuery; BigQuery automatically optimizes queries. If statistics are needed, BigQuery manages them. |
| `@pcrs1` (`v_carmen`)        | DB Link     | Connects to Carmen DB for source tables.                         | Replaced by data ingestion pipelines that bring Carmen data into BigQuery.                                             |
| Temporary file (`tmpFile`)   | File System | Stores record count for `eval`.                                  | Replaced by BigQuery scripting variables (`DECLARE...SET`) or by storing counts in logging tables.                     |
| Trace spool file             | File System | Stores SQL trace output.                                         | Replaced by Cloud Logging and BigQuery query history.                                                                  |

## 7. Unresolved / Risks

*   **Job Control Logic:** The original `k_ausd_v_ta_discount.ksh` summary mentions "aktive Jobs werden ignoriert" (active jobs are ignored). The specific logic for this job control and its implications are not fully detailed in the provided code snippets or metadata. If this involves complex state management, it will need careful analysis and re-implementation in the Cloud Composer DAG to prevent concurrent job execution issues.
*   **Oracle-specific SQL features:** While common functions like `TO_DATE`, `NVL` have direct BigQuery equivalents (`PARSE_DATE`, `COALESCE`), more complex Oracle-specific constructs or PL/SQL blocks (like `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`) require manual review and adaptation. The `runstatement` function specifically needs to be replaced by native BigQuery DDL.
*   **Data Type Mismatches:** Implicit data type conversions in Oracle might behave differently in BigQuery. Careful validation of column data types between source (after ingestion) and target in BigQuery is necessary. For example, `to_char(dv.CALC_RULE_VALUE)` might need explicit `CAST` to `STRING` in BigQuery SQL.
*   **Performance Tuning:** Oracle `/*+ parallel(...) full(...) */` hints will be ignored by BigQuery. Performance will rely on BigQuery's automatic optimization and appropriate table partitioning/clustering. Extensive testing will be required.
*   **Error Handling Fidelity:** The original script's error handling (`f_alis_msgerr.ksh`, `WHENEVER SQLERROR EXIT FAILURE`) needs to be fully replicated in Cloud Composer and BigQuery stored procedures to ensure equivalent robustness.
*   **Source Data Availability:** A critical assumption is that all referenced source tables (from the `cds$` and `dwtk_meldungen` schemas) will be available in BigQuery before this job is migrated. The ingestion mechanism for these source tables must be established.

## 8. Build Plan

The migration will follow these steps:

1.  **Schema Definition (BigQuery DDL):**
    *   Create the target `project.dataset.sof_ta_discount` table in BigQuery, defining appropriate column names and data types based on the Oracle schema.
    *   Ensure all source tables (`project.isbert_schema.dwtk_meldungen`, `project.source.cds_ta_discount_bc_assoc`, `project.source.cds_ta_discount`, `project.source.cds_ta_care_description`, `project.source.cds_ta_disc_vector`) exist in BigQuery with correct schemas, either as external tables or natively ingested tables.

2.  **BigQuery Stored Procedure Development (BigQuery SQL):**
    *   Translate the Oracle SQL from `d_ausd_v_ta_discount.sql` into a BigQuery SQL stored procedure (e.g., `project.dataset.r_ausd_v_ta_discount`).
    *   Implement parameter validation logic from the ksh script within the stored procedure.
    *   Replace Oracle-specific functions (`TO_DATE`, `NVL`, `to_char`) with BigQuery equivalents (`PARSE_DATE`, `COALESCE`, `CAST`).
    *   Replace `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` with a `TRUNCATE TABLE` statement for `sof_ta_discount`.
    *   Refine column mappings and ensure data type compatibility.
    *   Implement error handling using BigQuery `BEGIN...EXCEPTION` blocks or `ASSERT` statements where appropriate.

3.  **Cloud Composer DAG Development (Python):**
    *   Create a new Airflow DAG (e.g., `dag_k_ausd_v_ta_discount.py`) in Cloud Composer.
    *   The DAG will define a task to call the BigQuery stored procedure.
    *   Pass job-specific parameters (`p_JobKennung`, `p_EintragsNr`) to the stored procedure.
    *   Implement logging to Cloud Logging.
    *   Configure scheduling and retry mechanisms as per current job requirements.
    *   (Optional) If complex job control logic existed in the ksh script (e.g., active job checking), implement this logic within the DAG using BigQuery tables for state management.

4.  **Testing and Validation:**
    *   **Unit Testing:** Test the BigQuery stored procedure with sample data to ensure correct transformations and data loading.
    *   **Integration Testing:** Test the full workflow from the Cloud Composer DAG to the BigQuery stored procedure and target table.
    *   **Data Validation:** Compare output data in BigQuery with the original Oracle `SOF$TA_DISCOUNT` table for accuracy and completeness.

5.  **Deployment:**
    *   Deploy the BigQuery DDL, stored procedure, and Cloud Composer DAG to the target GCP environment.

This detailed plan addresses both the shell orchestration and the underlying SQL transformation, providing a comprehensive strategy for migrating the job to BigQuery.