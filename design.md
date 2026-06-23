# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh

## 1. Purpose & Scope
This job, identified by `run_id 5af228f1-3847-4cc6-9310-ed82ed19407c`, is assembled from 1 component and its direct dependencies. The primary purpose of the `k_ausd_bp_ta_bpr_instance.ksh` script is to orchestrate the execution of an SQL script for data preparation, handling parameter parsing, validation, and environment setup. It aims to extract and process base product instance data, ultimately populating a target table (`sof$ta_bpr_instance`). The overall process involves date calculation, parameter validation, and an Oracle SQL execution.

## 2. Source Inventory

The migration scope includes the following files:

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh**
    *   **Technology:** KornShell
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Summary:** Orchestrates the execution of an SQL script for data preparation, handling parameter parsing, validation, and environment setup.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_instance.sql**
    *   **Technology:** Oracle PL/SQL
    *   **Complexity Tier:** complex
    *   **Automation Bucket:** manual
    *   **Summary:** Prepares base product instance data by first truncating the target table and then loading it with filtered and joined data from contract and base product instance tables, utilizing a database link and date-based filtering.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh**
    *   **Technology:** KornShell
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** retire
    *   **Summary:** Calculates and formats today's date and yesterday's date, including handling month and year transitions and leap years, then prints them to standard output.

## 3. Target Architecture

The target platform is Google BigQuery. The migration will leverage BigQuery's native capabilities for data processing and orchestration.

*   **BigQuery Stored Procedures:** The KornShell orchestrator script (`k_ausd_bp_ta_bpr_instance.ksh`) will be migrated into a BigQuery Stored Procedure, encapsulating the parameter handling, date calculations, and the execution of the main data transformation logic.
*   **BigQuery SQL Script/Stored Procedure:** The Oracle PL/SQL script (`d_ausd_bp_ta_bpr_instance.sql`) will be converted into a BigQuery SQL script or a separate BigQuery Stored Procedure, performing the data loading and transformations.
*   **BigQuery Tables:** All source Oracle tables (`isbert_schema.dwtk_meldungen`, `cds$ta_cntrct`, `pds$ta_bpri_com`) and the target table (`sof$ta_bpr_instance`) will be migrated to BigQuery tables within appropriate datasets (e.g., `isbert_schema`, `dw_source_isrpt_isbert`). The `sof$ta_bpr_instance` table will likely be a partitioned and/or clustered table for performance.
*   **Data Ingestion:** Data from the legacy Oracle source systems for `cds$ta_cntrct`, `pds$ta_bpri_com`, and `isbert_schema.dwtk_meldungen` will need to be continuously ingested into BigQuery. This could be achieved using Cloud Data Fusion, Cloud Dataflow, or a direct database migration service. The `@pcrs1` database link implies a remote connection to a source system that will need to be handled during ingestion.
*   **Orchestration (Optional):** While the main orchestration logic will be within a BigQuery Stored Procedure, external scheduling (e.g., Cloud Composer/Airflow, Cloud Scheduler) might be used to trigger the top-level BigQuery Stored Procedure.

## 4. Data Flow & Lineage

The data flow for this job can be summarized as follows:

1.  **Parameter Acquisition:** The migrated BigQuery Stored Procedure for `k_ausd_bp_ta_bpr_instance.ksh` will accept parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
2.  **Date Calculation:** The functionality of `gestern.ksh` (calculating current and previous day's dates) will be replaced by native BigQuery date functions (`CURRENT_DATE()`, `DATE_SUB()`) directly within the orchestrating BigQuery Stored Procedure.
3.  **Parameter Validation:** The orchestrating Stored Procedure will perform validation checks on the input parameters (Job ID, Key Date, Entry Number) and date format using BigQuery SQL's conditional logic and error handling (`IF`, `ASSERT`).
4.  **Date Retrieval for SQL:** The main SQL transformation script will query `isbert_schema.dwtk_meldungen` in BigQuery to determine a `v_datum` (date) value, replacing the Oracle `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') ...` logic with `COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')`.
5.  **Data Transformation (Main SQL):** The migrated `d_ausd_bp_ta_bpr_instance.sql` logic (as a BigQuery SQL script or Stored Procedure) will:
    *   `TRUNCATE` the target BigQuery table `sof$ta_bpr_instance`.
    *   `INSERT` data into `sof$ta_bpr_instance` by joining `cds$ta_cntrct` and `pds$ta_bpri_com`. The join condition is `c.cntrct_id = bp.cntrct_id`.
    *   Filter conditions on `cds$ta_cntrct` include `cntrct_st IN (5, 6)`, `redundant_owner_id = 1`, `is_production = 1`, and date-based filtering using `v_datum`.
    *   Filter conditions on `pds$ta_bpri_com` include `is_production = 1` and date-based filtering using `v_datum`.
    *   String concatenations for `ICCID` will be converted to BigQuery `CONCAT` and `CAST` functions.
6.  **Record Count & Logging:** After the data transformation, the orchestrating BigQuery Stored Procedure will calculate the count of records in the target table (`sof$ta_bpr_instance`) and log this information, along with job status and parameters, into a dedicated BigQuery logging/control table.
7.  **Error Handling:** Error conditions (missing parameters, invalid date format) will trigger appropriate error messages and `SIGNAL SQLSTATE` to halt execution.

## 5. Transformation Logic

**5.1. `k_ausd_bp_ta_bpr_instance.ksh` (Orchestration Logic)**

The KornShell script's orchestration logic will be re-implemented as a BigQuery Stored Procedure.

*   **Parameter Handling:** The `getopts` logic will be replaced by direct input parameters to the BigQuery Stored Procedure.
*   **Date Calculation:** The calls to `gestern.ksh` and related date arithmetic will be replaced by BigQuery's `CURRENT_DATE()`, `DATE_SUB()`, `FORMAT_DATE()`, and `EXTRACT()` functions.
*   **Error Handling:** `pruefeParameterGesetzt`, `DWMSG_MeldeFehler`, and `DWDate_Datum_Check` will be translated into BigQuery `IF` statements and `ASSERT` or `SIGNAL SQLSTATE` to handle validation and errors.
*   **SQL Execution:** The `starteSQLSkript` call will be replaced by a direct call to the migrated BigQuery Stored Procedure for `d_ausd_bp_ta_bpr_instance.sql`.
*   **Record Count:** The temporary file logic (`cat $tmpFile`) will be replaced by a `SELECT COUNT(*)` on the target BigQuery table `sof$ta_bpr_instance`.
*   **Job Logging:** The commented-out `FOSJobErzeugeEintrag` will be implemented as an `INSERT` statement into a BigQuery control/logging table.

*BigQuery SQL Pseudocode for `k_ausd_bp_ta_bpr_instance.ksh` (from MCP output):*
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_bp_ta_bpr_instance`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE DEFAULT NULL;

  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET p_wiederanlaufWert = '0';
  END IF;

  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF ErrNr = 0 AND (p_Stichtag IS NULL OR p_Stichtag = '') THEN
    SET ErrNr = 1;
    SET ErrArg = 'Stichtag';
  END IF;

  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    SET ErrNr = 1;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr != 0 THEN
    SELECT FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg) AS error_message;
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Bitte ueber Rahmenscript aufrufen';
  END IF;

  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);

  IF v_stichtag_date IS NULL THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Datum hat ungueltiges Format DDMMYYYY';
  END IF;

  -- Core SQL logic migrated from d_ausd_bp_ta_bpr_instance.sql
  CALL `project.dataset.d_ausd_bp_ta_bpr_instance`(
    p_EintragsNr,
    p_JobKennung,
    p_Stichtag,
    p_wiederanlaufWert,
    v_datum_heute,
    v_datum_gestern
  );

  -- Example record count replacement for tmp file
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.sof$ta_bpr_instance` -- Assuming target table is named this in BQ
    WHERE DATE_FIELD = v_stichtag_date -- Placeholder, actual filter depends on target table schema
  );

  INSERT INTO `project.dataset.job_log`
  (
    tab_name,
    job_status,
    record_count,
    stichtag,
    created_at
  )
  VALUES
  (
    v_TabName,
    'A',
    v_records,
    v_stichtag_date,
    CURRENT_TIMESTAMP()
  );

  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
END;
```

**5.2. `d_ausd_bp_ta_bpr_instance.sql` (Data Transformation Logic)**

This Oracle PL/SQL script will be converted to a BigQuery SQL script or Stored Procedure.

*   **Variable Definition:** `DEFINE v_carmen = "@pcrs1"` indicates a database link. This will be replaced by ensuring data from `@pcrs1` (source system) is ingested into BigQuery. The `v_datum` derivation from `dwtk_meldungen` will be directly translated.
*   **Truncate Table:** `TRUNCATE TABLE sof$ta_bpr_instance REUSE STORAGE` will be a direct `TRUNCATE TABLE` statement in BigQuery.
*   **Data Insertion:** The `INSERT ... SELECT` statement will be converted, handling:
    *   Oracle `TO_CHAR`/`TO_DATE` functions replaced by BigQuery `FORMAT_DATE`/`PARSE_DATE`.
    *   Oracle string concatenation `||` replaced by BigQuery `CONCAT`.
    *   The `/*+ DRIVING_SITE(c) ORDERED FULL(c) FULL(bp) PARALLEL(c,4) PARALLEL(bp,4) */` hints are Oracle-specific and will be removed, relying on BigQuery's query optimizer.
*   **`DWPA_UTIL_SKRIPT.runstatement`:** This Oracle procedure call for truncating the table will be handled by BigQuery's `TRUNCATE TABLE` statement.
*   **`COMMIT`:** BigQuery DML operations are transactional by default, so explicit `COMMIT` is not needed.

*BigQuery SQL Equivalent for `d_ausd_bp_ta_bpr_instance.sql` (from MCP output):*
```sql
-- BigQuery SQL equivalent

DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` -- BigQuery table
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step01: truncate target table
TRUNCATE TABLE `sof$ta_bpr_instance`; -- BigQuery table

-- Step03: insert local basis product instances
INSERT INTO `sof$ta_bpr_instance`
(
  CNTRCT_ID,
  BPR_ID,
  BPR_INSTANCE_ID,
  ICCID,
  IMSI_MCC,
  IMSI_MNC,
  IMSI_HLR,
  IMSI_SI,
  CNTRCT_ID_REF
)
SELECT
  bp.cntrct_id,
  bp.bpr_id,
  bp.bpri_com_id AS bpr_instance_id,
  CONCAT(
    CAST(bp.iccid_mi AS STRING), '-',
    CAST(bp.iccid_ii AS STRING), '-',
    CAST(bp.iccid_iai AS STRING), '-',
    CAST(bp.iccid_nr AS STRING), '-',
    CAST(bp.iccid_cd AS STRING)
  ) AS iccid,
  bp.imsi_mcc,
  bp.imsi_mnc,
  bp.imsi_hlr,
  bp.imsi_si,
  bp.cntrct_id_ref
FROM `cds$ta_cntrct` c -- BigQuery table
JOIN `pds$ta_bpri_com` bp -- BigQuery table
  ON c.cntrct_id = bp.cntrct_id
WHERE c.cntrct_st IN (5, 6)
  AND c.redundant_owner_id = 1
  AND c.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (c.modified_at IS NULL OR c.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND c.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (c.valid_to IS NULL OR c.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND c.is_production = 1
  AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
  AND bp.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (bp.modified_at IS NULL OR bp.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND bp.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (bp.valid_to IS NULL OR bp.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND bp.is_production = 1;
```

**5.3. `gestern.ksh` (Date Calculation Logic)**

This KornShell script will be retired and its functionality absorbed directly into the orchestrating BigQuery Stored Procedure using native BigQuery date and time functions.

*BigQuery SQL Pseudocode for `gestern.ksh` (from MCP output):*
```sql
-- This logic will be integrated into the main orchestrating stored procedure.
DECLARE Var_Nummer_Heute_Tag INT64;
DECLARE Var_Nummer_Heute_Monat INT64;
DECLARE Var_Nummer_Heute_Jahr INT64;
DECLARE Var_Datum_Heute STRING;
DECLARE Var_Monat_Heute STRING;

DECLARE Var_Nummer_Gestern_Tag INT64;
DECLARE Var_Nummer_Gestern_Monat INT64;
DECLARE Var_Nummer_Gestern_Jahr INT64;
DECLARE Var_Datum_Gestern STRING;
DECLARE Var_Monat_Gestern STRING;

-- Datum ermitteln
SET Var_Nummer_Heute_Tag = EXTRACT(DAY FROM CURRENT_DATE());
SET Var_Nummer_Heute_Monat = EXTRACT(MONTH FROM CURRENT_DATE());
SET Var_Nummer_Heute_Jahr = EXTRACT(YEAR FROM CURRENT_DATE());

-- Calculate yesterday using native BigQuery date functions
DECLARE yesterday_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
SET Var_Nummer_Gestern_Tag = EXTRACT(DAY FROM yesterday_date);
SET Var_Nummer_Gestern_Monat = EXTRACT(MONTH FROM yesterday_date);
SET Var_Nummer_Gestern_Jahr = EXTRACT(YEAR FROM yesterday_date);

-- Format dates
SET Var_Datum_Heute = CONCAT(
  CAST(Var_Nummer_Heute_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Heute_Monat AS STRING), 2, '0'),
  LPAD(CAST(Var_Nummer_Heute_Tag AS STRING), 2, '0')
);

SET Var_Monat_Heute = CONCAT(
  CAST(Var_Nummer_Heute_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Heute_Monat AS STRING), 2, '0')
);

SET Var_Datum_Gestern = CONCAT(
  CAST(Var_Nummer_Gestern_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Gestern_Monat AS STRING), 2, '0'),
  LPAD(CAST(Var_Nummer_Gestern_Tag AS STRING), 2, '0')
);

SET Var_Monat_Gestern = CONCAT(
  CAST(Var_Nummer_Gestern_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Gestern_Monat AS STRING), 2, '0')
);

-- The output of these variables will be used within the main stored procedure.
```

## 6. External Dependencies

*   **Oracle Database Link (`@pcrs1`):** The `d_ausd_bp_ta_bpr_instance.sql` script makes use of an Oracle database link indicated by `&v_carmen = "@pcrs1"` and the `DRIVING_SITE(c)` hint. This implies that the source tables (`cds$ta_cntrct` and `pds$ta_bpri_com`) are located in a remote Oracle instance.
    *   **Replacement Strategy:** Data from these source Oracle tables must be continuously ingested into BigQuery. This can be achieved through:
        *   **Cloud Data Fusion:** Creating pipelines to extract data from the Oracle source and load it into BigQuery.
        *   **Cloud Dataflow:** Custom Dataflow jobs for more complex ingestion logic.
        *   **Database Migration Service (DMS):** For one-time or continuous replication of Oracle databases to BigQuery.
*   **Environment Variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`):** These are used in `k_ausd_bp_ta_bpr_instance.ksh` for paths and environment setup.
    *   **Replacement Strategy:** These will be replaced by:
        *   **BigQuery Stored Procedure Parameters:** For values that can change per execution (e.g., specific dates if not derived from `CURRENT_DATE()`).
        *   **BigQuery Dataset/Project Constants:** For fixed environment roots or paths within BigQuery.
        *   **Cloud Composer/Airflow Variables/Connections:** If external orchestration is used, these can manage environment-specific configurations.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):** These KornShell utility scripts are sourced by `k_ausd_bp_ta_bpr_instance.ksh`.
    *   **Replacement Strategy:** Their functionalities will be integrated directly into the BigQuery Stored Procedure (e.g., date validation, parameter checks) or replaced by native BigQuery capabilities.
*   **Oracle Package/Procedure (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`):** Used in `d_ausd_bp_ta_bpr_instance.sql`.
    *   **Replacement Strategy:** This specific call (for `TRUNCATE TABLE`) will be replaced by a direct BigQuery `TRUNCATE TABLE` DDL statement.

## 7. Unresolved / Risks

*   **External Data Ingestion:** The existence of `@pcrs1` implies a critical external dependency on a source Oracle database. The reliable and timely ingestion of `cds$ta_cntrct`, `pds$ta_bpri_com`, and `isbert_schema.dwtk_meldungen` into BigQuery is paramount and needs a dedicated ingestion strategy and implementation.
*   **`d_ausd_bp_ta_bpr_instance.sql` Complexity (Manual Bucket):** The SQL script is classified as "complex" and in the "manual" migration bucket. This indicates that direct, automated conversion to BigQuery SQL may not be straightforward. Significant manual effort will likely be required to ensure logical equivalence, data type mapping, performance optimization, and adherence to BigQuery best practices.
*   **`D_AUSD_BP_BPR_INSTANCE` Package Reference:** The `d_ausd_bp_ta_bpr_instance.sql` script also shows a `USES_PACKAGE:D_AUSD_BP_BPR_INSTANCE` edge. If this refers to a separate Oracle package that contains additional logic (beyond the SQL script itself), that package will need to be analyzed and migrated separately. Based on the lineage, it might be a self-reference, but further investigation might be needed.
*   **Specific Date Logic in `gestern.ksh`:** Although `gestern.ksh` is marked for retirement, its date calculation logic (especially the manual leap year check) needs careful review during re-implementation with BigQuery functions to ensure full functional parity, although BigQuery's native functions are generally more robust.
*   **Commented-out Sections:** The original `k_ausd_bp_ta_bpr_instance.ksh` contains commented-out sections for `FOSJobDeaktivate`, file post-processing (using `sed`, `sort`, `join`), and `FOSJobErzeugeEintrag`. It's assumed these are indeed inactive and will not be migrated. Confirmation with business users is recommended.

## 8. Build Plan

The migration will involve creating the following BigQuery components:

1.  **BigQuery Tables for Source Systems (DDL):**
    *   `isbert_schema.dwtk_meldungen`
    *   `cds$ta_cntrct`
    *   `pds$ta_bpri_com`
    *   **Language:** BigQuery DDL
    *   **Note:** These tables will serve as landing zones for data ingested from the legacy Oracle systems. Schemas should align with the source.

2.  **BigQuery Target Table (DDL):**
    *   `sof$ta_bpr_instance`
    *   **Language:** BigQuery DDL
    *   **Note:** Define appropriate partitioning and clustering keys based on expected query patterns for performance.

3.  **BigQuery Logging/Control Table (DDL):**
    *   `project.dataset.job_log` (or similar)
    *   **Language:** BigQuery DDL
    *   **Note:** To store job execution metadata, status, and record counts.

4.  **BigQuery Stored Procedure for `d_ausd_bp_ta_bpr_instance.sql` (Transformation Logic):**
    *   `project.dataset.d_ausd_bp_ta_bpr_instance`
    *   **Language:** BigQuery SQL (within a Stored Procedure)
    *   **Note:** This will contain the core data transformation and loading logic. Manual conversion will be required.

5.  **BigQuery Stored Procedure for `k_ausd_bp_ta_bpr_instance.ksh` (Orchestration Logic):**
    *   `project.dataset.r_ausd_bp_ta_bpr_instance`
    *   **Language:** BigQuery SQL (within a Stored Procedure)
    *   **Note:** This will be the main entry point, handling parameters, date calculation, calling the transformation procedure, and logging.

6.  **Data Ingestion Pipelines:**
    *   Pipelines to ingest data from Oracle source system(s) (e.g., `@pcrs1`) into the BigQuery tables `cds$ta_cntrct`, `pds$ta_bpri_com`, and `isbert_schema.dwtk_meldungen`.
    *   **Language:** Cloud Data Fusion pipelines, Dataflow jobs, or DMS configuration.

7.  **External Orchestration (Optional):**
    *   If external scheduling is required, an Airflow DAG (e.g., using Cloud Composer) or Cloud Scheduler job to trigger `project.dataset.r_ausd_bp_ta_bpr_instance`.
    *   **Language:** Python (for Airflow DAGs) or YAML/JSON for Cloud Scheduler.