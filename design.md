# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh

## 1. Purpose & Scope
This document outlines the migration design for the ETL job orchestrated by `k_ausd_bp_ta_bcp_iccid.ksh`. The original job, a KornShell script, acts as a control script, handling parameter parsing, date validation, and orchestrating the execution of a core Oracle PL/SQL script (`d_ausd_bp_ta_bcp_iccid.sql`). The SQL script's primary function is to truncate and load the `SOF$TA_BCP_ICCID` table with enriched data by joining `SOF$TA_BPR_BCP` and `SOF$TA_ICCID_VERTRAG` tables. The migration objective is to re-platform this job to Google Cloud Platform, utilizing BigQuery for data transformation and Airflow for orchestration.

## 2. Source Inventory

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh`**
    *   **Technology:** KornShell
    *   **Role:** Orchestration, parameter parsing, date validation, script execution.
    *   **Tier:** Medium (inferred)
    *   **Automation Bucket:** Semi-Auto (B2) (inferred)
    *   **Summary:** Main control script. It sources several utility scripts for error handling, date operations, parameter parsing, and SQL*Plus execution. It derives `p_datum_heute` and `p_datum_gestern` using `gestern.ksh` and then calls a helper function `starteSQLSkript` to execute `d_ausd_bp_ta_bcp_iccid.sql` with various parameters.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_iccid.sql`**
    *   **Technology:** Oracle PL/SQL
    *   **Role:** Core data transformation.
    *   **Tier:** Medium (inferred)
    *   **Automation Bucket:** Semi-Auto (B2) (inferred)
    *   **Summary:** Truncates the `sof$ta_bcp_iccid` table and inserts data from a join between `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag`. It also defines a date variable (`v_datum`) based on `isbert_schema.dwtk_meldungen`.

*   **Utility Scripts (KornShell - called by `k_ausd_bp_ta_bcp_iccid.ksh`):**
    *   `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`: Calculates and formats today's and yesterday's dates.
    *   `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling, logging, status management.
    *   `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_date.ksh`: Date calculations.
    *   `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing, validation, conversion.
    *   `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`: Helper routines for executing SQL*Plus scripts.

## 3. Target Architecture
The migrated job will run on Google Cloud Platform.
*   **Data Storage & Transformation:** Google BigQuery.
*   **Orchestration:** Cloud Composer (managed Airflow).

The Oracle tables will be migrated to BigQuery datasets and tables.
*   `sof$ta_bcp_iccid` will become `sof.ta_bcp_iccid` in BigQuery.
*   `sof$ta_bpr_bcp` will become `sof.ta_bpr_bcp` in BigQuery.
*   `sof$ta_iccid_vertrag` will become `sof.ta_iccid_vertrag` in BigQuery.
*   `isbert_schema.dwtk_meldungen` will become `isbert_schema.dwtk_meldungen` in BigQuery.

The KornShell orchestration logic will be converted into a Python-based Airflow DAG. The core data transformation logic from the Oracle PL/SQL script will be converted into BigQuery SQL, callable via a `BigQueryOperator` within the Airflow DAG.

## 4. Data Flow & Lineage
The original data flow involves reading from `isbert_schema.dwtk_meldungen`, `sof$ta_bpr_bcp`, and `sof$ta_iccid_vertrag`, processing, and writing to `sof$ta_bcp_iccid`.

**Migrated Data Flow:**
1.  **Airflow DAG Trigger:** The Airflow DAG corresponding to `k_ausd_bp_ta_bcp_iccid.ksh` is triggered.
2.  **Parameter Handling:** The DAG will ingest parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) similar to the shell script. Date validation will be re-implemented in Python.
3.  **Date Calculation:** The logic from `gestern.ksh` (calculating today's and yesterday's dates) will be re-implemented as a Python function within the DAG or a callable utility.
4.  **Metadata Query:** A BigQueryOperator will execute a SQL query to derive `v_datum` from `isbert_schema.dwtk_meldungen`.
5.  **Data Transformation (BigQuery SQL):** A BigQueryOperator will execute the migrated BigQuery SQL (derived from `d_ausd_bp_ta_bcp_iccid.sql`). This operator will first perform a `TRUNCATE` on `sof.ta_bcp_iccid` and then `INSERT` the results of the `SELECT DISTINCT` query from `sof.ta_bpr_bcp` and `sof.ta_iccid_vertrag`.
6.  **Error Handling & Logging:** The error handling and logging logic from `f_alis_msgerr.ksh` will be re-implemented using Airflow's native logging and error handling mechanisms.

## 5. Transformation Logic

**Original SQL (from `d_ausd_bp_ta_bcp_iccid.sql`):**
```sql
-- Step00: variable definition
DEFINE v_carmen = "@pcrs1"
COLUMN s_datum new_value v_datum noprint
SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
  FROM isbert_schema.dwtk_meldungen m
 WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

-- Step01: truncate target table
begin
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bcp_iccid REUSE STORAGE');
end;
/

-- Step11b: insert enriched data
INSERT INTO sof$ta_bcp_iccid
(CNTRCT_ID,
  BPR_ID,
  CNTRCT_ID_REF,
  TN_ICCID,
  TN_IMSI_HLR)
SELECT /*+ full(bp) parallel(bp,4) full(ic) parallel(ic,4) */
          distinct
          bp.cntrct_id,
          bp.bpr_id,
          bp.cntrct_id_ref,
          ic.tn_iccid,
          ic.tn_imsi_hlr
FROM      sof$ta_bpr_bcp bp,
          sof$ta_iccid_vertrag ic
WHERE     bp.cntrct_id_ref = ic.cntrct_id
;
COMMIT;
```

**Equivalent BigQuery SQL:**
```sql
-- Step00: variable definition equivalent
DECLARE v_carmen STRING DEFAULT '@pcrs1';
DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(
    FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))),
    '19000101'
  )
  FROM `isbert_schema.dwtk_meldungen` AS m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step01: truncate target table
TRUNCATE TABLE `sof.ta_bcp_iccid`;

-- Step11b: insert enriched data
INSERT INTO `sof.ta_bcp_iccid`
(
  cntrct_id,
  bpr_id,
  cntrct_id_ref,
  tn_iccid,
  tn_imsi_hlr
)
SELECT DISTINCT
  bp.cntrct_id,
  bp.bpr_id,
  bp.cntrct_id_ref,
  ic.tn_iccid,
  ic.tn_imsi_hlr
FROM `sof.ta_bpr_bcp` AS bp
JOIN `sof.ta_iccid_vertrag` AS ic
  ON bp.cntrct_id_ref = ic.cntrct_id;
```
The `DEFINE`, `COLUMN new_value`, `prompt`, `spool`, `WHENEVER SQLERROR`, `set timing on`, and `exit success` commands from the Oracle script are specific to SQL*Plus and will not be directly translated. Their functionalities (variable assignment, logging, error handling) will be handled by the Airflow orchestration layer. The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for truncating will be replaced by a direct `TRUNCATE TABLE` statement in BigQuery. The `COMMIT` is implicit in BigQuery DML operations.

## 6. External Dependencies
*   **Oracle Database:** The source tables `isbert_schema.dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`, and the target `sof$ta_bcp_iccid` reside in an Oracle database. These will be migrated to BigQuery tables.
*   **`DW_DIR_UTL` environment variable:** Used to define the `tmpFile` path. This will be replaced by a configurable Airflow variable or XCom for temporary storage if needed, or by direct BigQuery temporary tables.
*   **`${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`:** This shell script calculates dates. Its logic will be re-implemented in Python within the Airflow DAG.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`:** Error logging and handling. This will be replaced by Airflow's native logging and error handling.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`:** Date utility functions. Re-implement relevant functions in Python.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`:** Parameter parsing. Airflow DAG parameters or configuration will replace this.
*   **`${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`:** SQL*Plus helper. Replaced by `BigQueryOperator`.
*   **`@pcrs1` (from `v_carmen`):** This appears to be an external system identifier or database link. Its purpose and migration strategy need further investigation. If it represents a data source, it needs to be migrated to BigQuery or a connected external table. If it's a configuration value, it can be an Airflow variable.
*   **Commented-out `sed`, `sort`, `join` commands:** These suggest potential file processing that is currently inactive. If these become active in the future, they would need to be migrated to Cloud Dataflow, PySpark, or similar BigQuery-compatible processing. For now, they are ignored as they are commented out.

## 7. Unresolved / Risks
*   **Missing Complexity/Automation Data:** `file_complexity` and `automation_rate` data was not available for the source files. The inferred "Medium" complexity and "Semi-Auto" bucket may require further manual validation.
*   **Dynamic `BERT_DIR_ROOT`:** The shell script heavily relies on the `${BERT_DIR_ROOT}` environment variable. This dynamic path resolution needs to be explicitly defined or configured in the Airflow environment.
*   **`v_carmen = "@pcrs1"`:** The exact nature and necessity of `v_carmen` and the `@pcrs1` value are unclear and require further investigation.
*   **`isbert_schema.dwtk_meldungen` table structure and data types:** The BigQuery equivalent SQL assumes `timecreated` can be cast to `DATE`. This needs to be confirmed during schema migration.
*   **`sof$ta_bcp_iccid` columns and data types:** The `INSERT` statement specifies `CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR`. The corresponding data types in BigQuery should match the Oracle source to avoid implicit casting issues.
*   **Parameter `p_wiederanlaufWert`:** The script initializes `p_wiederanlaufWert` if empty but doesn't seem to use it in the provided code excerpt. Its purpose and how it impacts the overall flow needs to be confirmed for complete migration.
*   **`tmpFile` usage:** The `tmpFile` is used to store `v_records`. In BigQuery, record counts can be obtained directly from query results or metadata. The `eval "v_records=\`cat $tmpFile\`"` suggests a file-based communication, which should be replaced by Airflow XComs or direct variable passing.

## 8. Build Plan
1.  **Schema Migration:**
    *   Migrate `isbert_schema.dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`, and `sof$ta_bcp_iccid` from Oracle to BigQuery, preserving schema and data types.
    *   **Language:** DDL (BigQuery SQL).

2.  **BigQuery SQL Transformation:**
    *   Create a BigQuery SQL script (`d_ausd_bp_ta_bcp_iccid.bqsql`) for the core transformation logic. This script will contain the `DECLARE` statements, `TRUNCATE TABLE`, and `INSERT INTO ... SELECT DISTINCT` statements as identified in Section 5.
    *   **Language:** BigQuery SQL.

3.  **Airflow DAG Development:**
    *   Create an Airflow DAG (`k_ausd_bp_ta_bcp_iccid_dag.py`) to orchestrate the job.
    *   Define DAG parameters for `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`.
    *   Implement Python functions for date calculation (replacing `gestern.ksh`) and parameter validation.
    *   Use a `BigQueryOperator` to execute the `d_ausd_bp_ta_bcp_iccid.bqsql` script.
    *   Implement Airflow native logging and error handling.
    *   Define appropriate task dependencies.
    *   **Language:** Python.

4.  **Configuration:**
    *   Configure Airflow variables for any environment-specific settings (e.g., BigQuery project ID, dataset names, `v_carmen` value if applicable).
    *   **Language:** YAML/JSON (Airflow variables).

5.  **Testing:**
    *   Develop unit and integration tests for the BigQuery SQL transformation and the Airflow DAG.
    *   **Language:** Python, BigQuery SQL.