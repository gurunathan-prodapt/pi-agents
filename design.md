# Migration Design — DW.BERT_AUSD_V_TA_P_VERTRAG

## 1. Purpose & Scope
This job, `DW.BERT_AUSD_V_TA_P_VERTRAG`, is designed to update contract information, specifically related to "twin-bill" contracts. The core logic involves processing contract data from a temporary staging table (`sof$ta_vertrag_tmp`) and populating a primary contract table (`sof$ta_p_vertrag`) within an Oracle schema. The job orchestrates the execution of KornShell scripts that, in turn, execute an Oracle SQL*Plus script.

The scope of this migration is to re-implement the existing functionality on the Google Cloud Platform, specifically utilizing BigQuery for data storage and transformation, and likely Cloud Composer (Apache Airflow) for orchestration.

## 2. Source Inventory
The assembled job consists of four primary components:

*   **Orchestration (UC4 Job Definition)**
    *   **File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_VERTRAG.xml`
    *   **Technology:** UC4/Automic Job (UNIX type)
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** Defines the overall job, invoking the main KornShell wrapper script.

*   **Wrapper Shell Script**
    *   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** Acts as a framework script, setting up the environment, handling error logging and parameter parsing, and then calling the core control script.

*   **Control Shell Script**
    *   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** Controls the execution of the SQL script. It passes parameters, manages job status, and integrates with logging and error handling utilities (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).

*   **Core Data Transformation Script**
    *   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql`
    *   **Technology:** Oracle SQL*Plus
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** Contains the primary SQL logic to process contract data, truncate temporary tables, and populate the target `sof$ta_p_vertrag` table.

## 3. Target Architecture
The target architecture will leverage Google Cloud services:

*   **Orchestration:** Cloud Composer (Apache Airflow) to manage the job workflow. The current UC4 job will be translated into an Airflow DAG.
*   **Data Storage & Transformation:** BigQuery will replace the Oracle database for all permanent and temporary tables involved in this job.
    *   Oracle tables like `isbert_schema.dwtk_meldungen`, `sof$ta_vertrag_tmp`, `sof$ta_p_vertrag`, and all other `sof$` tables will be migrated to BigQuery datasets and tables.
    *   The PL/SQL procedure `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` used for truncating tables will need to be re-implemented, likely as direct BigQuery `TRUNCATE TABLE` statements or equivalent DDL operations within a Python operator.
*   **Code Execution:**
    *   The KornShell scripts will be re-written in Python, encapsulating the orchestration logic and BigQuery interactions.
    *   The Oracle SQL*Plus script will be converted to BigQuery Standard SQL.

## 4. Data Flow & Lineage
The current data flow is as follows:

1.  **UC4 Job (`DW.BERT_AUSD_V_TA_P_VERTRAG.xml`):** Initiates the entire workflow.
2.  **Wrapper Script (`r_ausd_v_ta_p_vertrag.ksh`):** Invoked by the UC4 job. It performs initial setup, parameter parsing, and error handling, then calls `k_ausd_v_ta_p_vertrag.ksh`.
3.  **Control Script (`k_ausd_v_ta_p_vertrag.ksh`):** Invoked by `r_ausd_v_ta_p_vertrag.ksh`. It prepares parameters for the SQL script and executes `d_ausd_v_ta_p_vertrag.sql` via `starteSQLSkript`.
4.  **SQL Script (`d_ausd_v_ta_p_vertrag.sql`):** Executed by `k_ausd_v_ta_p_vertrag.ksh`.
    *   **Reads from:**
        *   `isbert_schema.dwtk_meldungen`: To determine the `v_datum` (date stamp) based on `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        *   `sof$ta_vertrag_tmp`: The primary source table for contract data.
    *   **Transforms data:** Processes "twin-bill" contracts using a self-join on `sof$ta_vertrag_tmp`.
    *   **Writes to:** `sof$ta_p_vertrag`.
    *   **Truncates:** `sof$ta_p_vertrag` and numerous other `sof$` temporary/staging tables (`sof$ta_disc_zusgf`, `sof$ta_discount`, `sof$ta_barrier_zusgf`, `sof$ta_barrier`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`, `sof$ta_cntrct_valid`, `sof$ta_period`, `sof$ta_bp_ref`, `sof$ta_inv_assign`, `sof$ta_inv_def`, `sof$ta_acc_ref`, `sof$ta_notice`, `sof$ta_apn_ve`, `sof$ta_discount_rr`, `sof$ta_vvl_dwh`, `sof$ta_vvl_upgrade`, `sof$ta_cntrct_crs2`, `sof$ta_cntrct_crs3`, `sof$ta_inv_acc`, `sof$ta_vertrag_tmp`, `sof$ta_action_assoc`).

**Target Data Flow:**
The Airflow DAG will manage the execution of Python tasks.

1.  **Airflow DAG:** The entry point, replacing the UC4 job.
2.  **Python Operator (Wrapper & Control):** A Python script will encapsulate the logic from `r_ausd_v_ta_p_vertrag.ksh` and `k_ausd_v_ta_p_vertrag.ksh`, handling environment setup, parameter passing, logging, and error management.
3.  **BigQuery Operator (SQL Execution):** The converted BigQuery SQL script (`d_ausd_v_ta_p_vertrag.sql`) will be executed via a BigQuery operator.
    *   The `TRUNCATE` operations on various `sof$` tables will be executed as separate BigQuery DDL tasks or combined into the main SQL script.
    *   Data will be read from BigQuery tables (e.g., `dwtk_meldungen_bq`, `ta_vertrag_tmp_bq`) and written to BigQuery `ta_p_vertrag_bq`.

## 5. Transformation Logic
The core transformation resides in `d_ausd_v_ta_p_vertrag.sql`.

**Original Oracle SQL:**
```sql
-- Stichtag ermitteln
COLUMN s_datum new_value v_datum noprint
SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
  FROM isbert_schema.dwtk_meldungen m
 WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

-- Truncate target table
WHENEVER SQLERROR CONTINUE
begin
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_p_vertrag');
end;
/

-- Insert/Update twin-bill contract data
INSERT  INTO sof$ta_p_vertrag
       (vertrag_id_carmen, partner_id_carmen, rechdef_id_carmen, kundenkonto, mwst_kennzeichen,
        rahmenvertrag_id, rechnungslauf, vo_kenn, geplant_kuend, eingang_kuend,
        vertragsbeginn, vertragsstatus, sperrart, sperrgrund, stillegungszeitraum,
        twincard, dwh_tarifgr_text, bindefrist, letztes_upgrade, vertragsbindung,
        vertragsbindungseinheit, rechnungszahlart, rechnungsmedium, twin_vertrag_id,
        upgradeberechtigt, apn, upgradegrund, sv_id, vda, cost_centre,
        cost_centre_user, cntrct_ty, segment_id, rv_action_id, rechn_inh_konfig_text,
        order_number, commitment_reference_date, cntrct_validity_id)
SELECT /*+ parallel(v,4) parallel(pv,4) */
       v.vertrag_id_carmen, v.partner_id_carmen, v.rechdef_id_carmen, v.kundenkonto, v.mwst_kennzeichen,
       v.rahmenvertrag_id as rahmenvertrag_id, v.rechnungslauf, v.vo_kenn as vo_kenn,
       v.geplant_kuend, v.eingang_kuend, v.vertragsbeginn, v.vertragsstatus,
       v.sperrart, v.sperrgrund, v.stillegungszeitraum, v.twincard, v.dwh_tarifgr_text,
       v.bindefrist, v.letztes_upgrade, v.vertragsbindung, v.vertragsbindungseinheit,
       v.rechnungszahlart, v.rechnungsmedium, v.twin_vertrag_id, v.upgradeberechtigt,
       v.apn, v.upgradegrund, v.sv_id, v.vda, v.cost_centre, v.cost_centre_user,
       v.cntrct_ty, v.segment_id, v.rv_action_id, v.rechn_inh_konfig_text,
       v.order_number, v.commitment_reference_date, v.cntrct_validity_id
  FROM
        sof$ta_vertrag_tmp     v,
        sof$ta_vertrag_tmp     pv
  WHERE
        v.twin_vertrag_id = pv.vertrag_id_carmen (+);

commit;

-- Truncate temporary tables
WHENEVER SQLERROR CONTINUE
begin
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_disc_zusgf DROP STORAGE');
... (many more TRUNCATE statements)
end;
/
```

**BigQuery Standard SQL Conversion:**

*   **Date Determination:** The logic to get `v_datum` from `isbert_schema.dwtk_meldungen` will be translated to a BigQuery query.
    ```sql
    DECLARE v_datum STRING;
    SET v_datum = COALESCE((SELECT FORMAT_DATE('%Y%m%d', MAX(m.timecreated)) FROM `project.dataset.dwtk_meldungen_bq` m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'), '19000101');
    ```
*   **Table Truncation:** `TRUNCATE TABLE` statements will be used directly in BigQuery.
    ```sql
    TRUNCATE TABLE `project.dataset.sof_ta_p_vertrag_bq`;
    ```
*   **Main INSERT:** The `INSERT` statement will be converted, noting the ANSI SQL `LEFT JOIN` equivalent for the `(+)` operator.
    ```sql
    INSERT INTO `project.dataset.sof_ta_p_vertrag_bq`
           (vertrag_id_carmen, partner_id_carmen, rechdef_id_carmen, kundenkonto, mwst_kennzeichen,
            rahmenvertrag_id, rechnungslauf, vo_kenn, geplant_kuend, eingang_kuend,
            vertragsbeginn, vertragsstatus, sperrart, sperrgrund, stillegungszeitraum,
            twincard, dwh_tarifgr_text, bindefrist, letztes_upgrade, vertragsbindung,
            vertragsbindungseinheit, rechnungszahlart, rechnungsmedium, twin_vertrag_id,
            upgradeberechtigt, apn, upgradegrund, sv_id, vda, cost_centre,
            cost_centre_user, cntrct_ty, segment_id, rv_action_id, rechn_inh_konfig_text,
            order_number, commitment_reference_date, cntrct_validity_id)
    SELECT
           v.vertrag_id_carmen, v.partner_id_carmen, v.rechdef_id_carmen, v.kundenkonto, v.mwst_kennzeichen,
           v.rahmenvertrag_id, v.rechnungslauf, v.vo_kenn, v.geplant_kuend, v.eingang_kuend,
           v.vertragsbeginn, v.vertragsstatus, v.sperrart, v.sperrgrund, v.stillegungszeitraum,
           v.twincard, v.dwh_tarifgr_text, v.bindefrist, v.letztes_upgrade, v.vertragsbindung,
           v.vertragsbindungseinheit, v.rechnungszahlart, v.rechnungsmedium, v.twin_vertrag_id,
           v.upgradeberechtigt, v.apn, v.upgradegrund, v.sv_id, v.vda, v.cost_centre, v.cost_centre_user,
           v.cntrct_ty, v.segment_id, v.rv_action_id, v.rechn_inh_konfig_text,
           v.order_number, v.commitment_reference_date, v.cntrct_validity_id
      FROM
            `project.dataset.sof_ta_vertrag_tmp_bq` v
      LEFT JOIN
            `project.dataset.sof_ta_vertrag_tmp_bq` pv
        ON
            v.twin_vertrag_id = pv.vertrag_id_carmen;
    ```
*   **Other Truncations:** All `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE ...')` calls will be replaced with direct `TRUNCATE TABLE` statements in BigQuery.

## 6. External Dependencies
The original job has the following external dependencies:

*   **Oracle Database:** The primary data source and target are Oracle tables (e.g., `isbert_schema.dwtk_meldungen`, `sof$ta_vertrag_tmp`, `sof$ta_p_vertrag`). These will be migrated to BigQuery.
*   **Oracle DB Link (`PCRS1` / `v_carmen`):** The SQL script defines `v_carmen = "@pcrs1"`, indicating a potential database link to an external Oracle system named `pcrs1`. While not directly used in the main `INSERT` statement, its presence suggests this job *might* depend on data from or send data to this external system in other parts of the broader system. For this specific job's transformation, `sof$ta_vertrag_tmp` is the input.
    *   **Migration Plan:** If `sof$ta_vertrag_tmp` is sourced from `PCRS1`, data ingestion pipelines (e.g., Dataflow, Cloud Data Fusion, or custom Python scripts) will be established to bring data from the `PCRS1` Oracle instance into a BigQuery staging table that would serve as `sof_ta_vertrag_tmp_bq`. If `PCRS1` is only for reference or minor lookups, it might be ingested into a BigQuery reference table. If `PCRS1` is the source of `sof$ta_vertrag_tmp`, this dependency becomes a critical data ingestion stream.

## 7. Unresolved / Risks
*   **Parameter Passing:** The KornShell scripts use `getopts` for parameter parsing. This will need to be re-implemented in the Python wrapper for the Airflow DAG.
*   **Environment Variables:** The scripts rely on environment variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`). These paths and variables must be properly configured within the Airflow environment or passed as Airflow variables/parameters.
*   **Utility Scripts:** The `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` utility scripts will need to be re-written in Python or their functionality replaced with suitable Python libraries or Airflow features for logging, date handling, parameter processing, and SQL execution.
*   **Oracle-specific features:** The `/*+ parallel(v,4) parallel(pv,4) */` hint in the SQL is Oracle-specific and will be removed during conversion to BigQuery, as BigQuery handles parallelism automatically. `WHENEVER SQLERROR CONTINUE/EXIT FAILURE` and `SET TIMING ON/SERVEROUTPUT ON` are SQL*Plus specific commands and will be handled by Python error handling and logging.
*   **External System `PCRS1`:** The exact nature of the dependency on `PCRS1` needs further investigation. While the SQL doesn't explicitly query `@pcrs1`, it's defined. Confirm if `sof$ta_vertrag_tmp` or any other direct input to this job is populated from `PCRS1`. If so, a data ingestion pipeline from Oracle `PCRS1` to BigQuery is required.
*   **Transactional Integrity:** Oracle's `COMMIT` statement ensures transactional integrity. In BigQuery, operations are typically atomic, or can be made so by using appropriate DML statements within a single transaction if supported (e.g., DML in a script) or by staging data and performing a final overwrite. The current pattern of truncating and then inserting should be safe.

## 8. Build Plan
The migration will follow these steps:

1.  **BigQuery Schema Definition:**
    *   Create corresponding BigQuery tables for `isbert_schema.dwtk_meldungen`, `sof$ta_vertrag_tmp`, `sof$ta_p_vertrag`, and all other `sof$` tables that are truncated or used in the job. Data types will be mapped from Oracle to BigQuery.
2.  **Oracle `PCRS1` Data Ingestion (if necessary):**
    *   If `PCRS1` is confirmed as a source for `sof$ta_vertrag_tmp` or other tables, develop and deploy a data ingestion pipeline (e.g., Dataflow, Fivetran, custom Python) to bring this data into BigQuery.
3.  **SQL Conversion (`d_ausd_v_ta_p_vertrag.sql` to BQSQL):**
    *   Translate `d_ausd_v_ta_p_vertrag.sql` into BigQuery Standard SQL, replacing Oracle-specific syntax and functions.
    *   Replace `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE ...')` calls with direct BigQuery `TRUNCATE TABLE` statements.
    *   **Output:** `d_ausd_v_ta_p_vertrag_bq.sql` (BigQuery SQL)
4.  **Shell Script Conversion to Python:**
    *   Re-write the logic of `r_ausd_v_ta_p_vertrag.ksh` and `k_ausd_v_ta_p_vertrag.ksh` into a single Python script.
    *   This script will handle parameter parsing, logging (e.g., using Python's `logging` module), and invoking the BigQuery SQL.
    *   Replace calls to `DWMSG_*` utilities with Python equivalents or Airflow logging.
    *   **Output:** `p_vertrag_processor.py` (Python)
5.  **Airflow DAG Development:**
    *   Create an Airflow DAG (`dw_bert_ausd_v_ta_p_vertrag_dag.py`) that orchestrates the Python script and BigQuery tasks.
    *   Define tasks for:
        *   Executing the `p_vertrag_processor.py` (using `PythonOperator`).
        *   Executing the `d_ausd_v_ta_p_vertrag_bq.sql` (using `BigQueryOperator` or `BigQueryExecuteQueryOperator`).
        *   Tasks for truncating other temporary `sof$` tables (can be separate `BigQueryExecuteQueryOperator` tasks or part of the main SQL).
    *   **Output:** `dw_bert_ausd_v_ta_p_vertrag_dag.py` (Python Airflow DAG)
6.  **Testing:** Implement unit and integration tests for the Python scripts and BigQuery SQL.
7.  **Deployment:** Deploy the BigQuery tables, Python scripts, and Airflow DAG to the GCP environment.