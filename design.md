# Migration Design — DW.BERT_AUSD_V_TA_C_BFC

## 1. Purpose & Scope
This document outlines the migration design for the ETL job `DW.BERT_AUSD_V_TA_C_BFC` from a legacy UC4-orchestrated KornShell/Oracle SQL environment to Google Cloud Platform, utilizing BigQuery for data processing and Airflow (or Cloud Composer) for orchestration.

The primary purpose of this job is to update contract extension period caching, specifically targeting the `ta_c_bfc` table (referred to as `sof$ta_c_bfc` in the Oracle SQL script). It involves reading contract-related data from various source tables, calculating binding dates, and incrementally updating a cache table.

## 2. Source Inventory
The `DW.BERT_AUSD_V_TA_C_BFC` job is composed of the following source files:

| File Name | Relative Path | Technology | Category | Tool | Tier | Automation Bucket | Summary |
| :---------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------- | :--------- | :---------- | :----- | :---------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DW.BERT_AUSD_V_TA_C_BFC.xml` | `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_C_BFC.xml` | Orchestration | `uc4` | `UC4/Automic` | `medium` | `semi_auto` | UC4 UNIX job definition for updating contract extension period caching. It orchestrates the execution of a KornShell script. |
| `r_ausd_v_ta_c_bfc.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh` | Shell Script | `shell` | `KornShell` | `medium` | `auto` | KornShell wrapper script for updating the 'ta_c_bfc' table (Bindefristcache). It handles parameter parsing, environment setup, error trapping, and calls a core script for the actual update. |
| `k_ausd_v_ta_c_bfc.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh` | Shell Script | `shell` | `KornShell` | `medium` | `semi_auto` | KornShell script acting as a control script for `r_ausd_v_ta_c_bfc.ksh`. It handles job parameters and invokes the core SQL script. |
| `d_ausd_v_ta_c_bfc.sql` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_c_bfc.sql` | SQL | `sql` | `Oracle SQL` | `medium` | `semi_auto` | Oracle SQL script defining synonyms and a function to calculate contract binding dates, then uses these to populate and incrementally update a binding date cache table (`SOF$TA_C_BFC`) from various contract-related source tables. |
| `f_alis_msgerr.ksh` | `vobs/dw_source/isrpt/isbert/allgemein/is/util/bin/f_alis_msgerr.ksh` | Shell Script | `shell` | `KornShell` | `unknown` | `unknown` | Utility script for error messaging. |
| `h_alis_parameter.ksh` | `vobs/dw_source/isrpt/isbert/allgemein/is/util/bin/h_alis_parameter.ksh` | Shell Script | `shell` | `KornShell` | `unknown` | `unknown` | Utility script for parameter handling. |
| `h_alis_date.ksh` | `vobs/dw_source/isrpt/isbert/allgemein/is/util/bin/h_alis_date.ksh` | Shell Script | `shell` | `KornShell` | `unknown` | `unknown` | Utility script for date handling. |
| `h_alis_sqlplus.ksh` | `vobs/dw_source/isrpt/isbert/allgemein/is/util/bin/h_alis_sqlplus.ksh` | Shell Script | `shell` | `KornShell` | `unknown` | `unknown` | Utility script for SQL*Plus interaction. |

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services for data processing and orchestration.
*   **Orchestration:** Apache Airflow (or Google Cloud Composer) will manage the job workflow. A single Airflow DAG will replace the UC4 job.
*   **Data Processing:** Google BigQuery will be the primary data warehouse for all tables. The Oracle SQL logic will be converted to BigQuery Standard SQL.
*   **Transformation Logic:** The complex SQL logic from `d_ausd_v_ta_c_bfc.sql` will be directly translated into BigQuery SQL scripts. PL/SQL functions/packages like `cds$vr_Bindefrist.GetBindeFrist` will need to be re-implemented as BigQuery User-Defined Functions (UDFs) or potentially external functions if complex logic requires other services.
*   **Input/Output Tables:** All source tables (`sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, `isbert_schema.dwtk_meldungen`, `all_objects`) and target tables (`sof$ta_c_bfc`, `sof$ta_c_bfc_akt`) will reside in BigQuery.
*   **Utility Scripts:** The KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) will either be replaced by Airflow's native capabilities, BigQuery features, or re-implemented in Python as part of the Airflow DAG for any remaining necessary functionality (e.g., logging, error handling).

## 4. Data Flow & Lineage
The data flow of the `DW.BERT_AUSD_V_TA_C_BFC` job in the target BigQuery/Airflow environment will be as follows:

1.  **Airflow DAG Trigger:** The `dw_bert_ausd_v_ta_c_bfc` Airflow DAG is triggered (either manually or by a schedule to be determined).
2.  **Orchestration Task (`run_dw_bert_ausd_v_ta_c_bfc`):**
    *   This task will encapsulate the logic from `r_ausd_v_ta_c_bfc.ksh` and `k_ausd_v_ta_c_bfc.ksh`.
    *   It will execute the core BigQuery SQL logic derived from `d_ausd_v_ta_c_bfc.sql`.
    *   The BigQuery SQL script will perform the following steps:
        *   **Preparation:** Truncate the temporary table `project.dataset.sof$ta_c_bfc_akt`.
        *   **Data Aggregation:** Insert data into `project.dataset.sof$ta_c_bfc_akt` by joining `project.dataset.sof$ta_cntrct_crs`, `project.dataset.sof$ta_barrier`, `project.dataset.sof$ta_cntrct_valid`, and `project.dataset.sof$ta_period`. This step calculates aggregated contract activity data.
        *   **Initial Load (Conditional):** If the target table `project.dataset.sof$ta_c_bfc` is empty, it will be populated from `sof$ta_c_bfc_akt`.
        *   **Incremental Update (Merge):** A `MERGE` statement will update existing rows in `project.dataset.sof$ta_c_bfc` that have changed (based on `bfc_age` or `bfc_count`) and insert new rows from `sof$ta_c_bfc_akt`. The `bindefrist` (binding date) will be calculated using the BigQuery UDF equivalent of `bfc_get_bindefrist`.
        *   **Outdated Row Recomputation:** An `UPDATE` statement will recompute `bindefrist` for rows in `project.dataset.sof$ta_c_bfc` where the `bfc_procedure` (procedure version) is older than the current version, processed in batches.
        *   **Cleanup:** Truncate the temporary table `project.dataset.sof$ta_c_bfc_akt`.
3.  **Result:** The `project.dataset.sof$ta_c_bfc` table in BigQuery will be updated with the latest contract extension period caching data.

## 5. Transformation Logic

The core transformation logic resides within the `d_ausd_v_ta_c_bfc.sql` script, which will be converted to BigQuery Standard SQL.

**Key Transformations:**

*   **Data Types:** Oracle `DATE` will map to BigQuery `DATE` or `TIMESTAMP`, `NUMBER` to `NUMERIC` or `INT64`.
*   **Function Conversion:** The Oracle PL/SQL function `bfc_get_bindefrist` (which internally calls `Cds$vr_Bindefrist.GetBindeFrist`) must be re-implemented as a BigQuery UDF. This UDF will take `cntrct_id`, `commitment_reference_date`, and `cntrct_validity_id` as input and return a `DATE`.
*   **Variable Definitions:** Oracle `DEFINE` statements will be replaced by BigQuery `DECLARE` statements.
    *   `v_carmen` (DB-Link) will be removed as BigQuery connects directly to tables.
    *   `v_datum` (stichtag) derived from `dwtk_meldungen` will be obtained via a `SELECT` into a `DECLARE` variable.
    *   `v_bfc_procedure` (procedure creation date) from `all_objects` will also be obtained via a `SELECT` into a `DECLARE` variable.
    *   `v_max_update` will become a `DECLARE` variable.
*   **Temporary Table Management:** `TRUNCATE TABLE sof$ta_c_bfc_akt` will remain.
*   **Joins:** Oracle outer join syntax `(+)` will be converted to explicit `LEFT JOIN`s in BigQuery.
*   **Null Handling:** Oracle `NVL` function will be converted to `COALESCE`.
*   **Date Functions:** `TO_DATE('YYYYMMDD')` will be `PARSE_DATE('%Y%m%d', 'YYYYMMDD')`, `TO_CHAR(date, 'YYYYMMDD')` to `FORMAT_DATE('%Y%m%d', DATE(date))`, `TRUNC(date)` to `DATE(date)`.
*   **Conditional Logic:** The `IF v_rows = 0 THEN INSERT ... END IF` block will be implemented using a BigQuery `IF` control statement.
*   **Merge Statement:** The Oracle `MERGE INTO` statement will be directly translated to BigQuery `MERGE` syntax, handling `WHEN MATCHED THEN UPDATE` and `WHEN NOT MATCHED THEN INSERT` clauses. The `WHERE` clause in the `UPDATE` part of `MERGE` will be retained.
*   **Update Statement:** The Oracle `UPDATE` statement with `ROWNUM <= &v_max_update` for batch processing will be converted to BigQuery's `QUALIFY ROW_NUMBER() OVER (ORDER BY cntrct_id) <= v_max_update` for limiting the number of updated rows.
*   **Dynamic SQL:** References to `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` will need to be re-evaluated. If these are executing simple DDL/DML, they can be part of the BigQuery script. If they are complex stored procedures, those procedures will need migration.

**Example BigQuery SQL (from MCP Tool):**
```sql
-- BigQuery Script Version (excerpt from d_ausd_v_ta_c_bfc.sql)

DECLARE v_max_update INT64 DEFAULT 1000000;
DECLARE v_bfc_procedure DATE DEFAULT (
  SELECT COALESCE(MAX(DATE(created)), DATE '1900-01-01')
  FROM `project.dataset.all_objects` -- Corrected from `pcrs1.all_objects`
  WHERE object_name = 'CDS$VR_BINDEFRIST'
    AND object_type = 'PACKAGE'
);

CREATE TEMP FUNCTION bfc_get_bindefrist(
  i_cntrct_id STRING,
  i_commitment_reference_date DATE,
  i_cntrct_validity_id STRING
) AS (
  NULL -- Placeholder, actual logic to be implemented here (e.g., using JS UDF or external function)
);

-- Step 1: refresh staging table
TRUNCATE TABLE `project.dataset.sof$ta_c_bfc_akt`;

INSERT INTO `project.dataset.sof$ta_c_bfc_akt`
SELECT
  c.cntrct_id,
  MAX(c.commitment_reference_date) AS commitment_reference_date,
  MAX(c.cntrct_validity_id) AS cntrct_validity_id,
  MAX(GREATEST(
    COALESCE(c.bfc_age, DATE '1900-01-01'),
    COALESCE(b.bfc_age, DATE '1900-01-01'),
    COALESCE(v.bfc_age, DATE '1900-01-01'),
    COALESCE(p_fi.bfc_age, DATE '1900-01-01'),
    COALESCE(p_fo.bfc_age, DATE '1900-01-01'),
    COALESCE(p_fi_n.bfc_age, DATE '1900-01-01'),
    COALESCE(p_fo_n.bfc_age, DATE '1900-01-01')
  )) AS bfc_age,
  COUNT(1) AS bfc_count
FROM `project.dataset.sof$ta_cntrct_crs` c
LEFT JOIN `project.dataset.sof$ta_barrier` b
  ON c.cntrct_id = b.cntrct_id
LEFT JOIN `project.dataset.sof$ta_cntrct_valid` v
  ON c.cntrct_validity_id = v.cntrct_validity_id
LEFT JOIN `project.dataset.sof$ta_period` p_fi
  ON v.first_period_id = p_fi.period_id
LEFT JOIN `project.dataset.sof$ta_period` p_fo
  ON v.following_period_id = p_fo.period_id
LEFT JOIN `project.dataset.sof$ta_period` p_fi_n
  ON v.first_notice_period_id = p_fi_n.period_id
LEFT JOIN `project.dataset.sof$ta_period` p_fo_n
  ON v.follow_notice_period_id = p_fo_n.period_id
GROUP BY c.cntrct_id;

-- ... (remaining MERGE and UPDATE statements)
```

## 6. External Dependencies
The legacy job has the following external dependencies:

*   **Oracle Database:** The primary data source and target database (`sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, `isbert_schema.dwtk_meldungen`, `all_objects`, `sof$ta_c_bfc`, `sof$ta_c_bfc_akt`, `spr_schema.cds$vr_Bindefrist`, `spr_schema.spr$pa_types`, `spr_schema.cds$ta_cntrct`) are hosted in an Oracle database, accessed via `SQL*Plus` and potentially a DB-link `@pcrs1`.
    *   **Replacement:** All these tables and schema objects will be migrated to Google BigQuery datasets and tables. The DB-link and Oracle-specific schema references will be replaced with BigQuery fully-qualified table names (`project.dataset.table`). The `Cds$vr_Bindefrist.GetBindeFrist` package function will be re-implemented as a BigQuery UDF.
*   **Unix Host (`DWHDWH1P`):** The UC4 job runs on a specific Unix host.
    *   **Replacement:** This will be replaced by the Airflow/Cloud Composer environment, which provides managed execution of DAGs. The concept of a specific host for job execution will be abstracted away.
*   **Unix Login (`DW.UNIX.ISBERT`):** The job runs under a specific Unix login.
    *   **Replacement:** This will be replaced by Google Cloud service accounts with appropriate IAM roles and permissions to interact with BigQuery, Cloud Composer, and other necessary GCP services.
*   **KornShell Environment (`$HOME/.dw_init`, `BERT_DIR_ROOT`):** The shell scripts rely on specific environment variables and initialization files.
    *   **Replacement:** These will be replaced by Airflow variables, connections, or configuration settings within the DAG itself or the Cloud Composer environment. The utility scripts (`f_alis_msgerr.ksh`, etc.) will be either re-implemented in Python or their functionality absorbed by standard Airflow practices (e.g., logging, error handling).

## 7. Unresolved / Risks

*   **UDF/PL/SQL Logic (`bfc_get_bindefrist`, `Cds$vr_Bindefrist.GetBindeFrist`):** The exact logic of the `Cds$vr_Bindefrist.GetBindeFrist` package function is not available in the provided SQL. Its re-implementation as a BigQuery UDF (possibly a JavaScript UDF or an external function calling a Cloud Function) is critical and requires detailed analysis of its source code or functional specification. This is the main *semi_auto* (B2) challenge.
*   **Scheduler Detail:** The UC4 job did not provide an `EVNT_TIME` file or workflow plan, meaning the exact schedule and dependencies on other jobs are unknown. The Airflow DAG will initially be unscheduled and require manual review to determine its proper scheduling in the target environment.
*   **Oracle `all_objects` table:** The query against `all_objects` to determine `v_bfc_procedure` might need a BigQuery equivalent of a metadata table to track UDF versions or deployment dates if similar versioning logic is required.
*   **Error Handling and Retries:** The UC4 job's error handling and explicit restart notes (`ORA-00604`) require manual review to determine the appropriate Airflow retry strategy and error handling callbacks. The current Airflow design proposes `retries=0`.
*   **Utility Script Functionality:** The precise functionalities of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh` need to be assessed. Most are generic and can be replaced by Python code or Airflow features, but specific functionalities, if any, will require careful migration.
*   **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`:** The exact functionality of this stored procedure needs to be analyzed. If it executes DDL/DML, it can be translated to BigQuery SQL; if it performs complex procedural logic, it will require BigQuery stored procedure or UDF conversion.

## 8. Build Plan

The migration will involve the following ordered steps and generated files:

1.  **Migrate Oracle Tables to BigQuery:**
    *   **Action:** Create BigQuery datasets and tables corresponding to all source and target tables identified: `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, `isbert_schema.dwtk_meldungen`, `project.dataset.all_objects` (metadata), `sof$ta_c_bfc`, `sof$ta_c_bfc_akt`.
    *   **Language:** DDL (BigQuery Standard SQL).
2.  **Convert Oracle PL/SQL Function to BigQuery UDF:**
    *   **Action:** Re-implement the `bfc_get_bindefrist` logic (including the `Cds$vr_Bindefrist.GetBindeFrist` call) as a BigQuery User-Defined Function (UDF). This may require a JavaScript UDF or a separate Cloud Function if the logic is very complex.
    *   **Language:** BigQuery SQL (for UDF definition) / JavaScript / Python (for Cloud Function).
3.  **Generate BigQuery Standard SQL Script:**
    *   **Action:** Convert the `d_ausd_v_ta_c_bfc.sql` script into BigQuery Standard SQL, incorporating the BigQuery UDF.
    *   **File:** `d_ausd_v_ta_c_bfc.bqsql`
    *   **Language:** BigQuery Standard SQL.
4.  **Develop Airflow DAG:**
    *   **Action:** Create an Airflow DAG (`dw_bert_ausd_v_ta_c_bfc.py`) to orchestrate the execution. This DAG will replace the UC4 job and the KornShell wrapper scripts. It will contain a single `BigQueryOperator` (or similar) to execute the `d_ausd_v_ta_c_bfc.bqsql` script. It will also handle any necessary parameter passing or environment setup previously done by the ksh scripts.
    *   **File:** `dw_bert_ausd_v_ta_c_bfc.py`
    *   **Language:** Python (Airflow DAG).
5.  **Refactor Utility Logic:**
    *   **Action:** Any essential functionality from `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh` that cannot be replaced by native Airflow or BigQuery features should be reimplemented in Python as part of the Airflow DAG or as separate Python libraries.
    *   **Language:** Python.
6.  **Deploy and Test:**
    *   **Action:** Deploy the BigQuery DDL, UDFs, and Airflow DAG to the GCP environment. Perform thorough testing to ensure functional equivalence and performance.