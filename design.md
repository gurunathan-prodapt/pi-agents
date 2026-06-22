# Migration Design — DW.BERT_AUSD_V_TA_CNTRCT_CRS2

## 1. Purpose & Scope
This job, `DW.BERT_AUSD_V_TA_CNTRCT_CRS2`, is an ETL workflow primarily responsible for updating contract data. Specifically, it performs a data reconciliation process for the `ta_cntrct_crs2` table. The main business purpose is to populate the `sof$ta_cntrct_crs2` table with contract information, enriching it with parent contract details while excluding frame contract parent entries. The workflow is orchestrated by a UC4 job, which invokes a series of KornShell (ksh) scripts that ultimately execute an Oracle SQL script for the core data transformation.

## 2. Source Inventory
The job is composed of the following source files:

| File Path                                                                                                   | Technology  | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                            |
| :---------------------------------------------------------------------------------------------------------- | :---------- | :----- | :---------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_CRS2.xml` | UC4/Automic | medium | semi_auto         | UC4 UNIX job definition for updating contracts, excluding frame contract parents, by executing a ksh script.                                                                                                                         |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh`                       | KornShell   | medium | semi_auto         | KornShell script serving as a wrapper for data reconciliation of the `ta_cntrct_crs2` table. It handles parameter parsing, environment setup, logging, error trapping, and then calls a core processing script.                         |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh`                       | KornShell   | medium | semi_auto         | This KornShell script acts as a control script for data extraction, parsing parameters, sourcing utility functions, executing a SQL script, and handling errors. It manages the execution of a SQL script that likely processes data for the `ta_cntrct_crs2` table. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs2.sql`                       | Oracle SQL  | medium | semi_auto         | This SQL script truncates a contract table and then re-populates it by selecting and joining data from another contract table, filtering out parent contracts and enriching with parent contract numbers.                           |

## 3. Target Architecture
The migrated job will run on Google Cloud Platform, utilizing:
*   **Google Cloud Composer (Airflow)** for job orchestration.
*   **Google BigQuery** for data storage and transformation.
*   **Google Cloud Storage** for storing PySpark scripts.
*   **Google Cloud Dataproc** for executing PySpark transformation logic (if ksh scripts are converted to PySpark).

The target BigQuery schema will include the following tables:
*   `sof_ta_cntrct_crs2` (target table, corresponding to `sof$ta_cntrct_crs2` in Oracle)
*   `sof_ta_cntrct_crs` (source table, corresponding to `sof$ta_cntrct_crs` in Oracle)
*   `isbert_schema.dwtk_meldungen` (auxiliary table for date derivation) - this table will need to be migrated to BigQuery as well, retaining its schema and data.

## 4. Data Flow & Lineage
The original job's execution flow:
1.  **UC4 Job `DW.BERT_AUSD_V_TA_CNTRCT_CRS2.xml`**: Acts as the primary orchestrator, running on host `DWHDWH1P` using login `DW.UNIX.ISBERT`.
2.  **KornShell Wrapper `r_ausd_v_ta_cntrct_crs2.ksh`**: Invoked by the UC4 job. This script sets up the environment, handles logging, and calls the core controller script. It sources several utility scripts (`DW.HOLE_PFAD`, `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `DW.BERT_LESE_LOG`).
3.  **KornShell Controller `k_ausd_v_ta_cntrct_crs2.ksh`**: Invoked by the wrapper script. This script further sets up parameters, sources more utility scripts (`h_alis_job.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`), and crucially executes the Oracle SQL script.
4.  **Oracle SQL Script `d_ausd_v_ta_cntrct_crs2.sql`**: The core data transformation logic.
    *   **Reads from**: `isbert_schema.dwtk_meldungen` (to determine a date) and `sof$ta_cntrct_crs`.
    *   **Writes to**: `sof$ta_cntrct_crs2` (after truncating it).
    *   **Uses**: `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for table truncation.

**Migrated Data Flow (Airflow DAG):**

The UC4 job will be migrated to an Airflow DAG named `dw_bert_ausd_v_ta_cntrct_crs2`.
The ksh scripts will be transformed into a PySpark script (or potentially directly integrated BigQuery SQL) that is executed by a DataprocSubmitJobOperator.

The Airflow DAG will consist of:
*   A `start` task.
*   A `DataprocSubmitJobOperator` task named `run_bert_ausd_v_ta_cntrct_crs2` which will execute the translated PySpark/BigQuery SQL logic.
*   An `end` task.

The logical flow within the PySpark/BigQuery component will be:
1.  Derive the `v_datum` equivalent from the BigQuery table `isbert_schema.dwtk_meldungen`.
2.  Execute a `TRUNCATE TABLE` statement on the BigQuery target table `sof_ta_cntrct_crs2`.
3.  Execute an `INSERT INTO` statement to populate `sof_ta_cntrct_crs2` from `sof_ta_cntrct_crs`, applying the transformation logic.

## 5. Transformation Logic

The core transformation logic resides in `d_ausd_v_ta_cntrct_crs2.sql`, which will be converted to BigQuery SQL.

**Original Oracle SQL Logic:**
1.  **Date Derivation**: Determines `v_datum` by querying `isbert_schema.dwtk_meldungen` for the maximum `timecreated` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`, formatted as `YYYYMMDD`. If no date is found, defaults to `'19000101'`.
2.  **Truncate Target**: Clears the `sof$ta_cntrct_crs2` table using `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_crs2')`.
3.  **Insert Data**: Inserts selected columns into `sof$ta_cntrct_crs2` from a self-join of `sof$ta_cntrct_crs` (aliased as `c` and `cr`).
    *   The join condition is `c.cntrct_parent = cr.cntrct_id (+)`, which is an Oracle-specific syntax for a `LEFT JOIN`.
    *   A filter `cr.cntrct_ty (+) = 10` is applied to the right side of the join (parent contract type 10).
    *   A main filter `c.cntrct_ty <> 10` excludes contracts of type 10 from the main table (`c`).
    *   The `cr.contract_number` is aliased as `RV_NUM`.

**Migrated BigQuery SQL Logic:**

```sql
-- Derive v_datum equivalent
DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Clear target table
TRUNCATE TABLE `sof_ta_cntrct_crs2`;

-- Insert transformed data
INSERT INTO `sof_ta_cntrct_crs2` (
  cntrct_id,
  obj_version,
  contract_number,
  cntrct_template_id,
  cntrct_validity_id,
  valid_from,
  com_per_ext_rea_cv,
  billcycle_id,
  vo_code,
  cntrct_start_date,
  cntrct_st,
  cntrct_parent,
  cntrct_ty,
  cost_centre,
  cost_centre_user,
  commitment_reference_date,
  order_number,
  rv_num
)
SELECT
  c.cntrct_id,
  c.obj_version,
  c.contract_number,
  c.cntrct_template_id,
  c.cntrct_validity_id,
  c.valid_from,
  c.com_per_ext_rea_cv,
  c.billcycle_id,
  c.vo_code,
  c.cntrct_start_date,
  c.cntrct_st,
  c.cntrct_parent,
  c.cntrct_ty,
  c.cost_centre,
  c.cost_centre_user,
  c.commitment_reference_date,
  c.order_number,
  cr.contract_number AS rv_num
FROM `sof_ta_cntrct_crs` c
LEFT JOIN `sof_ta_cntrct_crs` cr
  ON c.cntrct_parent = cr.cntrct_id
 AND cr.cntrct_ty = 10
WHERE c.cntrct_ty <> 10;
```

**Key Transformations:**
*   Oracle outer join `(+)` replaced with standard `LEFT JOIN`.
*   Oracle `NVL` function replaced with BigQuery `COALESCE`.
*   Oracle `TO_CHAR(MAX(m.timecreated),'YYYYMMDD')` replaced with BigQuery `FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated)))`.
*   Oracle procedural `TRUNCATE TABLE` via `runstatement` replaced with direct BigQuery `TRUNCATE TABLE`.
*   SQL*Plus specific commands (`DEFINE`, `COLUMN`, `SPOOL`, `START`, `WHENEVER SQLERROR`, `PROMPT`, `COMMIT`) are removed as they are not applicable in BigQuery.

## 6. External Dependencies
*   **Original Oracle Database**: The source `sof$ta_cntrct_crs` and `isbert_schema.dwtk_meldungen` tables, along with the `isbert_schema.DWPA_UTIL_SKRIPT` package, are Oracle database objects. These will need to be migrated to BigQuery tables. The `DWPA_UTIL_SKRIPT.runstatement` call for truncation will be replaced by a native BigQuery `TRUNCATE TABLE` statement.
*   **File System**: The original ksh scripts perform file operations (e.g., sourcing other scripts, writing log files to `./tmp`). In the BigQuery environment, logging will be handled by Airflow/Dataproc mechanisms, and sourced scripts will be replaced by integrated logic within the PySpark script or BigQuery SQL.

## 7. Unresolved / Risks
*   **GCP Placeholders**: The Airflow DAG design includes placeholders for `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME`, and `YOUR_BUCKET_NAME`. These must be configured manually before deployment.
*   **No Explicit Schedule**: The UC4 job XML did not provide scheduling information (`EVNT_TIME` or `JOBP`), so the Airflow DAG is generated without a schedule (`schedule=None`). A schedule will need to be defined based on business requirements.
*   **UC4 Login/Host**: The UC4 job used `DW.UNIX.ISBERT` login and `DWHDWH1P` host. These security and execution context details need to be mapped to appropriate GCP IAM roles and Dataproc cluster configurations.
*   **Utility Scripts**: The numerous ksh utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, etc.) sourced by the original ksh scripts need to be reviewed. Their functionalities should either be integrated into the PySpark/BigQuery SQL logic or replaced with GCP-native equivalents (e.g., Cloud Logging for error messages). The current design assumes they are primarily environment setup and logging and can be handled within the PySpark wrapper logic or BigQuery SQL scripting.
*   **Table Schema Accuracy**: The SQL conversion assumes that the column names and data types inferred from the `INSERT` and `SELECT` statements in the Oracle SQL script accurately represent the underlying schema. A full schema migration will be required to confirm this.
*   **Parallel Hint**: The Oracle hint `/*+ parallel(c,4) parallel (cr, 4) */` is performance-related. BigQuery handles parallelism automatically, so this hint is removed in the translated SQL. Performance testing will be necessary.

## 8. Build Plan
The migration involves creating the following artifacts:

1.  **Airflow DAG (Python)**:
    *   **File Name**: `dw_bert_ausd_v_ta_cntrct_crs2_dag.py`
    *   **Language**: Python
    *   **Description**: Orchestrates the execution of the data transformation.
    *   **Content**: Based on the `uc4_to_airflow_dag_design` output, defining a `DataprocSubmitJobOperator` to run the PySpark script.

2.  **BigQuery SQL Script**:
    *   **File Name**: `d_ausd_v_ta_cntrct_crs2_bq.sql`
    *   **Language**: BigQuery SQL
    *   **Description**: Contains the core data transformation logic, derived from `d_ausd_v_ta_cntrct_crs2.sql`.
    *   **Content**: Based on the `hql_sql_to_bqsql_design` output, including `DECLARE`, `TRUNCATE TABLE`, and `INSERT INTO` statements.

3.  **PySpark Wrapper Script (Python)**:
    *   **File Name**: `r_ausd_v_ta_cntrct_crs2.py`
    *   **Language**: Python (PySpark)
    *   **Description**: A conceptual PySpark script that encapsulates the execution of the BigQuery SQL script and any necessary environment setup or logging that the original ksh scripts performed. This script will coordinate the BigQuery SQL execution.
    *   **Content**: Placeholder for the logic that executes the `d_ausd_v_ta_cntrct_crs2_bq.sql` in BigQuery, possibly handling parameters (`JobKennung`, `EintragsNr`) and logging.

**Order of Operations:**
1.  Migrate `isbert_schema.dwtk_meldungen` and `sof$ta_cntrct_crs` to BigQuery.
2.  Create the target BigQuery table `sof_ta_cntrct_crs2`.
3.  Develop and deploy the BigQuery SQL script `d_ausd_v_ta_cntrct_crs2_bq.sql`.
4.  Develop and deploy the PySpark wrapper script `r_ausd_v_ta_cntrct_crs2.py` to Google Cloud Storage.
5.  Develop and deploy the Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2_dag.py` to Cloud Composer.
6.  Configure GCP placeholders and schedule the Airflow DAG.