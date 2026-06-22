# Migration Design — DW.BERT_AUSD_V_TA_CNTRCT_CRS3

## 1. Purpose & Scope
This job is responsible for updating contract data, specifically including information about twin-bills, into the `sof$ta_cntrct_crs3` table. The source system uses a UC4 job to orchestrate KornShell scripts, which in turn execute an Oracle SQL script for the data transformation. The migration aims to re-implement this functionality on Google Cloud Platform, utilizing BigQuery for data processing and Airflow for orchestration. The scope includes the conversion of the Oracle SQL logic to BigQuery SQL, replacement of the KornShell orchestration with Airflow DAGs, and handling of external dependencies and logging.

## 2. Source Inventory

| Relative Path                                                                                                   | Technology  | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                                                           |
| :-------------------------------------------------------------------------------------------------------------- | :---------- | :----- | :---------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml`                                   | UC4/Automic | medium | semi_auto         | UC4 job definition for a Unix job that executes a KornShell script to update contracts.                                                                                                                                                                                                                                             |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh`                           | KornShell   | medium | semi_auto         | This ksh script acts as a control script for 'r_ausd_vertrag.ksh', handling job activation/deactivation, parsing parameters, and orchestrating the execution of an SQL script.                                                                                                                                                            |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh`                           | KornShell   | medium | semi_auto         | This KornShell script acts as a wrapper for synchronizing contract data into the 'ta_cntrct_crs3' table. It handles parameter parsing, environment setup, error logging, and orchestrates the execution of a core data processing script.                                                                                                    |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs3.sql`                           | Oracle SQL  | medium | semi_auto         | This SQL script truncates and populates the `SOF$TA_CNTRCT_CRS3` table with contract data, including twinbill information, by joining and unioning data from `SOF$TA_CNTRCT_CRS2`. It also retrieves a date from `DWTK_MELDUNGEN` for variable substitution. |

## 3. Target Architecture
The migrated solution will primarily leverage Google Cloud Platform services:
*   **Orchestration:** Apache Airflow on Cloud Composer will replace the UC4 scheduler and KornShell wrapper scripts.
*   **Data Processing:** Google BigQuery will replace the Oracle database for data storage and transformation.
*   **Logging/Monitoring:** Cloud Logging and Cloud Monitoring will be used for operational visibility.

The transformed data will reside in a BigQuery dataset, likely corresponding to the original Oracle schema (e.g., `isbert_schema` and a `sof` dataset).

## 4. Data Flow & Lineage

The original job execution flow is:
1.  **UC4 Job `DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml`**: Initiates the process.
2.  **`r_ausd_v_ta_cntrct_crs3.ksh`**: Wrapper script, sets up environment and calls `k_ausd_v_ta_cntrct_crs3.ksh`.
3.  **`k_ausd_v_ta_cntrct_crs3.ksh`**: Control script, handles parameters, logging, and executes `d_ausd_v_ta_cntrct_crs3.sql`.
4.  **`d_ausd_v_ta_cntrct_crs3.sql`**: Oracle SQL script, performs data transformation.

The data dependencies are:
*   **Reads from:**
    *   `isbert_schema.dwtk_meldungen` (to get `v_datum` substitution variable)
    *   `sof$ta_cntrct_crs2` (source for contract data)
*   **Writes to:**
    *   `sof$ta_cntrct_crs3` (target table for processed contract data)

The migrated data flow will be orchestrated by an Airflow DAG:
1.  **Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs3`**: Triggered on a schedule (to be determined, as UC4 schedule was not derivable).
2.  **Airflow Task (PythonOperator/BigQueryOperator)**: Emulates the logic of `r_ausd_v_ta_cntrct_crs3.ksh` and `k_ausd_v_ta_cntrct_crs3.ksh`. This task will handle environment variables, parameter passing (e.g., via Airflow XComs or template variables), and logging.
3.  **BigQueryOperator**: Executes the converted BigQuery SQL for `d_ausd_v_ta_cntrct_crs3.sql`.

## 5. Transformation Logic

The core transformation logic resides in `d_ausd_v_ta_cntrct_crs3.sql`. This Oracle SQL script:
*   **Retrieves `v_datum`**: From `isbert_schema.dwtk_meldungen` based on `job_kennung = 'BERT_DROP_TEMP_TABLE'`. This will be replaced by a BigQuery equivalent, potentially a separate SQL query or part of the main DML.
*   **Truncates target table**: `TRUNCATE TABLE sof$ta_cntrct_crs3`. This will map directly to `TRUNCATE TABLE` in BigQuery.
*   **Inserts data into `sof$ta_cntrct_crs3`**:
    *   It uses a `UNION` of two `SELECT` statements, both joining `sof$ta_cntrct_crs2` with itself (`c` and `ctb` aliases) to identify twin-bill information.
    *   It uses Oracle-specific `(+)` for `LEFT JOIN` and filters on `cntrct_ty` (contract type) to exclude specific types (10, 20) and include type 20 for twin-bill.
    *   The BigQuery conversion will involve:
        *   Replacing `NVL` with `IFNULL` or `COALESCE`.
        *   Replacing `TO_CHAR(date, 'YYYYMMDD')` with `FORMAT_DATE('%Y%m%d', DATE(...))`.
        *   Converting Oracle's `(+)` outer join syntax to standard `LEFT JOIN`.
        *   Removing SQL*Plus directives (`DEFINE`, `COLUMN`, `START`, `SPOOL`, `WHENEVER SQLERROR`, `PROMPT`, `COMMIT`).
        *   Adjusting table references to BigQuery dataset.table format (e.g., `` `project.dataset.table` ``).

**Converted BigQuery SQL (High-level outline):**

```sql
DECLARE v_datum STRING;

SET v_datum = (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

TRUNCATE TABLE `sof_dataset.ta_cntrct_crs3`;

INSERT INTO `sof_dataset.ta_cntrct_crs3` (...)
SELECT
  c.cntrct_id,
  -- ... other columns
  'TB' AS twinbill,
  ctb.cntrct_id AS twin_vertrag_id
FROM `sof_dataset.ta_cntrct_crs2` c
LEFT JOIN `sof_dataset.ta_cntrct_crs2` ctb
  ON c.cntrct_id = ctb.cntrct_parent
 AND ctb.cntrct_ty = 20
WHERE c.cntrct_ty NOT IN (10, 20)

UNION DISTINCT

SELECT
  ctb.cntrct_id,
  -- ... other columns
  'TB' AS twinbill,
  c.cntrct_id AS twin_vertrag_id
FROM `sof_dataset.ta_cntrct_crs2` c
JOIN `sof_dataset.ta_cntrct_crs2` ctb
  ON c.cntrct_id = ctb.cntrct_parent
WHERE ctb.cntrct_ty = 20
  AND c.cntrct_ty NOT IN (10, 20);
```

## 6. External Dependencies

The original job has the following external dependencies:
*   **Oracle Database:** Source tables `sof$ta_cntrct_crs2`, `sof$ta_cntrct_crs3`, `isbert_schema.dwtk_meldungen`. This will be migrated to Google BigQuery.
*   **UNIX Host `DWHDWH1P`:** Where the KornShell scripts execute. This execution environment will be replaced by Cloud Composer/Airflow workers.
*   **Login `DW.UNIX.ISBERT`:** The UNIX user for execution. This will be replaced by a GCP Service Account with appropriate BigQuery and Cloud Composer permissions.
*   **DB-Link `@pcrs1` (Carmen DB):** Referenced in the SQL (`DEFINE v_carmen = "@pcrs1"`). This indicates a potential cross-database query. In BigQuery, this would typically be handled by either:
    *   Migrating the Carmen DB data to BigQuery as well and performing direct joins.
    *   Using BigQuery federated queries (e.g., to Cloud SQL if Carmen DB remains external but accessible).
    *   Data replication (e.g., using DataStream or Fivetran) to bring Carmen DB data into BigQuery. The current SQL does not explicitly use `v_carmen` in the `FROM` clause, so it might be a placeholder or used in a part not shown. Further investigation is needed.
*   **Utility KornShell Scripts**: Scripts like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` are sourced by the KornShell control scripts. These will need to be re-implemented as Python functions/modules within the Airflow environment for logging, error handling, date manipulation, and parameter parsing.

## 7. Unresolved / Risks

*   **UC4 Schedule:** The UC4 export did not contain schedule information (`EVNT_TIME` file). The Airflow DAG will initially be given a placeholder schedule, which needs to be confirmed with business stakeholders.
*   **KornShell Utility Script Equivalents:** The exact logic and functionality of the sourced KornShell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`) need to be thoroughly analyzed and re-implemented in Python for the Airflow environment.
*   **`v_carmen` DB-Link Usage:** While defined, `v_carmen` is not explicitly used in the provided SQL. If it's used dynamically or in other parts of the script not shown, its migration strategy for cross-database access needs clarification.
*   **Error Handling and Logging:** The KornShell scripts include custom error handling and logging (`DWMSG_MeldeFehler`, `DWMSG_Logdateiname`, etc.). These need to be accurately translated to Airflow's logging mechanisms and potentially custom error handling in Python.
*   **Oracle Specifics:** The Oracle `PARALLEL` hint and `/* +append */` hint in the `INSERT` statement need to be reviewed for BigQuery equivalents or if they are still relevant/necessary in the BigQuery context. BigQuery handles parallelism automatically, and `INSERT` behavior might differ.
*   **Data Types:** While the design addresses general data type conversions, a detailed schema mapping is required to ensure precise data type conversion between Oracle and BigQuery for all involved tables and columns.

## 8. Build Plan

The build plan outlines the steps to create the BigQuery tables and Airflow DAG for this job.

1.  **Define BigQuery Schemas:**
    *   Create BigQuery datasets (`isbert_schema_target`, `sof_dataset_target` or similar naming convention).
    *   Define target table schemas for `dwtk_meldungen`, `ta_cntrct_crs2`, and `ta_cntrct_crs3` in BigQuery, mirroring the Oracle source as closely as possible, with appropriate BigQuery data types.

2.  **Migrate Data (Initial Load):**
    *   Extract historical data from `isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs2` from Oracle.
    *   Load extracted data into the corresponding BigQuery tables (`isbert_schema_target.dwtk_meldungen`, `sof_dataset_target.ta_cntrct_crs2`).

3.  **Develop BigQuery SQL Transformation:**
    *   Convert `d_ausd_v_ta_cntrct_crs3.sql` to BigQuery SQL, incorporating the changes identified in Section 5.
    *   Test the BigQuery SQL independently using sample data.

4.  **Develop Airflow DAG (`dw_bert_ausd_v_ta_cntrct_crs3.py`):**
    *   **DAG Definition:**
        *   `dag_id`: `dw_bert_ausd_v_ta_cntrct_crs3`
        *   `start_date`: Placeholder, to be determined.
        *   `schedule`: Placeholder, to be determined.
        *   `default_args`: Define owner, retries, etc.
    *   **Tasks:**
        *   `extract_v_datum_task`: A `BigQueryExecuteQueryOperator` or `PythonOperator` to retrieve the `v_datum` from `isbert_schema_target.dwtk_meldungen`. This value should be pushed to XComs.
        *   `truncate_and_insert_task`: A `BigQueryExecuteQueryOperator` to execute the converted BigQuery SQL. This task will pull `v_datum` from XComs and use it in the SQL query (if necessary, e.g., if `v_datum` needs to be passed as a variable to the SQL).
    *   **Replace KornShell Utilities:** Implement Python functions or operators for logging, error handling, and parameter management that replace the functionalities of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`.

5.  **Deploy and Test Airflow DAG:**
    *   Deploy the Airflow DAG to Cloud Composer.
    *   Perform unit, integration, and end-to-end testing of the DAG, verifying data accuracy and job execution.

6.  **Monitoring and Alerting:**
    *   Configure Cloud Monitoring alerts for DAG failures, task retries, and execution durations.
    *   Ensure proper logging to Cloud Logging.