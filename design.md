# Migration Design — DW.BERT_AUSD_V_TA_CNTRCT_CRS2

## 1. Purpose & Scope
This job, `DW.BERT_AUSD_V_TA_CNTRCT_CRS2`, is responsible for updating contract data, specifically reconciling and populating the `sof$ta_cntrct_crs2` table. The process involves truncating the existing data in `sof$ta_cntrct_crs2` and then re-populating it by selecting and joining data from `sof$ta_cntrct_crs`. A key aspect of the logic is filtering out parent contracts (Rahmenverträge) and enriching the contract records with parent contract numbers where applicable. The job is orchestrated by an UC4 scheduler, which triggers a series of KornShell and SQL scripts to perform the data manipulation.

## 2. Source Inventory
The job is composed of four primary components:

*   **UC4 Job Definition (Orchestration Layer)**
    *   **File:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_CRS2.xml`
    *   **Technology:** UC4/Automic
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** Defines the job schedule and entry point, executing the wrapper KornShell script.

*   **Wrapper KornShell Script (Environment & Control)**
    *   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** Acts as the main entry point from UC4. It handles parameter parsing, environment setup (`. $HOME/.dw_init`), logging, and error trapping, then calls the core KornShell script. It also sources common utility scripts like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh`.

*   **Core KornShell Script (Execution Orchestration)**
    *   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh`
    *   **Technology:** KornShell
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** This script manages the execution flow. It reads job parameters, sources SQL*Plus utility functions (`h_alis_sqlplus.ksh`), and crucially, calls an Oracle procedure `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` to `TRUNCATE TABLE sof$ta_cntrct_crs2`. Finally, it executes the main Oracle SQL script (`d_ausd_v_ta_cntrct_crs2.sql`).

*   **Oracle SQL Script (Data Transformation)**
    *   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs2.sql`
    *   **Technology:** Oracle SQL
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** This script contains the primary data manipulation logic. It performs an `INSERT INTO ... SELECT FROM` operation. It reads data from `sof$ta_cntrct_crs`, joins it with itself to identify parent contracts, filters out certain contract types (`c.cntrct_ty <> 10`), and populates `sof$ta_cntrct_crs2`. It also dynamically determines a `v_datum` from `isbert_schema.dwtk_meldungen` and makes use of an Oracle DB-Link `@pcrs1` for potential cross-database access.

## 3. Target Architecture
The target platform is Google BigQuery.
The migration will involve:

*   **Orchestration:** The UC4 job will be converted into a Google Cloud Composer (Airflow) DAG. This DAG will manage the execution of the migrated components.
*   **Data Processing:** The core SQL logic will be translated into BigQuery Standard SQL.
*   **Shell Script Logic:** The orchestration, environment setup, and error handling logic within the KornShell scripts will be re-implemented using Python within the Airflow DAG or as separate Python operators/tasks, interacting directly with BigQuery.
*   **Tables:**
    *   `sof$ta_cntrct_crs` (source) will be migrated to a BigQuery table, e.g., `project_id.source_dataset.ta_cntrct_crs`.
    *   `sof$ta_cntrct_crs2` (target) will be migrated to a BigQuery table, e.g., `project_id.dwh_dataset.ta_cntrct_crs2`.
    *   `isbert_schema.dwtk_meldungen` (lookup) will be migrated to a BigQuery table, e.g., `project_id.source_dataset.dwtk_meldungen`.
*   **Procedures/Functions:** The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` functionality for truncating tables will be replaced by equivalent BigQuery DDL operations within Python operators in the Airflow DAG.

## 4. Data Flow & Lineage

1.  **UC4 Job (Airflow DAG):** The Airflow DAG will be the top-level orchestrator.
2.  **Wrapper/Core KornShell Logic (Python Operators/Tasks in Airflow):**
    *   A Python task will handle environment variables and logging initialization similar to `. $HOME/.dw_init`.
    *   A Python task will execute a BigQuery DDL statement to `TRUNCATE TABLE project_id.dwh_dataset.ta_cntrct_crs2`. This replaces the `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` call.
    *   A Python task will extract `v_datum` from `project_id.source_dataset.dwtk_meldungen` by executing a BigQuery SQL query.
3.  **Oracle SQL (BigQuery SQL Task):** A BigQueryOperator task within the Airflow DAG will execute the translated BigQuery SQL.
    *   **Input:** `project_id.source_dataset.ta_cntrct_crs`, `project_id.source_dataset.dwtk_meldungen`
    *   **Output:** `project_id.dwh_dataset.ta_cntrct_crs2` (overwritten)
    *   **Transformations:** Data from `ta_cntrct_crs` is processed, self-joined for parent contract information, filtered by `cntrct_ty`, and inserted into `ta_cntrct_crs2`.

**Execution Order:**
UC4 Job (Airflow DAG) -> Python Task (Env/Logging) -> Python Task (Truncate `ta_cntrct_crs2`) -> Python Task (Get `v_datum`) -> BigQueryOperator (Execute transformed SQL)

## 5. Transformation Logic

**Original SQL (Oracle Extract):**
```sql
-- Truncate table sof$ta_cntrct_crs2
begin
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_cntrct_crs2');  
end;
/

-- Get v_datum
SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
  FROM isbert_schema.dwtk_meldungen m
 WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

-- Main INSERT INTO SELECT
INSERT  INTO sof$ta_cntrct_crs2(
        cntrct_id, obj_version, contract_number, cntrct_template_id, cntrct_validity_id,
        valid_from, com_per_ext_rea_cv, billcycle_id, vo_code, cntrct_start_date,
        cntrct_st, cntrct_parent, cntrct_ty, cost_centre, cost_centre_user,
        commitment_reference_date, order_number, rv_num)
   SELECT /*+ parallel(c,4) parallel (cr, 4) */
        c.cntrct_id, c.obj_version, c.contract_number, c.cntrct_template_id, c.cntrct_validity_id,
        c.valid_from, c.com_per_ext_rea_cv, c.billcycle_id, c.vo_code, c.cntrct_start_date,
        c.cntrct_st, c.cntrct_parent, c.cntrct_ty, c.cost_centre, c.cost_centre_user,
        c.commitment_reference_date, c.order_number,
        cr.contract_number RV_NUM
   FROM
        sof$ta_cntrct_crs      c,
        sof$ta_cntrct_crs      cr
   WHERE
        c.cntrct_parent = cr.cntrct_id (+)
   AND
        cr.cntrct_ty (+)        =  10   -- RV
   AND  c.cntrct_ty             <> 10;
```

**Migrated Logic (BigQuery Standard SQL & Python for Orchestration - conceptual):**
The `TRUNCATE` operation will be handled by a PythonOperator calling BigQuery DDL. The `v_datum` retrieval will also be a PythonOperator or directly embedded if simple enough. The core `INSERT ... SELECT` will be migrated as follows:

```sql
-- BigQuery SQL for `execute_main_sql` Airflow Task
INSERT INTO `project_id.dwh_dataset.ta_cntrct_crs2` (
        cntrct_id, obj_version, contract_number, cntrct_template_id, cntrct_validity_id,
        valid_from, com_per_ext_rea_cv, billcycle_id, vo_code, cntrct_start_date,
        cntrct_st, cntrct_parent, cntrct_ty, cost_centre, cost_centre_user,
        commitment_reference_date, order_number, rv_num)
SELECT
        c.cntrct_id, c.obj_version, c.contract_number, c.cntrct_template_id, c.cntrct_validity_id,
        c.valid_from, c.com_per_ext_rea_cv, c.billcycle_id, c.vo_code, c.cntrct_start_date,
        c.cntrct_st, c.cntrct_parent, c.cntrct_ty, c.cost_centre, c.cost_centre_user,
        c.commitment_reference_date, c.order_number,
        cr.contract_number AS RV_NUM
   FROM
        `project_id.source_dataset.ta_cntrct_crs` AS c
        LEFT OUTER JOIN `project_id.source_dataset.ta_cntrct_crs` AS cr
          ON c.cntrct_parent = cr.cntrct_id
   WHERE
        -- For LEFT OUTER JOIN, cr.cntrct_ty will be NULL if no match.
        -- The original Oracle condition 'cr.cntrct_ty (+) = 10' means "if there's a match, cr.cntrct_ty must be 10, otherwise it's still an outer join and cr.cntrct_ty would be NULL".
        -- And 'c.cntrct_ty <> 10' always applies.
        (cr.cntrct_ty = 10 OR cr.cntrct_id IS NULL)
   AND  c.cntrct_ty <> 10;
```
*Note:* The `parallel` hint will be removed as BigQuery handles parallelism automatically. The Oracle `(+)` outer join syntax has been converted to an explicit `LEFT OUTER JOIN` in BigQuery. The `WHERE` clause logic for `cr.cntrct_ty` was carefully translated to ensure equivalent behavior with the `LEFT OUTER JOIN`.

## 6. External Dependencies

*   **Oracle Database:** The primary external dependency is the Oracle database hosting `sof$ta_cntrct_crs`, `sof$ta_cntrct_crs2`, `isbert_schema.dwtk_meldungen`, and the `isbert_schema.DWPA_UTIL_SKRIPT` procedure.
    *   **Replacement Strategy:** These tables will be migrated to BigQuery as `project_id.source_dataset.ta_cntrct_crs`, `project_id.dwh_dataset.ta_cntrct_crs2`, and `project_id.source_dataset.dwtk_meldungen` respectively.
    *   **DB-Link (`@pcrs1`):** The Oracle DB-Link `@pcrs1` likely connects to another Oracle instance (referred to as "Carmen DB"). This needs further investigation. If `sof$ta_cntrct_crs` already consolidates data from this external system, its migration to BigQuery should suffice. If the Carmen DB is a live operational system that needs to be accessed directly, a separate BigQuery ingestion pipeline (e.g., Change Data Capture, batch exports from source) will be required for the Carmen DB data. Assuming `sof$ta_cntrct_crs` holds all necessary data, this dependency will be resolved by the migration of that table.
    *   **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`:** This Oracle procedure used for truncating tables will be replaced by a BigQuery DDL command (`TRUNCATE TABLE`) executed via a PythonOperator within the Airflow DAG.

## 7. Unresolved / Risks

*   **UC4 Includes (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`):** The exact content and purpose of these UC4 includes (`:inc`) are unknown. They need to be analyzed to determine if they contain critical logic, variables, or environment settings that need to be replicated in Airflow.
*   **KornShell Utilities:** The full functionality of the sourced KornShell utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) needs to be thoroughly analyzed. Each piece of logic (environment variable setting, date calculations, custom error handling, `starteSQLSkript` function) must be re-implemented robustly in Python within the Airflow DAG.
*   **Error Handling and Logging:** The legacy KornShell error handling (`DWMSG_MeldeFehler`, `trap`) is extensive. A comprehensive error handling and alerting strategy using Airflow's native capabilities (e.g., email alerts, Slack notifications) and cloud-native logging (e.g., Cloud Logging) must be designed and implemented.
*   **SQL `START ../trace.sql.cfg` and `SPOOL ./tmp/trace_d_ausd_v_ta_cntrct_crs2`:** These Oracle-specific commands for tracing and spooling will be removed. Equivalent BigQuery job monitoring and Stackdriver logging will replace this functionality.
*   **Dynamic `v_datum` Variable:** The logic for determining `v_datum` from `isbert_schema.dwtk_meldungen` is crucial and must be accurately translated. It will likely involve a separate BigQuery query in a PythonOperator.
*   **Data Type and Schema Fidelity:** Ensure that the data types and nullability constraints of the migrated BigQuery tables precisely match the Oracle source to prevent data integrity issues.
*   **Performance:** While BigQuery optimizes parallelism automatically, the performance of the migrated SQL should be thoroughly tested and optimized for large datasets.
*   **Testing Coverage:** The migration of orchestration logic from UC4 and KornShell to Airflow and Python introduces new components that require robust unit and integration testing.

## 8. Build Plan

1.  **Migrate Oracle Source Tables to BigQuery:**
    *   **Description:** Perform initial data load and set up ongoing synchronization for `sof$ta_cntrct_crs`, `isbert_schema.dwtk_meldungen` (and potentially data from `@pcrs1` if `sof$ta_cntrct_crs` does not encompass it).
    *   **Tool:** Google Cloud Data Migration Service (DMS) for Oracle to BigQuery, or custom Dataflow/Spark jobs for complex transformations during ingestion.
    *   **Output:** BigQuery tables: `project_id.source_dataset.ta_cntrct_crs`, `project_id.source_dataset.dwtk_meldungen`.
    *   **Language:** N/A (service configuration/SQL)

2.  **Translate Oracle SQL to BigQuery Standard SQL:**
    *   **Description:** Convert the core data transformation SQL in `d_ausd_v_ta_cntrct_crs2.sql` to BigQuery Standard SQL, adapting syntax (e.g., outer join, `NVL`, `TO_CHAR`).
    *   **Tool:** Manual conversion, potentially aided by Google Cloud SQL Translation.
    *   **Output:** `d_ausd_v_ta_cntrct_crs2.bqsql` (BigQuery SQL file).
    *   **Language:** BigQuery Standard SQL

3.  **Develop Airflow DAG for Orchestration:**
    *   **Description:** Create a Python-based Airflow DAG to manage the job's execution flow, replacing the UC4 and KornShell orchestration.
    *   **Tool:** Manual development using Python in Cloud Composer.
    *   **Output:** `dw_bert_ausd_v_ta_cntrct_crs2_dag.py` (Airflow DAG Python file).
    *   **Language:** Python
    *   **Tasks (conceptual):**
        *   `start_job`: `airflow.operators.dummy.DummyOperator`
        *   `truncate_target_table`: `airflow.operators.python.PythonOperator` executing `TRUNCATE TABLE \`project_id.dwh_dataset.ta_cntrct_crs2\``.
        *   `get_v_datum`: `airflow.operators.python.PythonOperator` executing a BigQuery query to fetch `v_datum` and pushing it to XCom.
        *   `execute_main_sql`: `airflow.providers.google.cloud.operators.bigquery.BigQueryOperator` executing `d_ausd_v_ta_cntrct_crs2.bqsql`, potentially using Jinja templating for variables like `v_datum`.
        *   `end_job`: `airflow.operators.dummy.DummyOperator`

4.  **Re-implement Shell Script Utilities in Python:**
    *   **Description:** Analyze and re-implement critical functionalities from the KornShell utility scripts (e.g., environment setup, parameter handling, custom logging, error trapping) into reusable Python functions or directly within the Airflow task logic.
    *   **Tool:** Manual development using Python.
    *   **Output:** Python utility modules, integrated logic in DAG tasks.
    *   **Language:** Python

5.  **Testing and Validation:**
    *   **Description:** Implement comprehensive testing to ensure functional equivalence and data accuracy.
    *   **Tool:** Pytest for Python code, Great Expectations or custom SQL scripts for data validation, Airflow testing utilities for DAG integrity.
    *   **Output:** Test reports, validated BigQuery data.
    *   **Language:** Python, BigQuery Standard SQL

