# Migration Design — DW.BERT_AUSD_V_TA_VERTRAG_TMP

## 1. Purpose & Scope
This document outlines the migration design for the ETL job `DW.BERT_AUSD_V_TA_VERTRAG_TMP`. The original job, managed by UC4, combines contract-related information from various source systems to populate a temporary contract table (`sof$ta_vertrag_tmp`). The migration will involve converting the UC4 job scheduler to an Airflow DAG and the core Oracle SQL transformation logic to BigQuery SQL. The intermediary KornShell wrapper scripts will be rendered obsolete.

The core business purpose is to prepare contract data (`Vertragsdatenabgleich`) from various sources for further reporting or processing within the data warehouse environment.

## 2. Source Inventory
The job is composed of the following files:

*   **`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_VERTRAG_TMP.xml`**
    *   **Technology:** UC4/Automic XML (Job definition)
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** Defines a Unix job that orchestrates the contract data preparation process.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh`**
    *   **Technology:** KornShell
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** Wrapper script for the `Vertragsdatenabgleich` process. Handles environment setup, parameter parsing, and calls the core control script.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh`**
    *   **Technology:** KornShell
    *   **Tier:** Medium
    *   **Automation Bucket:** Retire (B0) - *Note: This script is marked for retirement, indicating it will not be directly migrated but its function will be absorbed or eliminated.*
    *   **Summary:** Control script for data preparation. Handles parameter validation and orchestrates the execution of the main SQL script.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711/d_ausd_v_ta_vertrag_tmp.sql`**
    *   **Technology:** Oracle PL/SQL (SQL script)
    *   **Tier:** Medium
    *   **Automation Bucket:** Semi-Auto
    *   **Summary:** The core SQL script that populates the temporary contract table (`sof$ta_vertrag_tmp`) by joining and transforming data from various Oracle source tables and views.

## 3. Target Architecture

*   **Orchestration:** Apache Airflow on Google Cloud.
    *   A single Airflow DAG will replace the UC4 job.
    *   The DAG will orchestrate the execution of the BigQuery SQL transformation.
*   **Data Storage & Transformation:** Google BigQuery.
    *   All source Oracle tables/views will be replicated or migrated to BigQuery.
    *   The target table `sof$ta_vertrag_tmp` will be created as a BigQuery table.
    *   The SQL transformation logic will be converted from Oracle SQL to BigQuery SQL.
*   **Processing:** Google Cloud Dataproc (for PySpark, if intermediary ksh logic requires it).
    *   Initially, the UC4-to-Airflow tool suggested a PySpark job submission for the ksh script. However, given that the ksh scripts are primarily wrappers for SQL execution, it's more efficient to directly execute the migrated BigQuery SQL from Airflow using a `BigQueryOperator`. The `retire` bucket for `k_ausd_v_ta_vertrag_tmp.ksh` supports this simplification.

### Target Components & Layout

*   **Airflow DAG:** `dw_bert_ausd_v_ta_vertrag_tmp.py`
    *   Responsible for scheduling and executing the BigQuery SQL.
*   **BigQuery Dataset:** A dedicated dataset (e.g., `bert_dw_staging`) will house the migrated `sof$ta_vertrag_tmp` table.
*   **BigQuery Table:** `sof$ta_vertrag_tmp` (or a BigQuery equivalent name like `bert_ausd_v_ta_vertrag_tmp_staging`).
*   **BigQuery SQL Script:** A new `.sql` file containing the converted BigQuery logic.

## 4. Data Flow & Lineage

The current data flow is:
`UC4 Job (DW.BERT_AUSD_V_TA_VERTRAG_TMP.xml)`
  `-> calls r_ausd_v_ta_vertrag_tmp.ksh`
    `-> calls k_ausd_v_ta_vertrag_tmp.ksh`
      `-> executes d_ausd_v_ta_vertrag_tmp.sql`
        `-> Reads from: isbert_schema.dwtk_meldungen, sof$ta_cntrct_crs3, sof$ta_bp_ref, sof$ta_inv_acc, dwh$vi_s_rd_segment, sof$ta_notice, sof$ta_barrier_zusgf, sof$ta_cntrct_templ, sof$ta_cntrct_valid, sof$ta_period, sof$ta_vvl_upgrade, sof$ta_apn_ve, sof$ta_action_assoc, sof$vi_c_bfc`
        `-> Writes to: sof$ta_vertrag_tmp`

The migrated data flow will be:
`Airflow DAG (dw_bert_ausd_v_ta_vertrag_tmp)`
  `-> BigQueryOperator executing BigQuery SQL script`
    `-> Reads from: BigQuery tables corresponding to isbert_schema.dwtk_meldungen, sof$ta_cntrct_crs3, sof$ta_bp_ref, sof$ta_inv_acc, dwh$vi_s_rd_segment, sof$ta_notice, sof$ta_barrier_zusgf, sof$ta_cntrct_templ, sof$ta_cntrct_valid, sof$ta_period, sof$ta_vvl_upgrade, sof$ta_apn_ve, sof$ta_action_assoc, sof$vi_c_bfc` (e.g., `isbert_schema.dwtk_meldungen`, `sof_ta_cntrct_crs3`, etc., in BigQuery)
    `-> Writes to: BigQuery table bert_dw_staging.bert_ausd_v_ta_vertrag_tmp`

**Execution Order:**
1.  Airflow DAG `dw_bert_ausd_v_ta_vertrag_tmp` starts.
2.  A BigQuery task is initiated.
3.  The BigQuery SQL script first truncates the target table `bert_dw_staging.bert_ausd_v_ta_vertrag_tmp`.
4.  The script then executes the `INSERT INTO ... SELECT ... UNION ALL ...` statement, populating the target table.
5.  The BigQuery task completes, and the Airflow DAG concludes.

## 5. Transformation Logic
The core transformation logic is contained within `d_ausd_v_ta_vertrag_tmp.sql`.

### Oracle to BigQuery SQL Conversion Details:
*   **Truncate Table:** The `TRUNCATE TABLE sof$ta_vertrag_tmp` statement will be directly translated to BigQuery's `TRUNCATE TABLE \`project.dataset.sof$ta_vertrag_tmp\``.
*   **Variable `v_datum`:** The Oracle `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') ...` logic to derive `v_datum` will be converted to BigQuery using `DECLARE` and `SET` statements with `COALESCE` and `FORMAT_DATE/PARSE_DATE` functions.
*   **`INSERT INTO ... SELECT`:** The structure of the `INSERT INTO` statement and the two `SELECT` statements with `UNION ALL` will be preserved.
*   **Oracle Joins:** Oracle's proprietary `(+)` outer join syntax will be converted to explicit `LEFT JOIN` clauses in BigQuery.
*   **Functions:**
    *   `DECODE` will be converted to BigQuery's `CASE` statement.
    *   `NVL` will be converted to `COALESCE`.
    *   `TO_CHAR`, `TO_DATE`, `MONTHS_BETWEEN` will be converted to their BigQuery equivalents like `FORMAT_DATE`, `PARSE_DATE`, and `DATE_DIFF` respectively.
*   **Table References:** Oracle table names will be adapted to BigQuery's naming conventions and fully qualified with project and dataset (e.g., `isbert_schema.dwtk_meldungen` becomes \`project.dataset.isbert_schema_dwtk_meldungen\`).
*   **Comments & Directives:** Oracle-specific directives like `PROMPT`, `SPOOL`, `WHENEVER SQLERROR`, `START`, `DEFINE`, `COLUMN`, and `COMMIT` will be removed as they are not applicable in BigQuery SQL scripts run via Airflow.

## 6. External Dependencies
*   **Oracle Database:** The original job depends on an Oracle database for source tables (e.g., `sof$ta_cntrct_crs3`, `isbert_schema.dwtk_meldungen`) and the target table (`sof$ta_vertrag_tmp`).
    *   **Replacement:** All referenced Oracle source tables and views must be migrated or replicated to BigQuery. The target table `sof$ta_vertrag_tmp` will become a native BigQuery table.
*   **UC4 Scheduler:** The job is scheduled and managed by UC4.
    *   **Replacement:** An Airflow DAG will replace the UC4 scheduling.
*   **KornShell Environment:** The job executes within a KornShell environment.
    *   **Replacement:** The direct execution of the BigQuery SQL script via Airflow eliminates the need for the KornShell wrappers. Environment variables will be handled through Airflow variables or BigQuery scripting parameters.

## 7. Unresolved / Risks
*   **`k_ausd_v_ta_vertrag_tmp.ksh` Retirement:** The `retire` bucket for `k_ausd_v_ta_vertrag_tmp.ksh` implies its logic may no longer be necessary or will be handled differently. This needs explicit confirmation. If there's any critical logic beyond calling the SQL script (e.g., specific error handling, logging, or parameter manipulation not present in the SQL), it needs to be incorporated into the Airflow DAG or the BigQuery SQL. Based on the current analysis, it appears to be a pure wrapper.
*   **`v_datum` derivation:** The source `v_datum` is derived from `isbert_schema.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`. The exact semantics of `BERT_DROP_TEMP_TABLE` and its implications for the job's execution date logic need to be confirmed to ensure the BigQuery translation of `v_datum` is functionally equivalent.
*   **Database Links:** The `DEFINE v_carmen = "@pcrs1"` suggests a potential database link to a `pcrs1` database. This implies data could be sourced from another Oracle instance.
    *   **Risk:** If `pcrs1` refers to an external Oracle system, its data sources will also need to be identified and migrated/replicated to BigQuery. The current SQL does not explicitly use `v_carmen` in its `FROM` clause, but this needs verification.
*   **Performance Tuning:** The Oracle SQL uses `/*+ parallel(c,4) ... */` hints. While BigQuery automatically handles parallelism, the BigQuery SQL should be reviewed for optimal performance and cost.
*   **Missing Scheduling Information:** The UC4 XML did not provide scheduling details (`EVNT_TIME` file). The Airflow DAG will be initially created with no schedule (`schedule=None`), requiring manual configuration based on the original UC4 schedule.

## 8. Build Plan
1.  **Migrate Source Oracle Tables to BigQuery:**
    *   Identify all source tables and views used in `d_ausd_v_ta_vertrag_tmp.sql`.
    *   Migrate these tables from Oracle to BigQuery, establishing appropriate schema and data types. Ensure `isbert_schema.dwtk_meldungen` is migrated.
2.  **Create Target BigQuery Table:**
    *   Define the schema for `bert_dw_staging.bert_ausd_v_ta_vertrag_tmp` in BigQuery, mirroring the output structure of the original SQL.
3.  **Generate BigQuery SQL Script:**
    *   Use the output from `hql_sql_to_bqsql_design` to create the BigQuery SQL script (`dw_bert_ausd_v_ta_vertrag_tmp_transform.sql`).
    *   Review and refine the generated BigQuery SQL for correctness, performance, and BigQuery best practices. Pay special attention to the `v_datum` derivation and `MONTHS_BETWEEN` conversions.
4.  **Develop Airflow DAG:**
    *   Use the output from `uc4_to_airflow_dag_design` as a template.
    *   Create a Python Airflow DAG file (`dw_bert_ausd_v_ta_vertrag_tmp.py`).
    *   Implement a `BigQueryOperator` to execute the BigQuery SQL script.
    *   Configure `GCP_PROJECT_ID`, `DATAPROC_REGION`, `DATAPROC_CLUSTER_NAME` (if PySpark is ever needed), and `GCS_BUCKET_NAME` placeholders.
    *   Determine and configure the actual schedule for the DAG.
    *   The `retries` and `retry_delay` can be set based on standard practices or specific requirements.
5.  **Testing:**
    *   Unit test the BigQuery SQL transformation.
    *   Integrate and end-to-end test the Airflow DAG with the BigQuery SQL.
    *   Validate data integrity and transformation accuracy against the legacy system.
6.  **Deployment:**
    *   Deploy the Airflow DAG to the production Airflow environment.
    *   Deploy the BigQuery SQL script and table definitions.