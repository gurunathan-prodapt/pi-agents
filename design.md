# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh

## 1. Purpose & Scope
This migration design document outlines the strategy for migrating the ETL job identified by `job_id: 5af228f1`, originating from the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh`, to Google Cloud Platform (GCP) with BigQuery as the target data warehouse.

The original job serves as a control script (`k_ausd_v_ta_barrier_zusgf.ksh`) that orchestrates the execution of an Oracle PL/SQL script (`d_ausd_v_ta_barrier_zusgf.sql`). The core business purpose of the SQL script is to process barrier-related information, specifically to concatenate and aggregate barrier details (such as `sperrart`, `sperrgrund`, and `stilllegungszeitraum`) for each contract (`cntrct_id`) from a source table (`sof$ta_barrier`) and store the summarized results in a target table (`sof$ta_barrier_zusgf`). The KSH script also handles job entry management and error reporting.

The scope of this migration includes converting the KornShell orchestration logic into an Airflow DAG and translating the Oracle PL/SQL data transformation logic into BigQuery SQL.

## 2. Source Inventory

### 2.1. k_ausd_v_ta_barrier_zusgf.ksh
*   **Technology:** KornShell (KSH)
*   **Summary:** This script acts as a job controller. It sources several utility shell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`), parses command-line parameters (`-j` for JobKennung, `-f` for EintragsNr), and crucially, invokes an Oracle SQL script (`d_ausd_v_ta_barrier_zusgf.sql`) via a `starteSQLSkript` function. It also handles basic error checking and exit codes.
*   **Complexity Tier:** `medium`
*   **Migration Bucket:** `semi_auto`

### 2.2. d_ausd_v_ta_barrier_zusgf.sql
*   **Technology:** Oracle PL/SQL
*   **Summary:** This script is responsible for the core data processing. It defines an Oracle package `sof$sp_table_functions` containing a pipelined table function `concat_barriers`. This function concatenates barrier attributes for each `cntrct_id`. The script first truncates the target table `sof$ta_barrier_zusgf` and then inserts processed data into it. The source data for this processing comes primarily from `sof$ta_barrier`. An additional lookup on `isbert_schema.dwtk_meldungen` is used to determine a date parameter, though its direct impact on the main `INSERT` query in the provided code is indirect/commented out for table naming.
*   **Inputs:** `sof$ta_barrier`, `isbert_schema.dwtk_meldungen`
*   **Outputs:** `sof$ta_barrier_zusgf`
*   **Complexity Tier:** `medium` (inferred from parent job)
*   **Migration Bucket:** `semi_auto` (inferred from parent job)

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services.

*   **Orchestration:** Apache Airflow on Cloud Composer.
*   **Data Warehouse:** BigQuery.
*   **Data Transformation:** BigQuery SQL (standard SQL).
*   **Source Data Landing:** Source Oracle tables (`sof$ta_barrier`, `isbert_schema.dwtk_meldungen`) will be migrated/replicated to BigQuery. This typically involves a data ingestion layer (e.g., CDC using Datastream or batch loads). We assume these source tables will exist in a BigQuery dataset (e.g., `raw_zone` or `staging_dataset`) before this job runs.
*   **Target Tables:** The target table `sof$ta_barrier_zusgf` will be created in a BigQuery dataset (e.g., `transformed_zone` or `reporting_dataset`).

### 3.1. BigQuery Dataset Layout
*   `[project_id].[source_dataset].sof_ta_barrier`: Migrated `sof$ta_barrier` table.
*   `[project_id].[source_dataset].dwtk_meldungen`: Migrated `isbert_schema.dwtk_meldungen` table.
*   `[project_id].[target_dataset].sof_ta_barrier_zusgf`: Target table for processed barrier data.

## 4. Data Flow & Lineage

The data flow in the target BigQuery environment will be as follows:

1.  **Orchestration:** An Airflow DAG will manage the execution of the BigQuery transformation.
2.  **Date Parameter Retrieval (Optional/Simplified):** The original KSH script's date logic, deriving `v_datum` from `isbert_schema.dwtk_meldungen`, might be simplified or removed if the date is passed as an Airflow DAG parameter or derived dynamically within BigQuery if strictly needed for filtering/partitioning. For this design, we assume this logic can be integrated directly into the SQL if required or managed by Airflow.
3.  **Truncate Target Table:** A BigQuery `TRUNCATE TABLE` statement will clear the `[project_id].[target_dataset].sof_ta_barrier_zusgf` table. This will be an independent BigQuery operator in the Airflow DAG.
4.  **Data Transformation and Load:** The core logic, derived from `d_ausd_v_ta_barrier_zusgf.sql`, will be executed as a BigQuery `INSERT` statement using the transformed SQL. This statement will read from `[project_id].[source_dataset].sof_ta_barrier` and insert into `[project_id].[target_dataset].sof_ta_barrier_zusgf`. This will be the primary BigQuery operator in the Airflow DAG.

**Overall Lineage (Conceptual):**
`[project_id].[source_dataset].dwtk_meldungen` (READS)
`[project_id].[source_dataset].sof_ta_barrier` (READS)
  → BigQuery SQL Transformation (equivalent of `d_ausd_v_ta_barrier_zusgf.sql`)
  → `[project_id].[target_dataset].sof_ta_barrier_zusgf` (WRITES)

## 5. Transformation Logic

The core transformation logic from `d_ausd_v_ta_barrier_zusgf.sql` will be re-implemented in BigQuery Standard SQL, as provided by the `hql_sql_to_bqsql_design` tool.

### 5.1. k_ausd_v_ta_barrier_zusgf.ksh (Orchestration to Airflow)
The KSH script's primary functions:
*   **Environment setup:** `. $HOME/.dw_init` and sourcing utility scripts will be replaced by Airflow task dependencies, Python operators, or BigQuery configurations.
*   **Parameter parsing:** Parameters like `p_JobKennung` and `p_EintragsNr` will be passed as Airflow DAG parameters or XComs.
*   **Error handling:** KSH error handling (`f_alis_msgerr.ksh`) will be replaced by Airflow's built-in error handling, retry mechanisms, and alerting.
*   **SQL script execution:** The `starteSQLSkript` call will be replaced by a BigQueryOperator executing the converted SQL.

### 5.2. d_ausd_v_ta_barrier_zusgf.sql (Oracle PL/SQL to BigQuery SQL)

The transformation logic will be directly translated into a BigQuery SQL script. The key elements of the conversion are:

*   **Oracle Package and Pipelined Table Function:** The `CREATE OR REPLACE PACKAGE sof$sp_table_functions` and its `concat_barriers` pipelined function will be replaced by BigQuery's `STRING_AGG` function within `GROUP BY` clauses, combined with Common Table Expressions (CTEs) for clarity.
*   **`TRUNCATE TABLE`:** The Oracle `TRUNCATE TABLE sof$ta_barrier_zusgf` will be directly translated to BigQuery `TRUNCATE TABLE \`project_id.target_dataset.sof_ta_barrier_zusgf\``.
*   **Data Selection and Transformation:**
    *   **Source Data:** `sof$ta_barrier` will be mapped to `\`project_id.source_dataset.sof_ta_barrier\`.
    *   **Date Derivation:** The `SELECT NVL(TO_CHAR(MAX(m.timecreated),\'YYYYMMDD\'),\'19000101\')` from `dwtk_meldungen` will be handled as an Airflow variable or a separate BigQuery SQL query if the date is a critical parameter for filtering. Given the final SQL doesn't use `&v_datum` in table names, this logic might be redundant or for logging/auditing.
    *   **String Manipulation:** Oracle functions like `REPLACE`, `LENGTH`, `LTRIM`, `DECODE`, `TO_CHAR` will be converted to their BigQuery SQL equivalents (`REPLACE`, `LENGTH`, `LTRIM`, `CASE WHEN`, `FORMAT_DATE`, `CONCAT`).
    *   **Concatenation Logic:** The complex `LOOP` and `PIPE ROW` logic in the Oracle pipelined function will be replaced by `STRING_AGG` with appropriate `ORDER BY` and `GROUP BY cntrct_id`. The logic for `sperrgrund_zusgf` will use `CASE WHEN` and conditional aggregation (`COUNTIF`).
    *   **Parallel Hints:** Oracle's `/* PARALLEL(tgt,4) */` and `--+ PARALLEL(bar,4)` hints will be removed as BigQuery handles parallelism automatically.

**BigQuery SQL Code (as generated by CM MCP):**

```sql
TRUNCATE TABLE `sof$ta_barrier_zusgf`;

INSERT INTO `sof$ta_barrier_zusgf`
  (cntrct_id,
   sperrart_alle,
   sperrgrund_alle,
   stilllegungszeitraum_alle,
   sperrgrund_zusgf)
WITH barrier_src AS (
  SELECT DISTINCT
    CAST(cntrct_id AS INT64) AS cntrct_id,
    REPLACE(REPLACE(sperrart, 'Rufnummern', ''), ' ', '') AS sperrart,
    sperrgrund,
    CASE
      WHEN ist_stillegung = 1 THEN
        CASE
          WHEN sperr_ende IS NULL THEN
            CONCAT('ab ', FORMAT_DATE('%d.%m.%Y', DATE(sperr_beginn)))
          ELSE
            CONCAT(
              FORMAT_DATE('%d.%m.%Y', DATE(sperr_beginn)),
              ' - ',
              FORMAT_DATE('%d.%m.%Y', DATE(sperr_ende))
            )
        END
      ELSE NULL
    END AS stilllegungszeitraum_alle,
    CASE
      WHEN barrier_reason_cv = 2 THEN 2
      ELSE 3
    END AS sperrgrund_zusgf
  FROM `sof$ta_barrier`
),
agg AS (
  SELECT
    cntrct_id,
    STRING_AGG(sperrart, ',' ORDER BY sperrart) AS sperrart_alle,
    STRING_AGG(sperrgrund, ',' ORDER BY sperrart) AS sperrgrund_alle,
    STRING_AGG(stilllegungszeitraum_alle, ', ' ORDER BY sperrart) AS stilllegungszeitraum_alle,
    CASE
      WHEN COUNTIF(sperrgrund_zusgf != 2) > 0 THEN 3
      ELSE 2
    END AS sperrgrund_zusgf
  FROM barrier_src
  GROUP BY cntrct_id
)
SELECT
  cntrct_id,
  sperrart_alle,
  sperrgrund_alle,
  stilllegungszeitraum_alle,
  sperrgrund_zusgf
FROM agg;
```

## 6. External Dependencies
The `lineage_assembled_jobs` record showed no explicit `external_systems` for this job. However, the source code implies:

*   **Oracle Database:** The primary data source and target are Oracle tables. This will be replaced by BigQuery tables in the target architecture. Data ingestion from Oracle to BigQuery (e.g., using Datastream for CDC or batch ETL tools) must be established prior to this job's migration.
*   **File System:** The KSH script uses local file system operations (`. $HOME/.dw_init`, `tmpFile`, `spool ./tmp/trace_d_ausd_v_ta_barrier_zusgf`). These will be replaced:
    *   Environment variable sourcing: Managed by Airflow environment configuration.
    *   Temporary files: Replaced by BigQuery's temporary tables, CTEs, or potentially Cloud Storage if intermediate files are truly needed for complex multi-step processes (unlikely for this specific SQL).
    *   Spooling/Tracing: Replaced by Airflow logging, Cloud Logging, and BigQuery job history.
*   **Custom Utilities:** The KSH script sources several custom utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). These will either be:
    *   Replaced by Airflow's native features (e.g., parameter passing, error handling).
    *   Rewritten as Python functions within the Airflow DAG if their logic is complex and cannot be replaced by native Airflow/BigQuery features.
    *   The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for truncating tables will be replaced by a direct BigQuery `TRUNCATE TABLE` statement.

## 7. Unresolved / Risks

*   **Missing Lineage Details:** The automated lineage extraction (`lineage_edges`) did not explicitly capture the `INVOKES` relationship between `k_ausd_v_ta_barrier_zusgf.ksh` and `d_ausd_v_ta_barrier_zusgf.sql`, nor the `READS`/`WRITES` for the SQL script. This required manual inspection and inference. This indicates a potential gap in the automated lineage data, which might require further manual validation for other similar jobs.
*   **`v_datum` Usage:** The determination of `v_datum` from `isbert_schema.dwtk_meldungen` was present in the SQL script but not directly used in the final `INSERT` statement for table names (only commented out references to `&v_datum` in older modifications). A detailed review with the business or source system experts is needed to confirm if this date parameter is still relevant and how it should be handled in BigQuery. If it's for auditing/logging, it can be handled outside the core transformation.
*   **Source Data Latency:** The migration assumes that `sof$ta_barrier` and `isbert_schema.dwtk_meldungen` are available and up-to-date in BigQuery. The chosen data ingestion method for these tables must align with the job's requirements for data freshness.
*   **Performance Tuning:** While BigQuery handles parallelism automatically, the `STRING_AGG` with `ORDER BY` over a large number of grouped records might require performance tuning in BigQuery, such as optimizing data distribution or using appropriate clustering/partitioning.
*   **`sperrgrund_zusgf` Aggregation:** The `CASE WHEN COUNTIF(sperrgrund_zusgf != 2) > 0 THEN 3 ELSE 2` logic correctly translates the original code's intent where `r_out.sperrgrund_zusgf := 3;` if any `r_bar.sperrgrund_zusgf != 2`. This should be verified with business logic if the default is always `2` when no other condition is met.

## 8. Build Plan

The build plan will involve creating an Airflow DAG for orchestration and a BigQuery SQL script for the data transformation.

1.  **BigQuery DDL for Target Table:**
    *   **Language:** BigQuery DDL
    *   **Output:** `create_sof_ta_barrier_zusgf.sql`
    *   **Description:** Create the target table `[project_id].[target_dataset].sof_ta_barrier_zusgf` with appropriate data types inferred from the Oracle source and the transformed BigQuery SQL output. Columns would be `cntrct_id` (INT64), `sperrart_alle` (STRING), `sperrgrund_alle` (STRING), `stilllegungszeitraum_alle` (STRING), `sperrgrund_zusgf` (INT64).

2.  **BigQuery Transformation SQL:**
    *   **Language:** BigQuery SQL
    *   **Output:** `transform_sof_ta_barrier_zusgf.sql`
    *   **Description:** This file will contain the generated BigQuery SQL for truncating and inserting data into `sof_ta_barrier_zusgf`, as provided in Section 5.2. This SQL will replace the logic from `d_ausd_v_ta_barrier_zusgf.sql`.

3.  **Airflow DAG for Orchestration:**
    *   **Language:** Python
    *   **Output:** `k_ausd_v_ta_barrier_zusgf_dag.py`
    *   **Description:** This DAG will orchestrate the execution.
        *   **Task 1 (`truncate_target_table`):** A `BigQueryOperator` to execute `TRUNCATE TABLE \`project_id.target_dataset.sof_ta_barrier_zusgf\``.
        *   **Task 2 (`load_transformed_data`):** A `BigQueryOperator` to execute the `transform_sof_ta_barrier_zusgf.sql` script.
        *   **Dependencies:** `truncate_target_table` >> `load_transformed_data`.
        *   **Parameter Handling:** If the `v_datum` logic from the KSH script is deemed necessary, it can be handled by an initial PythonOperator that queries `dwtk_meldungen` and pushes the date to XCom, which is then used by the BigQueryOperator for filtering or dynamic table naming (if applicable).
        *   **Error Handling/Logging:** Leverage Airflow's native capabilities.
