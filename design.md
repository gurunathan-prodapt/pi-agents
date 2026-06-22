# Migration Design — BERT_V_TA_DISC_ZUSGF

## 1. Purpose & Scope
The `BERT_V_TA_DISC_ZUSGF` job is responsible for concatenating discount descriptions from a source table (`sof$ta_discount`) and storing the aggregated information into a target table (`sof$ta_disc_zusgf`). This job's execution is currently orchestrated by UC4, which invokes shell scripts that, in turn, execute an Oracle PL/SQL script containing the core transformation logic. The migration aims to re-implement this entire workflow on Google Cloud Platform (GCP), using Apache Airflow for orchestration and Google BigQuery for all data storage and transformation.

## 2. Source Inventory

| File Path                                                                                                                   | Technology    | Purpose                                                                                                                                              | Complexity Tier | Automation Bucket |
| :-------------------------------------------------------------------------------------------------------------------------- | :------------ | :--------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------- | :---------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml` | UC4/Automic   | Orchestrates the concatenation of discount descriptions by executing a KornShell script.                                                             | medium          | semi_auto         |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh`                                       | KornShell     | Control script for `r_ausd_vertrag.ksh` that manages job execution, handles parameters, and orchestrates the execution of `d_ausd_v_ta_disc_zusgf.sql`. | medium          | semi_auto         |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh`                                       | KornShell     | Wrapper script (Rahmenskript) orchestrating the data reconciliation for `ta_disc_zusgf`, handling parameters, environment setup, logging, and errors. | medium          | retire            |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql`                                       | Oracle PL/SQL | Defines custom object types and a PL/SQL package with a pipelined table function to concatenate discount information into `SOF$TA_DISC_ZUSGF`.         | complex         | manual            |

## 3. Target Architecture

The migrated job will operate entirely within Google Cloud Platform, utilizing the following managed services:

*   **Orchestration:** Apache Airflow running on Cloud Composer to manage the workflow and task dependencies.
*   **Data Storage & Transformation:** Google BigQuery will serve as the primary data warehouse, handling both the storage of source and target tables and the execution of all transformation logic.
*   **Script Execution:** Python code within Airflow tasks will replace the shell script logic. If complex shell script logic requires distributed processing, Cloud Dataproc (running PySpark) could be an alternative, though Python operators are preferred for simpler orchestration.

The four-stage original architecture (UC4 -> Wrapper KSH -> Control KSH -> Oracle SQL) will be streamlined into an Airflow DAG that directly executes BigQuery SQL.

## 4. Data Flow & Lineage

**Original Data Flow:**
The UC4 job `DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml` invokes `r_ausd_v_ta_disc_zusgf.ksh`. This wrapper script then calls `k_ausd_v_ta_disc_zusgf.ksh`, which finally executes `d_ausd_v_ta_disc_zusgf.sql`.

The Oracle PL/SQL script `d_ausd_v_ta_disc_zusgf.sql` performs the following data operations:
*   **Reads from:**
    *   `isbert_schema.dwtk_meldungen` (to determine a date for processing)
    *   `sof$ta_discount` (contains raw discount information)
*   **Writes to:**
    *   `sof$ta_disc_zusgf` (stores the concatenated discount descriptions)

**Migrated Data Flow:**

1.  **Airflow DAG:** The UC4 job `DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml` will be converted into an Airflow DAG (`dw_bert_ausd_v_ta_disc_zusgf`). This DAG will define the overall workflow.
2.  **Shell Script Logic in Airflow:** The orchestration and parameter handling logic present in `r_ausd_v_ta_disc_zusgf.ksh` and `k_ausd_v_ta_disc_zusgf.ksh` will be re-implemented directly as Python functions or tasks within the Airflow DAG. This eliminates the need for intermediate shell scripts.
3.  **BigQuery SQL Transformation:** The core data transformation logic from `d_ausd_v_ta_disc_zusgf.sql` will be migrated to a BigQuery SQL script. This script will be executed directly by an Airflow BigQuery operator.

**Target Lineage (Conceptual):**

*   **Source Tables:** Migrated `isbert_schema.dwtk_meldungen` and `sof$ta_discount` tables residing in BigQuery.
*   **Transformation:** BigQuery SQL script (derived from `d_ausd_v_ta_disc_zusgf.sql`), orchestrated by Airflow.
*   **Target Table:** Migrated `sof$ta_disc_zusgf` table residing in BigQuery.

## 5. Transformation Logic

The primary data transformation occurs within `d_ausd_v_ta_disc_zusgf.sql`, which aggregates and concatenates discount descriptions.

**Original Oracle PL/SQL Logic:**
The script performs several steps:
*   Dynamically determines a processing date (`v_datum`) from `isbert_schema.dwtk_meldungen`.
*   Defines Oracle object types (`sof$ty_o_discount`, `sof$ty_t_discount`) and a PL/SQL package (`sof$sp_discount_functions`) containing a pipelined table function `concat_discounts`. This function groups discount records by `cntrct_id` and `cntrct_obj_version`, concatenating `rabatt || ' (' || rabatthoehe || '%)'` into a single `rabatt_alle` string, with an implicit length limit of 500 characters.
*   Truncates the target table `sof$ta_disc_zusgf`.
*   Inserts aggregated data into `sof$ta_disc_zusgf` by joining distinct contract vectors with the output of the `concat_discounts` function.

**Target BigQuery SQL Logic (Recommended Conversion):**
The Oracle-specific constructs (object types, PL/SQL packages, pipelined functions) will be replaced with standard BigQuery SQL features, primarily Common Table Expressions (CTEs) and the `STRING_AGG` aggregate function.

```sql
-- BigQuery SQL equivalent for d_ausd_v_ta_disc_zusgf.sql
-- All table names are placeholders and assume prior migration to BigQuery.

-- Step 1: Derive runtime date value. This variable can also be passed as an Airflow DAG parameter.
DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `your_gcp_project.your_bigquery_dataset.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 2: Clear target contents (if required by logic).
TRUNCATE TABLE `your_gcp_project.your_bigquery_dataset.sof_ta_disc_zusgf`;

-- Step 3: Populate target table with concatenated discount information.
INSERT INTO `your_gcp_project.your_bigquery_dataset.sof_ta_disc_zusgf` (
  cntrct_id,
  cntrct_obj_version,
  disc_vector_ty,
  rabatt_alle
)
WITH distinct_vectors AS (
  -- Identify unique contract-discount vector combinations
  SELECT DISTINCT
    cntrct_id,
    disc_vector_ty,
    cntrct_obj_version
  FROM `your_gcp_project.your_bigquery_dataset.sof_ta_discount`
),
prepared_discounts AS (
  -- Prepare the discount text for concatenation
  SELECT
    cntrct_id,
    cntrct_obj_version,
    CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)') AS rabatt_text
  FROM `your_gcp_project.your_bigquery_dataset.sof_ta_discount`
),
aggregated_discounts AS (
  -- Aggregate and concatenate the discount texts per contract
  SELECT
    cntrct_id,
    cntrct_obj_version,
    -- Apply SUBSTR for explicit length control if the 500-char limit is strict
    STRING_AGG(rabatt_text, ', ' ORDER BY rabatt_text) AS rabatt_alle
  FROM prepared_discounts
  GROUP BY
    cntrct_id,
    cntrct_obj_version
)
SELECT
  dv.cntrct_id,
  dv.cntrct_obj_version,
  dv.disc_vector_ty,
  ad.rabatt_alle
FROM distinct_vectors dv
LEFT JOIN aggregated_discounts ad
  ON dv.cntrct_id = ad.cntrct_id
 AND dv.cntrct_obj_version = ad.cntrct_obj_version;
```

**Key Conversion Details:**
*   **Data Types:** Oracle `NUMBER(10)` converts to BigQuery `INT64`. Oracle `VARCHAR2` types convert to BigQuery `STRING`. Date/timestamp functions like `NVL` and `TO_CHAR` are replaced by `COALESCE` and `FORMAT_DATE`/`FORMAT_TIMESTAMP` respectively.
*   **Pipelined Function:** The complex pipelined table function in Oracle is effectively replaced by a series of CTEs and the `STRING_AGG` function in BigQuery, which is designed for efficient string concatenation within groups.
*   **Join Syntax:** Oracle's `(+)` for outer joins is converted to explicit `LEFT JOIN` syntax.
*   **DDL & DML:** Oracle's `CREATE TYPE`, `CREATE PACKAGE` and `DROP TYPE` statements are removed as BigQuery uses a schema-on-read approach and does not have equivalent object types or procedural packages. `TRUNCATE TABLE` and `INSERT` statements are directly supported in BigQuery.
*   **Performance Hints:** Oracle-specific hints like `PARALLEL(tgt,4)` and `ALTER SESSION` commands are not applicable in BigQuery and will be removed; BigQuery automatically manages parallelism.

## 6. External Dependencies

The original job has a dependency on an **Oracle Database** which hosts the source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_discount`) and the target table (`sof$ta_disc_zusgf`). There is also a reference to `CARMEN DB` via a DB link, though its active usage in this specific SQL script is not evident.

**Replacement Strategy:**

*   **Oracle Database:** All required Oracle tables will be migrated to Google BigQuery.
    *   **Initial Data Load:** A one-time bulk load of historical data can be performed using Google Cloud services like Database Migration Service, Cloud Data Fusion, or custom ETL processes (e.g., Dataflow).
    *   **Ongoing Synchronization:** For continuous updates, Change Data Capture (CDC) mechanisms (e.g., from GoldenGate to Pub/Sub to Dataflow) or scheduled batch loads will be implemented to keep the BigQuery tables synchronized with the Oracle source until it is fully decommissioned.
*   **CARMEN DB:** If the `CARMEN DB` is a live dependency for other processes and its data is implicitly used, its relevant tables must also be migrated to BigQuery, or a secure and performant connection (e.g., using BigQuery federated queries with Cloud SQL for Oracle, or replicating data into BigQuery) must be established. For `BERT_V_TA_DISC_ZUSGF`, the `DEFINE v_carmen` variable is present but not actively used in the provided SQL, suggesting it might be an unused artifact or used in other parts of the larger system not covered here.

## 7. Unresolved / Risks

*   **Shell Script Business Logic:** While the shell scripts primarily handle orchestration, environment setup, and basic parameter parsing, a thorough review is needed to ensure no critical business logic is embedded within `f_alis_msgerr.ksh`, `h_alis_job.ksh`, etc., that isn't directly replicated by Airflow's native capabilities or Python helper functions. The `r_ausd_v_ta_disc_zusgf.ksh` is marked for `retire`, suggesting it might be fully absorbed into the Airflow DAG's structure.
*   **Dynamic Parameterization:** The UC4 variables (e.g., `&DWH_JOB_KENNUNG`) and shell script parameters (e.g., `$p_JobKennung`, `$p_EintragsNr`) need to be accurately identified and mapped to Airflow DAG parameters, variables, or XComs for seamless execution.
*   **Date Derivation Logic (`v_datum`):** The exact logic and dependencies for `v_datum` from `isbert_schema.dwtk_meldungen` must be fully understood and replicated in BigQuery. If `dwtk_meldungen` is a transient or highly dynamic table, its migration strategy is critical.
*   **`STRING_AGG` Length Limit:** BigQuery's `STRING_AGG` does not inherently enforce a maximum string length like `VARCHAR2(500)`. If the 500-character limit for `rabatt_alle` is a strict business requirement, `SUBSTR(STRING_AGG(...), 1, 500)` or similar explicit truncation must be added to the BigQuery SQL.
*   **Oracle `DWPA_UTIL_SKRIPT.runstatement`:** This is a procedural call to an Oracle utility. Its equivalent functionality in BigQuery is a direct `TRUNCATE TABLE` statement. Ensure no side effects or complex logic are part of `runstatement` that would be missed by a direct replacement.
*   **`../trace.sql.cfg` and Spooling:** The `START ../trace.sql.cfg` and `SPOOL ./tmp/...` commands for tracing and output capture will need to be replaced by Cloud Logging and Airflow logging mechanisms.

## 8. Build Plan

The build plan focuses on creating an Airflow DAG that orchestrates the BigQuery SQL transformation.

1.  **Migrate Oracle Tables to BigQuery:**
    *   **Action:** Create corresponding tables in BigQuery (e.g., `your_gcp_project.your_bigquery_dataset.dwtk_meldungen`, `your_gcp_project.your_bigquery_dataset.sof_ta_discount`, `your_gcp_project.your_bigquery_dataset.sof_ta_disc_zusgf`).
    *   **Language:** DDL for BigQuery.
    *   **Tools:** BigQuery DDL, Data Migration Service, Cloud Data Fusion, Dataflow.

2.  **Develop BigQuery SQL Transformation Script:**
    *   **Action:** Translate the Oracle PL/SQL in `d_ausd_v_ta_disc_zusgf.sql` into a BigQuery-compatible SQL script (`d_ausd_v_ta_disc_zusgf_bq.sql`).
    *   **Language:** BigQuery SQL.
    *   **Tools:** Manual conversion, potentially assisted by `hql_sql_to_bqsql_design`.

3.  **Develop Airflow DAG for Orchestration:**
    *   **Action:** Create an Airflow DAG (`dw_bert_ausd_v_ta_disc_zusgf.py`) to manage the end-to-end workflow.
        *   **Task 1: Parameter Initialization/Environment Setup:** Replicate relevant logic from `r_ausd_v_ta_disc_zusgf.ksh` and `k_ausd_v_ta_disc_zusgf.ksh` for parameter parsing and environment variable setup using Python operators.
        *   **Task 2: BigQuery SQL Execution:** Use `BigQueryOperator` or `BigQueryExecuteQueryOperator` to execute the `d_ausd_v_ta_disc_zusgf_bq.sql` script. Ensure dynamic values (like `v_datum`) are passed as templated parameters or query parameters.
        *   **Logging and Error Handling:** Integrate Cloud Logging and Airflow's native error handling and retry mechanisms.
    *   **Language:** Python (for Airflow DAG and Python operators).
    *   **Tools:** `uc4_to_airflow_dag_design` (as a guide for DAG structure), Airflow SDK.

**Estimated Output Files:**

*   `airflow/dags/dw_bert_ausd_v_ta_disc_zusgf.py`
*   `bigquery/sql/d_ausd_v_ta_disc_zusgf_bq.sql`