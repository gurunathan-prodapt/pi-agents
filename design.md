# Migration Design — DW.BERT_AUSD_BP_TA_APN_VERTRAG

## 1. Purpose & Scope

The job `DW.BERT_AUSD_BP_TA_APN_VERTRAG` is responsible for the preparation of instantiated base products. Specifically, it processes Access Point Name (APN) and contract reference data, aggregates this information, and stores it in a target table. The original implementation involves a UC4 scheduler triggering a KornShell script, which in turn executes an Oracle PL/SQL script. The scope of this migration is to re-platform this entire workflow to Google Cloud Platform, using BigQuery for data storage and transformation, and Airflow for orchestration.

## 2. Source Inventory

This job comprises three core components:

| File Path                                                                                                   | Technology        | Tool          | Summary                                                                                                                                                                                                                                                              | Tier     | Automation Bucket |
| :---------------------------------------------------------------------------------------------------------- | :---------------- | :------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------- | :---------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/.../DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml`                            | UC4               | UC4/Automic   | UC4 JOBS_UNIX object defining the job `DW.BERT_AUSD_BP_TA_APN_VERTRAG`, responsible for orchestrating the execution of a KornShell script.                                                                                                                    | medium   | semi_auto         |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh`                     | Shell             | KornShell     | A control script parsing parameters, setting up the environment, performing validation checks, and orchestrating the execution of a SQL script (`d_ausd_bp_ta_apn_vertrag.sql`) to process data.                                                                   | medium   | semi_auto         |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql`                     | SQL               | Oracle PL/SQL | An Oracle PL/SQL script that processes APN and contract reference data from a source table, aggregates them, and inserts the results into a target table using a cursor-based loop for row-by-row processing.                                                      | complex  | manual            |

## 3. Target Architecture

The migrated job will leverage Google Cloud Platform services:

*   **Orchestration**: Apache Airflow on Cloud Composer will replace UC4. The UC4 job will be converted into an Airflow Directed Acyclic Graph (DAG).
*   **Data Storage & Transformation**: Google BigQuery will replace the Oracle database. All source and target tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_apn`, `sof$ta_apn_vertrag`) will be migrated to BigQuery datasets and tables.
*   **Execution Environment**: The KornShell script's logic will be translated into a Python script executed within an Airflow `PythonOperator`. The Oracle PL/SQL will be converted to BigQuery Standard SQL and executed using an Airflow `BigQueryOperator` or a Python script leveraging the BigQuery client library.

## 4. Data Flow & Lineage

The original job has a hierarchical execution and data flow:

1.  **UC4 (`DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml`)**: Triggers the KornShell script.
2.  **KornShell (`k_ausd_bp_ta_apn_vertrag.ksh`)**:
    *   Initializes environment, sources utility scripts.
    *   Parses input parameters (JobKennung, Stichtag, EintragsNr, wiederanlaufWert).
    *   Performs date validation.
    *   Executes the Oracle PL/SQL script (`d_ausd_bp_ta_apn_vertrag.sql`), passing parameters.
    *   Handles record counts.
3.  **Oracle PL/SQL (`d_ausd_bp_ta_apn_vertrag.sql`)**:
    *   **Reads from**: `isbert_schema.dwtk_meldungen` (for `v_datum`), `sof$ta_bpr_apn` (source data for aggregation).
    *   **Transforms**: Iteratively processes `sof$ta_bpr_apn` by `cntrct_id`, concatenating `access_point_name` and `cntrct_id_ref` into comma-separated strings.
    *   **Writes to**: `sof$ta_apn_vertrag` (target aggregated data).
    *   Uses Oracle-specific utilities like `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for `TRUNCATE` and `SPR_SCHEMA.SPR$PA_ANALYZE.ANALYZE_OBJECTS` (commented out).

**Target Data Flow (Airflow/BigQuery):**

1.  **Airflow DAG (`dw_bert_ausd_bp_ta_apn_vertrag`)**:
    *   A `PythonOperator` will encapsulate the control logic of `k_ausd_bp_ta_apn_vertrag.ksh`. This operator will prepare parameters and dynamically construct/execute the BigQuery SQL.
    *   The BigQuery SQL for `d_ausd_bp_ta_apn_vertrag.sql` will be executed by a `BigQueryOperator` or via the BigQuery Python client.

**Execution Order in Airflow:**

`start_task` (Airflow DAG start)
`->` `execute_control_script` (PythonOperator implementing ksh logic)
`->` `execute_bq_sql` (BigQueryOperator executing transformed SQL)
`->` `end_task` (Airflow DAG end)

## 5. Transformation Logic

### 5.1. UC4 XML to Airflow DAG

The `DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml` UC4 job will be transformed into an Airflow DAG.
*   **DAG ID**: `dw_bert_ausd_bp_ta_apn_vertrag`.
*   **Schedule**: `None` (as no UC4 schedule was provided; will require manual definition or external trigger).
*   **Task**: A single primary task, `run_main_process`, will be defined. This task will be a `PythonOperator` that executes the transformed ksh logic.

### 5.2. KornShell (`k_ausd_bp_ta_apn_vertrag.ksh`) to Python Script (Airflow PythonOperator)

The KornShell script's functionalities will be reimplemented in a Python script executed by an Airflow `PythonOperator`.
*   **Environment Initialization**: The sourcing of `. $HOME/.dw_init` and other utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) will be replaced by Python equivalents. This may involve using Airflow Variables/Connections for configuration, Python's `datetime` module for date calculations, and custom Python functions for parameter validation and error handling.
*   **Parameter Parsing**: `getopts` logic will be translated to Python's `argparse` or handled directly via Airflow DAG parameters.
*   **SQL Execution**: The `starteSQLSkript` function call will be replaced by a BigQuery client execution of the migrated BigQuery SQL, passing required parameters as Jinja templates or Python variables.
*   **Record Count Handling**: The logic for capturing and reporting record counts via `$tmpFile` will be adapted to Python, potentially logging to Airflow logs or pushing to XComs.

### 5.3. Oracle PL/SQL (`d_ausd_bp_ta_apn_vertrag.sql`) to BigQuery Standard SQL

The Oracle PL/SQL script, which is currently procedural and uses row-by-row processing, will be converted to BigQuery Standard SQL using set-based operations for efficiency.

*   **`DECLARE v_datum`**: The logic to derive `v_datum` from `isbert_schema.dwtk_meldungen` will be translated to a BigQuery `DECLARE` statement using `FORMAT_DATE` and `COALESCE`.
    ```sql
    DECLARE v_datum STRING DEFAULT (
      SELECT COALESCE(
        FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))),
        '19000101'
      )
      FROM `isbert_schema.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    ```
*   **`TRUNCATE TABLE sof$ta_apn_vertrag`**: Will become `TRUNCATE TABLE \`dataset.sof_ta_apn_vertrag\``.
*   **Procedural Loop to `STRING_AGG`**: The core aggregation logic from the `FOR rec_cn IN (...) LOOP` will be replaced by `STRING_AGG` within a `GROUP BY` clause.
    ```sql
    INSERT INTO `sof_ta_apn_vertrag` (cntrct_id, apn_list, cntrct_ref_list)
    SELECT
      cntrct_id,
      SUBSTR(TRIM(TRAILING ', ' FROM STRING_AGG(access_point_name, ', ' ORDER BY access_point_name)), 1, 100) AS apn_list,
      SUBSTR(TRIM(TRAILING ', ' FROM STRING_AGG(cntrct_id_ref, ', ' ORDER BY cntrct_id_ref)), 1, 100) AS cntrct_ref_list
    FROM `sof_ta_bpr_apn`
    GROUP BY cntrct_id
    ORDER BY cntrct_id;
    ```
*   **Oracle-specific functions**: `substr`, `rtrim`, `length`, `NVL`, `TO_CHAR` will be mapped to BigQuery equivalents (`SUBSTR`, `TRIM`, `LENGTH`, `COALESCE`, `FORMAT_DATE`).
*   **Oracle Utilities**: `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` and `SPR_SCHEMA.SPR$PA_ANALYZE.ANALYZE_OBJECTS` will be handled as direct DDL/DML statements or removed if their function is not required in BigQuery.

## 6. External Dependencies

The original job has a strong dependency on an Oracle database.

*   **Oracle Database (Input/Output)**:
    *   `isbert_schema.dwtk_meldungen`
    *   `sof$ta_bpr_apn`
    *   `sof$ta_apn_vertrag`
    These Oracle tables will be migrated to BigQuery. The schema names (`isbert_schema`, `sof`) will be mapped to BigQuery datasets, and the table names will remain similar, adjusted for BigQuery naming conventions if necessary (e.g., lowercase, hyphens instead of underscores if preferred).
    **Replacement Strategy**: BigQuery tables in a dedicated data warehouse project.
*   **Oracle Connection (`@pcrs1`)**: This Oracle connection alias will be replaced by implicit BigQuery connections within the GCP environment, authenticated via service accounts.
    **Replacement Strategy**: GCP Service Account with BigQuery Data Editor/Viewer roles.
*   **UNIX Host (`DWHDWH2P`) and Login (`DW.UNIX.ISBERT`)**: The execution context will shift from a specific UNIX host with a dedicated login to the managed environment of Cloud Composer (Airflow workers) and BigQuery, authenticated via a GCP service account.
    **Replacement Strategy**: Cloud Composer environment, GCP Service Account.

## 7. Unresolved / Risks

*   **UC4 Includes (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`)**: The exact functionality of these UC4 include files is unknown. They likely contain common path definitions and logging configurations.
    *   **Resolution**: These need to be analyzed. Path definitions can be translated to Airflow Variables or environment variables in the Python script. Logging can be integrated with Airflow's native logging to Cloud Logging.
*   **KornShell Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, etc.)**: The helper scripts sourced by `k_ausd_bp_ta_apn_vertrag.ksh` contain shared logic that must be re-implemented in Python.
    *   **Resolution**: These should be carefully reviewed and Python equivalents developed or integrated into the Airflow PythonOperator's logic.
*   **KornShell Commented Code**: The `sed`, `sort`, `join` commands in the ksh script are currently commented out. If they represent dormant functionality that might be activated, their migration would require additional design.
    *   **Resolution**: Confirm with business owners if this functionality is ever used. If so, it would translate to data manipulation steps in BigQuery SQL or Python/Pandas within the Airflow task.
*   **Error Handling**: The `WHENEVER SQLERROR EXIT FAILURE` in Oracle PL/SQL needs proper handling in BigQuery and Airflow.
    *   **Resolution**: Airflow tasks inherently fail on unhandled exceptions in Python or SQL errors in BigQuery, which aligns with this behavior. Retries and alerts can be configured at the Airflow task level.
*   **Dynamic `v_datum` from `dwtk_meldungen`**: The dependency on `dwtk_meldungen` for `v_datum` suggests a control table for job execution dates.
    *   **Resolution**: This pattern is directly translatable to BigQuery SQL as shown in the design. Ensure `dwtk_meldungen` is also migrated and correctly populated.

## 8. Build Plan

The migration will involve building the following components:

1.  **Airflow DAG (`dw_bert_ausd_bp_ta_apn_vertrag.py`)** - Python
    *   Defines the overall workflow, dependencies, and task parameters.
    *   Will contain a `PythonOperator` for the control logic and `BigQueryOperator` (or `PythonOperator` for BigQuery client calls) for the SQL transformation.

2.  **Control Logic Python Script (`k_ausd_bp_ta_apn_vertrag_wrapper.py`)** - Python
    *   This script will be called by the Airflow `PythonOperator`.
    *   It will implement the parameter parsing, environment setup, and date validation logic originally found in `k_ausd_bp_ta_apn_vertrag.ksh`.
    *   It will construct and execute the BigQuery SQL transformation.

3.  **BigQuery SQL Transformation Script (`d_ausd_bp_ta_apn_vertrag_bq.sql`)** - BigQuery Standard SQL
    *   Contains the `DECLARE`, `TRUNCATE TABLE`, and `INSERT INTO` statements with `STRING_AGG` and `GROUP BY` logic derived from `d_ausd_bp_ta_apn_vertrag.sql`.
    *   This script will be executed by the control logic Python script or directly by a `BigQueryOperator` in the DAG.