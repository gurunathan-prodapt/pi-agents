# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh

## 1. Purpose & Scope

This document outlines the migration design for the ETL job primarily driven by `k_ausd_bp_ta_msisdn.ksh`. The job's core purpose is to extract and prepare MSISDN (Mobile Station International Subscriber Directory Number) related basis product data for the BERT system. This involves executing an Oracle SQL script that reads from historical MSISDN data and an event log, processes it, and writes the results to a current MSISDN table. The `k_ausd_bp_ta_msisdn.ksh` script acts as a sub-orchestrator, invoked by a higher-level script, `r_ausd_bp_ta_msisdn.ksh`, to manage parameters, environment, and error handling before executing the core SQL logic.

The scope of this migration includes the `k_ausd_bp_ta_msisdn.ksh` script, its invoking script `r_ausd_bp_ta_msisdn.ksh`, and the embedded Oracle SQL script `d_ausd_bp_ta_msisdn.sql`, along with any directly referenced utility scripts. The target platform for this migration is Google Cloud BigQuery for data storage and transformation, orchestrated by Airflow.

## 2. Source Inventory

| File Name (Relative Path)                                             | Technology   | Complexity Tier | Automation Bucket | Purpose/Summary                                                                                                                                                                                                                                                                                              |
| :-------------------------------------------------------------------- | :----------- | :-------------- | :---------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh` | KornShell    | Medium          | semi_auto         | Core script. Parses parameters, performs date checks, invokes `d_ausd_bp_ta_msisdn.sql` to process MSISDN basis product data. Interacts with utility scripts for error handling, date functions, and parameter parsing.                                                                                             |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh` | KornShell    | Medium          | semi_auto         | Orchestration script. Sets up the environment, handles command-line arguments (`-s` Stichtag, `-l` Wiederanlaufwert), logs job status, and invokes `k_ausd_bp_ta_msisdn.ksh` with specific parameters. Provides usage information.                                                                            |
| `vobs/dw_source/isrpt/isbert/SQL/aufbereitung/sql/d_ausd_bp_ta_msisdn.sql` | Oracle SQL*Plus |                 |                   | Data transformation script. Truncates and inserts into `sof$ta_msisdn` from `sof$ta_msisdn_his` based on maximum valid_to date. Uses `isbert_schema.dwtk_meldungen` for date determination. Contains Oracle-specific syntax and hints. |

**Associated Utility Scripts (Inferred from KSH content):**

*   `. $HOME/.dw_init`: Environment initialization.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling and messaging.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utility functions.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing utility.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus wrapper utility.
*   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: Calculates yesterday's date.

## 3. Target Architecture

The migrated solution will leverage Google Cloud Platform (GCP) services:

*   **Data Storage & Transformation:** Google BigQuery. All Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_msisdn_his`, `sof$ta_msisdn`) will be migrated to corresponding BigQuery tables within a designated dataset (e.g., `isbert_raw`, `isbert_staging`, `isbert_curated`).
*   **Orchestration:** Cloud Composer (managed Airflow). The shell scripts will be re-engineered into Python-based Airflow DAGs.
*   **Logging & Monitoring:** Cloud Logging and Cloud Monitoring, integrated with Airflow.

**BigQuery Table Mapping:**

| Source Oracle Table            | Target BigQuery Table (Example)       | Description                                        |
| :----------------------------- | :------------------------------------ | :------------------------------------------------- |
| `isbert_schema.dwtk_meldungen` | `project.isbert_raw.dwtk_meldungen` | Source table for date determination.               |
| `sof$ta_msisdn_his`            | `project.isbert_raw.sof_ta_msisdn_his` | Historical MSISDN data, primary input for ETL.     |
| `sof$ta_msisdn`                | `project.isbert_curated.sof_ta_msisdn` | Target table for processed MSISDN data.            |

## 4. Data Flow & Lineage

The original job involves a hierarchical execution:

1.  **`r_ausd_bp_ta_msisdn.ksh` (Orchestrator):**
    *   Receives command-line parameters (`-s Stichtag`, `-l Wiederanlaufwert`).
    *   Sets up environment and logging.
    *   Invokes `k_ausd_bp_ta_msisdn.ksh` with parameters (`-j JobKennung`, `-s p_stichtag`, `-f DW_EintragsNr`, `-l p_wiederanlaufWert`).
    *   Handles overall job status logging.

2.  **`k_ausd_bp_ta_msisdn.ksh` (Sub-Orchestrator):**
    *   Receives parameters from `r_ausd_bp_ta_msisdn.ksh`.
    *   Sources utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
    *   Executes `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh` to get dates.
    *   Executes the Oracle SQL script `d_ausd_bp_ta_msisdn.sql` using `starteSQLSkript` (from `h_alis_sqlplus.ksh`).
    *   Captures record counts into a temporary file.

3.  **`d_ausd_bp_ta_msisdn.sql` (Data Transformer):**
    *   **Reads from:**
        *   `isbert_schema.dwtk_meldungen` (to determine `v_datum`).
        *   `sof$ta_msisdn_his` (for the main data processing).
    *   **Writes to:**
        *   `sof$ta_msisdn` (after truncation).

**Migrated Data Flow (Airflow DAG):**

1.  **Airflow DAG `r_ausd_bp_ta_msisdn_dag`:**
    *   **Start Task:** Initializes Airflow variables, parameters (Stichtag, Wiederanlaufwert), and logging.
    *   **Date Calculation Task:** Replaces `gestern.ksh` to calculate relevant dates (e.g., using Python's `datetime` or Airflow's built-in macros).
    *   **BigQuery Truncate Task:** Executes a BigQuery DDL statement to truncate `project.isbert_curated.sof_ta_msisdn`.
    *   **BigQuery Transformation Task:** Executes a BigQuery SQL script (derived from `d_ausd_bp_ta_msisdn.sql`) to insert data into `project.isbert_curated.sof_ta_msisdn` from `project.isbert_raw.sof_ta_msisdn_his` and `project.isbert_raw.dwtk_meldungen`.
    *   **Cleanup/Metadata Task:** Updates job status, logs metrics (e.g., record count), replacing temporary file usage and `DWMSG_SetzeStatusOK`.

## 5. Transformation Logic

The core transformation logic resides in `d_ausd_bp_ta_msisdn.sql`.

**Original Logic (`d_ausd_bp_ta_msisdn.sql`):**

1.  **Date Determination:**
    ```sql
    SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
    FROM isbert_schema.dwtk_meldungen m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    ```
    This defines `v_datum` based on the maximum `timecreated` for a specific job in `dwtk_meldungen`.

2.  **Table Truncation:**
    ```sql
    isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_msisdn REUSE STORAGE');
    ```
    This truncates the target table `sof$ta_msisdn`.

3.  **Data Insertion:**
    ```sql
    INSERT INTO sof$ta_msisdn
    (BPR_INSTANCE_ID, MSISDN, CALLNUMBER_ROLE_ID, VALID_TO )
    SELECT /*+ full(cn1) parallel(cn1,4) */
            cn1.bpri_com_id AS bpr_instance_id,
            cn1.msisdn,
            cn1.callnumber_role_id,
            nvl(cn1.valid_to, to_date('47121231','yyyymmdd')) valid_to
    FROM    (SELECT   /*+ full(cn) parallel(cn,4) */
                      cn.*,
                      max( nvl(cn.valid_to, to_date('47121231','yyyymmdd')) )
                           over ( partition by cn.bpri_com_id ) as max_valid_to
             FROM     sof$ta_msisdn_his cn
                     ) cn1
    WHERE   nvl(cn1.valid_to, to_date('47121231','yyyymmdd')) = max_valid_to
    ;
    ```
    This selects the latest valid record for each `bpri_com_id` from `sof$ta_msisdn_his` and inserts it into `sof$ta_msisdn`.

**Target BigQuery SQL Logic:**

1.  **Date Determination (if needed by the SQL script itself):**
    ```sql
    SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM `project.isbert_raw.dwtk_meldungen` AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    ```
    This would be used to set a variable if the date is a dynamic part of the subsequent SQL. More likely, this logic will be handled by Airflow before triggering the main BigQuery job, passing the date as a templated parameter.

2.  **Table Truncation:**
    ```sql
    TRUNCATE TABLE `project.isbert_curated.sof_ta_msisdn`;
    ```
    This will be an `BigQueryOperator` task in Airflow.

3.  **Data Insertion (BigQuery Standard SQL):**
    ```sql
    INSERT INTO `project.isbert_curated.sof_ta_msisdn`
    (BPR_INSTANCE_ID, MSISDN, CALLNUMBER_ROLE_ID, VALID_TO)
    SELECT
        cn1.bpri_com_id,
        cn1.msisdn,
        cn1.callnumber_role_id,
        COALESCE(cn1.valid_to, PARSE_DATE('%Y%m%d', '47121231'))
    FROM (
        SELECT
            cn.*,
            MAX(COALESCE(cn.valid_to, PARSE_DATE('%Y%m%d', '47121231')))
                OVER (PARTITION BY cn.bpri_com_id) AS max_valid_to
        FROM `project.isbert_raw.sof_ta_msisdn_his` AS cn
    ) AS cn1
    WHERE COALESCE(cn1.valid_to, PARSE_DATE('%Y%m%d', '47121231')) = cn1.max_valid_to;
    ```
    *   Oracle-specific hints (`/*+ full(cn1) parallel(cn1,4) */`) will be removed as BigQuery handles parallelism automatically.
    *   `NVL` is replaced by `COALESCE`.
    *   `TO_DATE` is replaced by `PARSE_DATE` (or `CAST` if the string is already in a compatible format).
    *   Table names are fully qualified with `project.dataset.table`.

## 6. External Dependencies

The initial analysis did not identify any explicit `external_systems` for this specific job (`[]`). However, the content reveals:

*   **Oracle Database:** The `d_ausd_bp_ta_msisdn.sql` script directly queries and updates Oracle tables.
    *   **Replacement:** This dependency will be removed by migrating the data to BigQuery. The BigQuery tables (`project.isbert_raw.dwtk_meldungen`, `project.isbert_raw.sof_ta_msisdn_his`, `project.isbert_curated.sof_ta_msisdn`) will serve as the new data sources and targets.
*   **Oracle Database Link/Service (`@pcrs1`):** The `DEFINE v_carmen = "@pcrs1"` in the SQL script suggests a potential database link or service name.
    *   **Replacement:** If `@pcrs1` implies querying data from another Oracle instance, that source data will also need to be migrated or ingested into BigQuery. If it's purely a local alias, it can be removed. Further investigation is needed to determine the exact nature and usage of `v_carmen`.
*   **Filesystem Utilities:** The KSH scripts rely on standard Unix utilities (`ksh`, `grep`, `sed`, `sort`, `join`) and custom utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`).
    *   **Replacement:** These will be replaced by Python code within the Airflow DAG, leveraging Python's standard library for file operations, date manipulation, and argument parsing. Airflow's native logging and error handling mechanisms will replace the custom KSH functions. `h_alis_sqlplus.ksh` will become obsolete as BigQuery SQL is executed directly.

## 7. Unresolved / Risks

*   **`v_carmen = "@pcrs1"` Resolution:** The exact nature and purpose of this Oracle definition need to be clarified. If it points to an external Oracle instance, that data source requires a separate migration plan to BigQuery. Currently, `lineage_external_systems` shows no explicit external systems, so this might be an internal database link, but it's an unresolved detail.
*   **KSH Utility Script Re-implementation:** The custom KSH utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`) need careful re-implementation in Python/Airflow to ensure equivalent functionality and error handling. This is a "semi_auto" aspect as the core logic is understood but custom wrappers need manual translation.
*   **Commented-out File Processing Logic:** The commented-out `sed`, `sort`, `join` commands in `k_ausd_bp_ta_msisdn.ksh` suggest a dormant file processing capability. A risk exists if these are enabled in the future, as they would require additional migration effort to PySpark or BigQuery data manipulation. For now, they are ignored, but their potential re-activation should be noted.
*   **Error Handling Fidelity:** The custom error handling (`DWMSG_MeldeFehler`, `trap`) in the KSH scripts needs to be accurately translated to Airflow's exception handling and logging to maintain operational parity.

## 8. Build Plan

The migration will involve the following steps:

1.  **BigQuery Schema Creation:**
    *   Create the target BigQuery dataset (`project.isbert_raw`, `project.isbert_curated`).
    *   Define schemas for `dwtk_meldungen`, `sof_ta_msisdn_his`, and `sof_ta_msisdn` tables in BigQuery, inferring data types from the Oracle source.

2.  **Data Ingestion (Historical):**
    *   Ingest historical data from `isbert_schema.dwtk_meldungen` and `sof$ta_msisdn_his` from Oracle into their respective BigQuery raw tables. This might involve tools like `Cloud Data Fusion` or `Cloud Storage Transfer Service` for initial bulk load.

3.  **BigQuery SQL Development:**
    *   Rewrite `d_ausd_bp_ta_msisdn.sql` into BigQuery Standard SQL (`d_ausd_bp_ta_msisdn.bqsql`). Ensure all Oracle-specific syntax and functions are converted (e.g., `NVL` to `COALESCE`, `TO_DATE` to `PARSE_DATE`).

4.  **Airflow DAG Development (`r_ausd_bp_ta_msisdn_dag.py`):**
    *   **Python Language:** All orchestration logic will be written in Python.
    *   **Airflow Parameters:** Define DAG parameters for `stichtag` (Stichtag) and `wiederanlaufwert` (Wiederanlaufwert).
    *   **Tasks:**
        *   `start_task`: Airflow `DummyOperator` or Python `BashOperator` for initial setup.
        *   `calculate_dates_task`: Python `PythonOperator` to replace `gestern.ksh` and other date calculations.
        *   `truncate_target_table_task`: Airflow `BigQueryOperator` to execute `TRUNCATE TABLE project.isbert_curated.sof_ta_msisdn`.
        *   `run_transformation_task`: Airflow `BigQueryOperator` to execute `d_ausd_bp_ta_msisdn.bqsql`, passing `stichtag` as a templated parameter if needed.
        *   `log_metrics_task`: Python `PythonOperator` to log record counts and job status, replacing `tmpFile` and `DWMSG_SetzeStatusOK`.
        *   **Dependencies:** Configure task dependencies in the DAG to reflect the original execution flow.

5.  **Utility Script Replacement:**
    *   Develop Python equivalents for the sourced KSH utility scripts, integrating their functionality directly into the Airflow DAG or as callable Python modules.
    *   `h_alis_sqlplus.ksh` functionality will be removed as `BigQueryOperator` will directly execute SQL.

6.  **Testing:**
    *   Unit test BigQuery SQL transformations.
    *   Integrate test the Airflow DAG with dummy data in BigQuery.
    *   Perform end-to-end testing with representative data volumes.

7.  **Deployment:**
    *   Deploy the Airflow DAG to Cloud Composer.
    *   Schedule the DAG to run at the appropriate frequency.