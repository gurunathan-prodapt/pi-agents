# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh

## 1. Purpose & Scope
This job, originally a KornShell script named `k_ausd_bp_ta_rn_da_vda_tk.ksh`, acts as a control script for a data processing task. Its primary purpose is to orchestrate the execution of an Oracle SQL script (`d_ausd_bp_ta_rn_da_vda_tk.sql`) after performing initial setup and parameter validation. The script handles environment initialization, parses command-line parameters (job identifier, entry number, reference date, restart value), validates the date format, and then executes the core SQL logic using `sqlplus`. It is responsible for processing basis product data, specifically related to telephone numbers (DA-, VDA-, and TK-Rufnummern), and storing the results in a temporary table. The `purpose_note` indicates it's "Job assembled from 1 component(s); stage dist: medium=1".

The scope of this migration is to re-implement this orchestration and data transformation logic in Google Cloud's BigQuery and Airflow.

## 2. Source Inventory
The job consists of two primary source files:

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh`**
    *   **Technology:** KornShell script
    *   **Category:** shell
    *   **Tool:** KornShell
    *   **File Purpose:** Control script that initializes the environment, parses parameters, performs date validation, and orchestrates the execution of a core SQL script for data processing.
    *   **Complexity Tier:** Undetermined (file_complexity data not found)
    *   **Migration Flags:** Undetermined (file_complexity data not found)
    *   **Automation Bucket:** semi_auto

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_rn_da_vda_tk.sql`**
    *   **Technology:** Oracle SQL
    *   **File Purpose:** Performs the core data transformation, reading from a source table and inserting into a target table after truncation.

The ksh script also includes commented-out sections for `sed`, `sort`, and `join` commands on files like `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`, suggesting potential legacy file-based processing that might be dormant or subject to future requirements.

## 3. Target Architecture
The migrated solution will leverage Google Cloud Platform services:
*   **Orchestration:** Apache Airflow on Cloud Composer to schedule and execute the data pipeline.
*   **Data Transformation:** BigQuery SQL for all data processing logic.
*   **Data Storage:** BigQuery tables for both source and target data.

The job will be deployed as an Airflow DAG written in Python, containing a single task that executes the transformed SQL in BigQuery.

## 4. Data Flow & Lineage
The original data flow is as follows:
1.  The `k_ausd_bp_ta_rn_da_vda_tk.ksh` KornShell script is executed.
2.  It sources several utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) for environment setup, error handling, date validation, parameter parsing, and SQL*Plus interaction.
3.  It determines a `p_Stichtag` (reference date) and other parameters (`p_JobKennung`, `p_EintragsNr`, `p_wiederanlaufWert`).
4.  It constructs the path to the SQL script `d_ausd_bp_ta_rn_da_vda_tk.sql`.
5.  It calls a function `starteSQLSkript` (presumably a wrapper around `sqlplus`) to execute `d_ausd_bp_ta_rn_da_vda_tk.sql`, passing parameters.
6.  The `d_ausd_bp_ta_rn_da_vda_tk.sql` script:
    *   Determines a `v_datum` from `isbert_schema.dwtk_meldungen`.
    *   Truncates the `sof$ta_rn_da_vda_tk` table.
    *   Inserts data into `sof$ta_rn_da_vda_tk` from `sof$ta_rn_einzeln` based on the presence of DA\_RN\_msisdn, VDA\_RN\_msisdn, or TK\_RN\_msisdn.
7.  The ksh script then captures the number of processed records from a temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_da_vda_tk.tmp`).

The migrated data flow will be:
1.  An Airflow DAG `d_ausd_bp_ta_rn_da_vda_tk` is scheduled and executed.
2.  A single `BigQueryExecuteQueryOperator` task named `process_ta_rn_da_vda_tk` executes the combined BigQuery SQL.
3.  The BigQuery SQL:
    *   Truncates the target table `sof$ta_rn_da_vda_tk`.
    *   Inserts data from the source table `sof$ta_rn_einzeln` into `sof$ta_rn_da_vda_tk` based on the same filtering logic.

## 5. Transformation Logic
The core transformation logic resides in the Oracle SQL script and will be translated to BigQuery SQL.

**Original Oracle SQL (`d_ausd_bp_ta_rn_da_vda_tk.sql`):**
```sql
TRUNCATE TABLE sof$ta_rn_da_vda_tk REUSE STORAGE;

INSERT INTO sof$ta_rn_da_vda_tk
(CNTRCT_ID, DA_RN_MSISDN, DA_RN_STATUS, DA_RN_VALID_TO, VDA_RN_MSISDN, VDA_RN_STATUS, VDA_RN_VALID_TO, TK_RN_MSISDN, TK_RN_STATUS, TK_RN_VALID_TO)
SELECT /*+ full(rp) parallel(rp,4) */
        cntrct_id,
        DA_RN_msisdn, DA_RN_status, DA_RN_valid_to,
        VDA_RN_msisdn, VDA_RN_status, VDA_RN_valid_to,
        TK_RN_msisdn, TK_RN_status, TK_RN_valid_to
FROM    sof$ta_rn_einzeln rp
WHERE   DA_RN_msisdn IS NOT NULL OR
        VDA_RN_msisdn IS NOT NULL OR
        TK_RN_msisdn IS NOT NULL;
```

**Migrated BigQuery SQL:**
The BigQuery SQL will perform the same `TRUNCATE` and `INSERT` operations. The Oracle-specific `/*+ full(rp) parallel(rp,4) */` hint will be removed as it's not applicable in BigQuery. BigQuery table names will follow the pattern used in the generated DAG (`sof$ta_rn_da_vda_tk` and `sof$ta_rn_einzeln`).

```sql
TRUNCATE TABLE `sof$ta_rn_da_vda_tk`;

INSERT INTO `sof$ta_rn_da_vda_tk`
(
  CNTRCT_ID,
  DA_RN_MSISDN,
  DA_RN_STATUS,
  DA_RN_VALID_TO,
  VDA_RN_MSISDN,
  VDA_RN_STATUS,
  VDA_RN_VALID_TO,
  TK_RN_MSISDN,
  TK_RN_STATUS,
  TK_RN_VALID_TO
)
SELECT
  cntrct_id,
  DA_RN_msisdn,
  DA_RN_status,
  DA_RN_valid_to,
  VDA_RN_msisdn,
  VDA_RN_status,
  VDA_RN_valid_to,
  TK_RN_msisdn,
  TK_RN_status,
  TK_RN_valid_to
FROM `sof$ta_rn_einzeln` rp
WHERE DA_RN_msisdn IS NOT NULL
   OR VDA_RN_msisdn IS NOT NULL
   OR TK_RN_msisdn IS NOT NULL;
```

The ksh script's parameter parsing and date validation logic will be handled implicitly by Airflow's scheduling context or by implementing these as Python functions within the DAG if specific parameter handling is still required for external triggers. The `isbert_schema.dwtk_meldungen` table lookup for `v_datum` will need to be re-evaluated for its relevance and implemented as a BigQuery lookup or an Airflow XCom if the date is a dependency for other tasks. For this specific SQL, `v_datum` is defined but not used in the `INSERT` statement, so its direct migration for this component is not critical unless it is relevant for broader job context.

## 6. External Dependencies
The `lineage_assembled_jobs.external_systems` was empty, indicating no explicitly tracked external systems (like Oracle, SFTP, S3) are directly linked to this job in the lineage data. However, the analysis of the ksh script reveals implicit dependencies:

*   **Oracle Database:** The `d_ausd_bp_ta_rn_da_vda_tk.sql` script directly interacts with an Oracle database (tables `sof$ta_rn_da_vda_tk`, `sof$ta_rn_einzeln`, `isbert_schema.dwtk_meldungen`). This will be replaced by BigQuery tables.
*   **KornShell Utility Scripts:** The ksh script sources several internal utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`). These scripts handle environment setup, error logging, date operations, and parameter processing.
    *   **Replacement Strategy:**
        *   Environment initialization (`.dw_init`): Replaced by Airflow environment configuration and Python logic within the DAG.
        *   Error handling (`f_alis_msgerr.ksh`): Replaced by Airflow's native logging and error handling mechanisms.
        *   Date validation (`h_alis_date.ksh`, `gestern.ksh`): Replaced by Python's `datetime` module or Airflow's built-in macros for date manipulation.
        *   Parameter parsing (`h_alis_parameter.ksh`): Replaced by Airflow DAG parameters or configuration.
        *   SQL*Plus interaction (`h_alis_sqlplus.ksh`): No direct replacement needed as `BigQueryExecuteQueryOperator` handles BigQuery execution.

## 7. Unresolved / Risks
*   **File Complexity Data:** No complexity tier or migration flags were found for the `k_ausd_bp_ta_rn_da_vda_tk.ksh` script in `file_complexity`. This could indicate that a detailed complexity analysis for this file is missing, which might lead to underestimation of effort if hidden complexities exist.
*   **Commented-out Code:** The ksh script contains substantial commented-out code related to `sed`, `sort`, and `join` operations on intermediate files (`cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`). While currently inactive, their presence suggests potential dormant requirements or historical processing steps. It's a risk if these processes need to be reactivated or are implicitly required by downstream systems not fully understood. It's recommended to confirm if these commented sections are truly obsolete. If not, they would need to be migrated to BigQuery-native transformations (e.g., SQL queries, Dataflow/Dataproc if complex file processing is needed).
*   **External Script Logic:** The exact logic within the sourced ksh utility scripts (e.g., `f_alis_msgerr.ksh`, `gestern.ksh`) has not been fully analyzed as they were not directly part of the `component_files` for this job. Assumptions have been made about their functionality based on their names. A deeper dive might be required if their functionality is complex and needs precise replication in the BigQuery/Airflow environment.
*   **Temporary File Usage:** The ksh script uses `tmpFile` (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_da_vda_tk.tmp`) to store record counts. This will be replaced by Airflow XComs or direct BigQuery query results if the record count is still required for logging or downstream processes.

## 8. Build Plan
The migration will involve the following steps:

1.  **BigQuery Table Creation:**
    *   Create the target BigQuery table `sof$ta_rn_da_vda_tk` with the schema derived from the `INSERT` statement (CNTRCT_ID, DA_RN_MSISDN, DA_RN_STATUS, DA_RN_VALID_TO, VDA_RN_MSISDN, VDA_RN_STATUS, VDA_RN_VALID_TO, TK_RN_MSISDN, TK_RN_STATUS, TK_RN_VALID_TO).
    *   Ensure the source BigQuery table `sof$ta_rn_einzeln` exists and is populated with the necessary data.
    *   Ensure the `isbert_schema.dwtk_meldungen` table (or its BigQuery equivalent) exists if the `v_datum` logic is still required.

2.  **BigQuery SQL Script Generation:**
    *   Generate a single BigQuery SQL script (`d_ausd_bp_ta_rn_da_vda_tk_bq.sql`) containing the `TRUNCATE TABLE` and `INSERT INTO ... SELECT` statements.

3.  **Airflow DAG Development:**
    *   Create an Airflow DAG Python file (`d_ausd_bp_ta_rn_da_vda_tk_dag.py`) with the following structure:
        *   **Imports:** `timedelta`, `DAG` from `airflow`, `BigQueryExecuteQueryOperator` from `airflow.providers.google.cloud.operators.bigquery`.
        *   **`default_args`:** Define owner, start_date, retries, etc.
        *   **`build_bigquery_sql()` function:** Encapsulate the BigQuery SQL script within this Python function for readability and to allow for dynamic SQL generation if needed in the future.
        *   **DAG Definition:**
            *   `dag_id`: `d_ausd_bp_ta_rn_da_vda_tk`
            *   `schedule_interval`: Define according to the current job's schedule (currently `None` in the generated DAG, but should reflect the original job's cadence).
            *   `description`: "BigQuery DAG for ta_rn_da_vda_tk processing"
            *   `tags`: `["bigquery", "dw", "isbert"]`
        *   **Task Definition:**
            *   A `BigQueryExecuteQueryOperator` task named `process_ta_rn_da_vda_tk`.
            *   `sql`: Call `build_bigquery_sql()`.
            *   `use_legacy_sql`: `False`.
            *   `create_disposition`: `CREATE_IF_NEEDED`.
            *   `write_disposition`: `WRITE_APPEND` (as truncation is handled by the SQL itself).
            *   `location`: Specify the correct BigQuery dataset location (e.g., `EU` or `US`).

4.  **Deployment:**
    *   Deploy the `d_ausd_bp_ta_rn_da_vda_tk_dag.py` file to the Cloud Composer environment.

5.  **Testing:**
    *   Thoroughly test the DAG and BigQuery SQL to ensure data accuracy and performance match the legacy system.

6.  **Parameter Handling (if applicable):**
    *   If the parameters `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert` are dynamic and passed from an upstream scheduler, implement Airflow mechanisms (e.g., `dag_run.conf` or custom operators) to retrieve and pass these values to the BigQuery SQL.