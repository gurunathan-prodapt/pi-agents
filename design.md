# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh

## 1. Purpose & Scope
This document outlines the migration design for the legacy KornShell script `k_ausd_v_ta_vvl_dwh.ksh` to Google Cloud Platform (GCP), specifically targeting BigQuery for data processing and Cloud Composer (Airflow) for orchestration.

The script `k_ausd_v_ta_vvl_dwh.ksh` acts as an orchestration layer. Its primary purpose is to:
*   Initialize the environment.
*   Parse command-line parameters (`p_JobKennung`, `p_EintragsNr`).
*   Validate these parameters.
*   Construct the path to a SQL script: `d_ausd_v_ta_vvl_dwh.sql`.
*   Execute `d_ausd_v_ta_vvl_dwh.sql` with the collected parameters using a helper function `starteSQLSkript`.
*   Potentially capture the number of records processed from a temporary file.

The SQL script `d_ausd_v_ta_vvl_dwh.sql` performs data processing, reading from source tables `DWTK_MELDUNGEN` and `DWH$TA_F_VVL_EREIGNISSE` and writing/merging data into `SOF$TA_VVL_DWH` and `VIA`. It also utilizes a PL/SQL package `DWPA_UTIL_SKRIPT`.

The overall business purpose is to process data related to `ta_vvl_dwh` (presumably "Vertragsverlängerung Data Warehouse" or similar, given the table name), likely for reporting or further data warehousing stages.

## 2. Source Inventory

| File Name                                             | Technology | Tier          | Automation Bucket | Summary                                                                                                                                                                             |
| :---------------------------------------------------- | :--------- | :------------ | :---------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `k_ausd_v_ta_vvl_dwh.ksh`                             | KornShell  | (Not found)   | (Not found)       | Orchestration script for executing `d_ausd_v_ta_vvl_dwh.sql`, handling environment setup, parameter parsing, and validation. Identified as a "pipeline_orchestrator".               |
| `d_ausd_v_ta_vvl_dwh.sql` (referenced)                | SQL        | (Not analyzed)| (Not analyzed)    | Data processing script that reads from `DWTK_MELDUNGEN`, `DWH$TA_F_VVL_EREIGNISSE`, and writes/merges into `SOF$TA_VVL_DWH`, `VIA`. Uses `DWPA_UTIL_SKRIPT` package. |

*Note: `file_complexity` and `automation_rate` data were not available for the source file.*

## 3. Target Architecture

The migration will target the following GCP components:

*   **Cloud Composer (Airflow):** To orchestrate the data pipeline, replacing the KornShell script's control flow. A new Airflow DAG will be created.
*   **BigQuery:** To host the migrated data tables and execute the transformed SQL logic.
    *   **Datasets:** Dedicated BigQuery datasets will be created to house the tables. For example, `legacy_source_db.DWTK_MELDUNGEN` might become `project_id.source_dataset.dwtk_meldungen`.
    *   **Tables:** `DWTK_MELDUNGEN`, `DWH$TA_F_VVL_EREIGNISSE`, `SOF$TA_VVL_DWH`, `VIA` will be migrated to BigQuery.
*   **Cloud Storage:** Potentially used for temporary file storage if `tmpFile` operations are critical and cannot be directly translated to BigQuery.

## 4. Data Flow & Lineage

**Current Data Flow:**
1.  A UC4 job (e.g., `DW.BERT_AUSD_V_TA_VVL_DWH.xml`) invokes `k_ausd_v_ta_vvl_dwh.ksh`.
2.  `k_ausd_v_ta_vvl_dwh.ksh` (KornShell script):
    *   Sources environment variables and utility scripts.
    *   Parses job-specific parameters.
    *   Calls `starteSQLSkript` to execute `d_ausd_v_ta_vvl_dwh.sql`.
3.  `d_ausd_v_ta_vvl_dwh.sql` (SQL script):
    *   **Reads from:** `DWTK_MELDUNGEN`, `DWH$TA_F_VVL_EREIGNISSE` (source databases/schemas).
    *   **Writes to/Merges into:** `SOF$TA_VVL_DWH`, `VIA` (target tables).
    *   **Uses:** PL/SQL package `DWPA_UTIL_SKRIPT`.

**Migrated Data Flow (Conceptual):**
1.  **Airflow DAG:** A new Airflow DAG, named appropriately (e.g., `bert_ausd_v_ta_vvl_dwh`), will replace the UC4 job and the KornShell orchestration.
2.  **Tasks in DAG:**
    *   **Parameter Task:** A PythonOperator or BashOperator to handle parameter passing (e.g., `p_JobKennung`, `p_EintragsNr`) to subsequent tasks.
    *   **SQL Execution Task:** A BigQueryOperator (or PythonOperator executing BigQuery client code) will execute the migrated `d_ausd_v_ta_vvl_dwh.sql` logic.
        *   This task will connect to BigQuery.
        *   It will execute the transformed BigQuery SQL, reading from the migrated `DWTK_MELDUNGEN` and `DWH$TA_F_VVL_EREIGNISSE` tables.
        *   It will write/merge the results into `SOF$TA_VVL_DWH` and `VIA` in BigQuery.
    *   **Post-processing/Logging Task:** A PythonOperator for any necessary post-execution logging or metric collection (replacing `eval "v_records=\`cat $tmpFile\`"` and `DWMSG_MeldeFehler`).

## 5. Transformation Logic

### 5.1. `k_ausd_v_ta_vvl_dwh.ksh` (KornShell Script)

**Legacy Logic:**
*   Environment setup: `. $HOME/.dw_init`, sourcing utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
*   Parameter parsing: `getopts` for `j` (JobKennung) and `f` (EintragsNr).
*   Parameter validation: `pruefeParameterGesetzt` calls.
*   SQL script execution: `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung`.
*   Temporary file handling: `$tmpFile` for record count.
*   Error handling: `DWMSG_MeldeFehler`.

**Target Logic (Cloud Composer / Airflow DAG):**
*   **Environment Initialization:** Replace with Airflow's environment configuration. Global variables/connections in Airflow can manage `BERT_DIR_ROOT`, `DW_DIR_UTL`.
*   **Parameter Handling:** Airflow DAG parameters or XComs can manage `p_JobKennung` and `p_EintragsNr`. These can be passed as `render_templates` to the BigQuery SQL task.
*   **Utility Functions:** The functions in sourced scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `pruefeParameterGesetzt`, `DWMSG_MeldeFehler`, `starteSQLSkript`) will need to be re-implemented in Python within Airflow tasks or replaced by equivalent Airflow/GCP services. For example, `starteSQLSkript` functionality will be handled by the BigQueryOperator. Logging will use Airflow's native logging.
*   **SQL Execution:** The core call to `starteSQLSkript` will be replaced by a `BigQueryOperator` that executes the migrated `d_ausd_v_ta_vvl_dwh.sql` code.
*   **Temporary File (`tmpFile`):** If the record count is essential, this can be achieved by reading from the target BigQuery table after the SQL execution or by including a `COUNT(*)` query in the SQL task itself and pushing the result to XComs. Cloud Storage can also be used for transient file storage if absolutely necessary.

### 5.2. `d_ausd_v_ta_vvl_dwh.sql` (SQL Script)

**Legacy Logic:**
*   Reads from `DWTK_MELDUNGEN` and `DWH$TA_F_VVL_EREIGNISSE`.
*   Writes/merges data into `SOF$TA_VVL_DWH` and `VIA`.
*   Uses `DWPA_UTIL_SKRIPT` (PL/SQL package).

**Target Logic (BigQuery SQL):**
*   **Schema and Data Type Conversion:** All tables (`DWTK_MELDUNGEN`, `DWH$TA_F_VVL_EREIGNISSE`, `SOF$TA_VVL_DWH`, `VIA`) will be converted from their legacy database (likely Oracle, given PL/SQL package use) to BigQuery tables. Data types will be mapped to BigQuery's equivalent types.
*   **SQL Syntax Translation:** The SQL code within `d_ausd_v_ta_vvl_dwh.sql` will be translated from its current dialect (e.g., Oracle SQL) to BigQuery Standard SQL. This includes:
    *   `MERGE` statements.
    *   `INSERT` statements.
    *   Function calls (e.g., within `DWPA_UTIL_SKRIPT`).
*   **`DWPA_UTIL_SKRIPT`:** This PL/SQL package will need to be analyzed. Its functionality will either be:
    *   Re-implemented as BigQuery UDFs (JavaScript or SQL).
    *   Converted into inline BigQuery SQL.
    *   If it involves complex procedural logic, it might be migrated to a BigQuery Stored Procedure or a Python component executed in the Airflow DAG.

## 6. External Dependencies

| Original System / Object | Description                                                                        | Migration Approach                                                                                                                                                                                                                                   |
| :----------------------- | :--------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **UC4 Scheduler**        | The presumed scheduler that invokes the KornShell script.                          | Replaced by Cloud Composer (Airflow) scheduling capabilities. The Airflow DAG will be scheduled to run at the appropriate frequency/triggers.                                                                                                        |
| **Legacy Database(s)**   | Source tables (`DWTK_MELDUNGEN`, `DWH$TA_F_VVL_EREIGNISSE`) and target tables (`SOF$TA_VVL_DWH`, `VIA`). Also the database for `DWPA_UTIL_SKRIPT`. | All database tables will be migrated to BigQuery. This involves schema conversion and data ingestion (e.g., via batch loads using Dataflow or `bq load` commands, or streaming with Dataflow/Pub/Sub if real-time updates are needed). |
| **File System**          | For sourcing utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, etc.) and the SQL script, and for `tmpFile`. | Sourced scripts will be absorbed into the Airflow DAG's Python code or replaced by Airflow/GCP services. The SQL script will be directly part of the BigQueryOperator task. Temporary file handling will use BigQuery queries or Cloud Storage. |
| **`DW.HOLE_PFAD`**       | (From lineage) External package used by UC4 job.                                   | If this is related to path resolution for the ksh script, it will be replaced by Airflow's configuration management and file path handling.                                                                                                            |
| **`DW.BERT_LESE_LOG`**   | (From lineage) External package used by UC4 job for logging.                       | Replaced by Airflow's native logging capabilities, integrated with Cloud Logging.                                                                                                                                                                    |
| **`DW.UNIX.ISBERT`**     | (From lineage) Login used by UC4 job.                                              | Replaced by GCP service accounts for authentication and authorization within Airflow and BigQuery.                                                                                                                                                 |
| **`DWHDWH1P`**           | (From lineage) Host.                                                               | Irrelevant in the serverless BigQuery/Cloud Composer architecture.                                                                                                                                                                                   |

## 7. Unresolved / Risks

*   **Complexity of `DWPA_UTIL_SKRIPT`:** The exact logic within the `DWPA_UTIL_SKRIPT` PL/SQL package is unknown. If it contains complex procedural logic, its migration to BigQuery (UDFs, Stored Procedures) or Python tasks in Airflow could be complex and requires detailed analysis. This is a potential risk that might lead to manual intervention or redesign (B3/B4).
*   **Exact Behavior of `starteSQLSkript`:** While presumed to execute SQL, the precise mechanisms (e.g., connection parameters, error handling, output capture) within `starteSQLSkript` are abstracted. This needs to be thoroughly understood to accurately replicate in Airflow/BigQuery.
*   **`tmpFile` Handling:** The specific use case and criticality of `v_records=\`cat $tmpFile\`` need clarification. If this record count is essential for downstream processes, the Airflow task needs to reliably capture this metric from BigQuery.
*   **Implicit Dependencies:** The sourced utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, etc.) may contain other implicit dependencies or environment variables that need to be explicitly identified and configured in the GCP environment.
*   **Data Volume and Performance:** The current performance characteristics of `d_ausd_v_ta_vvl_dwh.sql` are unknown. Performance tuning might be required post-migration in BigQuery, especially for large datasets.
*   **Parameter `h`:** The KornShell script has a parameter `h` that prints "Bitte ueber Rahmenscript aufrufen" and exits. This suggests it's designed to be called by another "framework script". The Airflow DAG should handle this upstream invocation logic.

## 8. Build Plan

The migration will follow these steps:

1.  **Schema Migration (BigQuery):**
    *   Migrate `DWTK_MELDUNGEN` schema to BigQuery.
    *   Migrate `DWH$TA_F_VVL_EREIGNISSE` schema to BigQuery.
    *   Migrate `SOF$TA_VVL_DWH` schema to BigQuery.
    *   Migrate `VIA` schema to BigQuery.
    *   (Language: DDL in BigQuery Standard SQL)
2.  **Data Ingestion (BigQuery):**
    *   Load historical data from legacy `DWTK_MELDUNGEN` to BigQuery.
    *   Load historical data from legacy `DWH$TA_F_VVL_EREIGNISSE` to BigQuery.
    *   (Language: `bq load` commands, Dataflow jobs, or other ETL tools)
3.  **SQL Script Transformation:**
    *   Translate `d_ausd_v_ta_vvl_dwh.sql` from its current SQL dialect to BigQuery Standard SQL.
    *   Analyze `DWPA_UTIL_SKRIPT` and migrate its functionality to BigQuery UDFs/Stored Procedures or Airflow Python tasks.
    *   (Language: BigQuery Standard SQL, JavaScript for UDFs, Python for procedural logic if applicable)
4.  **Airflow DAG Development:**
    *   Create a new Airflow DAG `bert_ausd_v_ta_vvl_dwh.py`.
    *   Implement tasks for parameter handling.
    *   Implement a `BigQueryOperator` task to execute the transformed `d_ausd_v_ta_vvl_dwh.sql`.
    *   Implement tasks for logging and any post-processing logic (e.g., capturing record counts).
    *   Configure Airflow connections and variables to replace legacy environment settings.
    *   Define the DAG schedule.
    *   (Language: Python)
5.  **Testing and Validation:**
    *   Unit testing of individual BigQuery SQL components.
    *   Integration testing of the Airflow DAG with BigQuery.
    *   Data validation to ensure migrated data and processed outputs match legacy system results.
    *   Performance testing in BigQuery.
    *   (Language: Python for Airflow tests, SQL for data validation)