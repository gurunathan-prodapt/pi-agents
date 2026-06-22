# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh

## 1. Purpose & Scope

This KornShell script, `k_ausd_v_ta_acc_ref.ksh`, serves as a control and orchestration script for processing data related to the `ta_acc_ref` table. Its primary function is to:
- Parse command-line parameters (`p_JobKennung`, `p_EintragsNr`).
- Source various utility scripts for environment setup, error handling, date functions, and SQL*Plus execution.
- Validate the provided parameters.
- Orchestrate the execution of a SQL script, `d_ausd_v_ta_acc_ref.sql`, which processes data for the `ta_acc_ref` table.
- Read a record count from a temporary file after the SQL execution.
- Manage error reporting and script exit codes.

The business purpose of this job is to prepare and process data, likely for reporting or further data warehousing, ensuring data consistency and error handling during the process. The migration aims to re-platform this orchestration logic to Google Cloud Platform, specifically leveraging Cloud Composer (Airflow) for scheduling and orchestration, and BigQuery for data processing and storage.

## 2. Source Inventory

| File Name                                                         | Technology  | Complexity Tier | Automation Bucket |
| :---------------------------------------------------------------- | :---------- | :-------------- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh` | KornShell   | Complex         | B4 (Redesign)     |

**Rationale for Tier and Bucket:**
The script's role as an orchestrator, handling parameter parsing, sourcing external utilities, and dynamically executing a SQL script, positions it as "Complex". The explicit `gcp_target_hint` of "Cloud Composer (Airflow)" and `migration_stance` of "REPLATFORM" indicates a significant architectural shift, categorizing its migration into the "Redesign" (B4) bucket. This involves not just code translation but a complete rethinking of its operational pattern within a cloud-native environment.

## 3. Target Architecture

The target architecture will leverage Google Cloud Platform services to replace the existing KornShell orchestration and Oracle SQL processing.

-   **Orchestration:** The `k_ausd_v_ta_acc_ref.ksh` script's orchestration logic (parameter handling, sequencing, error management) will be re-implemented as an **Apache Airflow DAG** running on **Cloud Composer**.
-   **Data Processing:** The SQL script `d_ausd_v_ta_acc_ref.sql` will be translated into **BigQuery SQL**. This BigQuery SQL will be executed as a `BigQueryOperator` task within the Airflow DAG.
-   **Data Storage:** The `ta_acc_ref` table, currently residing in an Oracle database, will be migrated to a **BigQuery table**. All other tables referenced in `d_ausd_v_ta_acc_ref.sql` will also be migrated to BigQuery.
-   **Logging and Monitoring:** Airflow's native logging and Cloud Logging/Monitoring will replace the shell script's basic console output and `DWMSG_MeldeFehler` calls.
-   **Parameter Management:** Airflow's DAG parameters or `Variable` objects will manage `p_JobKennung` and `p_EintragsNr`.

**BigQuery Layout:**
-   A dedicated BigQuery dataset, e.g., `isbert_ds`, will house the `ta_acc_ref` table and any other dependent tables (e.g., `d_ausd_v_ta_acc_ref.sql` inputs).
-   Tables will adhere to BigQuery's best practices for partitioning and clustering as needed for performance.

## 4. Data Flow & Lineage

The current data flow is:

1.  **Start `k_ausd_v_ta_acc_ref.ksh`**: Invoked with parameters `j` and `f`.
2.  **Environment Setup**: Sourcing `$HOME/.dw_init` and other utility scripts.
3.  **Parameter Validation**: `pruefeParameterGesetzt` checks `p_JobKennung` and `p_EintragsNr`.
4.  **SQL Script Execution**: `starteSQLSkript` invokes SQL*Plus to run `d_ausd_v_ta_acc_ref.sql`.
5.  **Data Transformation**: `d_ausd_v_ta_acc_ref.sql` reads from source tables (unknown from provided info, but assumed Oracle) and writes/updates the `ta_acc_ref` table (Oracle).
6.  **Record Count**: The SQL execution is expected to output a record count to a temporary file, which is then read by the ksh script.
7.  **Error Handling**: `DWMSG_MeldeFehler` on parameter errors.

**Migrated Data Flow (Airflow on Cloud Composer):**

1.  **Airflow DAG Trigger**: The `k_ausd_v_ta_acc_ref` DAG is triggered, potentially with `job_kennung` and `eintrags_nr` parameters.
2.  **Environment/Parameter Task**: An Airflow task (e.g., `PythonOperator`) handles parameter validation and environment variable setup, equivalent to the shell script's initializations.
3.  **BigQuery SQL Task**: A `BigQueryOperator` executes the translated `d_ausd_v_ta_acc_ref.bqsql` against the BigQuery `isbert_ds`.`ta_acc_ref` table. This task will also handle fetching any affected row counts directly from BigQuery.
4.  **Logging/Monitoring**: Airflow's built-in mechanisms will log task progress, success, and failures. Custom logging for the record count can be implemented.

**Lineage:**
-   **Source**: Legacy Oracle tables (implied inputs to `d_ausd_v_ta_acc_ref.sql`).
-   **Transformation**: Airflow DAG orchestrating BigQuery SQL.
-   **Target**: BigQuery `isbert_ds.ta_acc_ref` table.

## 5. Transformation Logic

**KornShell Script (`k_ausd_v_ta_acc_ref.ksh`) to Airflow DAG:**

-   **Parameter Parsing (`getopts`):** Will be replaced by Airflow DAG parameters or XComs if parameters are dynamic across tasks.
-   **Environment Sourcing (`. $HOME/.dw_init`, etc.):** Environmental variables will be managed through Airflow Variables, environment variables configured in the Composer environment, or passed via XComs. Utility functions (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) will need to be re-implemented in Python as Airflow custom operators, hooks, or as part of a Python utility module.
-   **Parameter Validation (`pruefeParameterGesetzt`):** This logic will be implemented as a Python function within an Airflow `PythonOperator` task at the beginning of the DAG.
-   **SQL Script Invocation (`starteSQLSkript`):** This will be replaced by a `BigQueryOperator` or `BigQueryExecuteQueryOperator` in Airflow, executing the BigQuery-translated SQL.
-   **Temporary File (`tmpFile`) and Record Count (`eval "v_records=\`cat $tmpFile\`"`):** The concept of a temporary file for record counts will be deprecated. BigQuery operations can directly return row counts or other metrics, which can then be logged or passed via XComs.
-   **Error Handling (`DWMSG_MeldeFehler`):** Airflow's task dependencies, retries, and failure callbacks will handle error scenarios. Custom error logging to Cloud Logging can be integrated.
-   **Orchestration Flow:** The sequential execution of the KornShell script (setup, validate, execute SQL, get count) will directly map to a linear or branched Airflow DAG, defining task dependencies.

**SQL Script (`d_ausd_v_ta_acc_ref.sql`) to BigQuery SQL:**

-   The content of `d_ausd_v_ta_acc_ref.sql` is currently unknown, but it is assumed to be Oracle SQL.
-   **SQL Dialect Conversion:** The Oracle SQL will be translated to BigQuery Standard SQL, addressing any dialect differences, function mappings, and data type conversions.
-   **Table References:** All tables referenced in `d_ausd_v_ta_acc_ref.sql` (e.g., source tables, `ta_acc_ref`) must be available in BigQuery.
-   **Performance Optimization:** The converted BigQuery SQL should be optimized for BigQuery's columnar storage and distributed query engine (e.g., using partitioning, clustering, appropriate join strategies).

## 6. External Dependencies

The `lineage_assembled_jobs.external_systems` entry for this job was empty. However, the analysis of the KornShell script and its dependencies reveals the following:

-   **Oracle Database:** The script implicitly depends on an Oracle database via `h_alis_sqlplus.ksh` which suggests the use of SQL*Plus to execute `d_ausd_v_ta_acc_ref.sql`.
    -   **Replacement:** The Oracle database will be replaced by **Google BigQuery**. All relevant tables, including `ta_acc_ref` and any tables read by `d_ausd_v_ta_acc_ref.sql`, must be migrated to BigQuery.
-   **KornShell Utility Scripts:**
    -   `$HOME/.dw_init`: Environment initialization.
    -   `f_alis_msgerr.ksh`: Error messaging framework.
    -   `h_alis_date.ksh`: Date utility functions.
    -   `h_alis_parameter.ksh`: Parameter parsing/validation utilities.
    -   `h_alis_sqlplus.ksh`: SQL*Plus execution wrapper.
    -   **Replacement:** These scripts will be re-implemented in **Python** as part of the Airflow DAG's supporting codebase. This might involve creating custom Airflow operators, hooks, or a Python package deployed to the Cloud Composer environment. Standard Python libraries and Airflow features will be utilized for common functionalities like date handling and parameter management. Error reporting will use Airflow's native mechanisms and integration with Cloud Logging.
-   **Temporary File System:** The script uses a local temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_acc_ref_$$.tmp`) to store and retrieve the record count.
    -   **Replacement:** In Airflow/BigQuery, direct mechanisms for obtaining row counts from `BigQueryOperator` results or BigQuery table metadata will be used. Temporary file usage will be eliminated.

## 7. Unresolved / Risks

-   **Content of `d_ausd_v_ta_acc_ref.sql`:** The exact SQL code in `d_ausd_v_ta_acc_ref.sql` is not available. This is a significant unknown. The complexity of its Oracle SQL (e.g., use of proprietary functions, PL/SQL blocks, complex subqueries, or OLAP functions) will directly impact the effort required for BigQuery SQL translation.
-   **Source Tables for `d_ausd_v_ta_acc_ref.sql`:** The specific input tables read by `d_ausd_v_ta_acc_ref.sql` are unknown. Their schemas and migration status to BigQuery need to be confirmed.
-   **Utility Script Re-implementation:** The `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` scripts require detailed analysis and re-implementation in Python. Their full functionalities need to be understood to ensure a complete and accurate migration.
-   **`r_ausd_vertrag.ksh` reference:** The script comments reference `r_ausd_vertrag.ksh` as a control script, but its exact relationship and whether it orchestrates this job or is related to other jobs is not fully clear from the available lineage. If `r_ausd_vertrag.ksh` is a wrapper, its migration strategy needs to be defined.
-   **Dynamic SQL/External Commands in `d_ausd_v_ta_acc_ref.sql`:** If the SQL script contains dynamic SQL or invokes external commands (e.g., via `DBMS_SCHEDULER`), this will add complexity to the BigQuery migration and may require a different approach (e.g., using `BigQuery Data Transfer Service` or more complex Airflow operators).
-   **UC4 Integration (Potential):** While `lineage_assembled_jobs.component_files` only lists the ksh script, the presence of `UC4_PROD` in the `lineage_edges` snippet implies other UC4 jobs exist within the larger system. If this ksh script is ultimately invoked by a UC4 scheduler, the migration should consider this higher-level orchestration, potentially integrating the new Airflow DAG into the broader UC4-to-Airflow migration strategy.

## 8. Build Plan

The migration will involve the following steps:

1.  **Analyze `d_ausd_v_ta_acc_ref.sql` (Manual/Semi-Automated):**
    *   **Action:** Retrieve the source code for `d_ausd_v_ta_acc_ref.sql`.
    *   **Output:** Detailed understanding of its SQL logic, source tables, and data types.
    *   **Language:** Oracle SQL.
2.  **Migrate Dependent Tables to BigQuery (Automated/Manual):**
    *   **Action:** Identify all source tables read by `d_ausd_v_ta_acc_ref.sql` and the target `ta_acc_ref` table. Plan and execute their migration to BigQuery, including schema definition, data ingestion, and historization (if applicable).
    *   **Output:** BigQuery table definitions (`DDL`) and loaded data.
    *   **Language:** BigQuery SQL.
3.  **Translate `d_ausd_v_ta_acc_ref.sql` to BigQuery SQL (Automated/Manual):**
    *   **Action:** Convert the Oracle SQL script to BigQuery Standard SQL.
    *   **Output:** `d_ausd_v_ta_acc_ref.bqsql`.
    *   **Language:** BigQuery SQL.
4.  **Develop Airflow Utility Package (Manual):**
    *   **Action:** Re-implement the functionalities of the KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) as Python functions or classes within a reusable Python package for Cloud Composer.
    *   **Output:** `airflow_utils.py` (or similar package structure).
    *   **Language:** Python.
5.  **Develop `k_ausd_v_ta_acc_ref` Airflow DAG (Manual):**
    *   **Action:** Create a Python Airflow DAG that encapsulates the orchestration logic of `k_ausd_v_ta_acc_ref.ksh`. This includes parameter handling, validation, and calling the BigQuery SQL task.
    *   **Output:** `k_ausd_v_ta_acc_ref_dag.py`.
    *   **Language:** Python (Airflow DAG).
6.  **Deploy and Test:**
    *   **Action:** Deploy the BigQuery SQL and Airflow DAG to a development Cloud Composer environment. Rigorously test parameter passing, SQL execution, error handling, and logging.
    *   **Output:** Verified and validated migrated job.
    *   **Language:** N/A