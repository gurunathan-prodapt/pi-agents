# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh

## 1. Purpose & Scope

This migration job focuses on `r_ausd_bp_ta_bpr_bcp.ksh`, a KornShell script responsible for orchestrating the initial provision of selected "Basisprodukte" (base products) for the BERT system. Its primary function is to parse command-line parameters (key date and a restart value), set up the execution environment, implement logging and error handling, and then invoke a core KornShell script, `k_ausd_bp_ta_bpr_bcp.ksh`, to perform the actual data extraction and staging.

The scope of this migration is to re-platform this orchestration logic from its current KornShell implementation to a Google Cloud Composer (Airflow) DAG, targeting the BigQuery data platform for any underlying data processing.

## 2. Source Inventory

The migration involves a single primary source file:

*   **File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_bcp.ksh`
*   **Technology:** KornShell Script
*   **Tool:** KornShell
*   **Category:** Shell, Orchestration Script
*   **Purpose:** Coordinates data provisioning for the BERT system, handling parameters, environment setup, logging, error handling, and invoking a core processing script.
*   **Complexity Tier:** Medium (inferred from `lineage_assembled_jobs` stage distribution; no explicit `file_complexity` entry found). The orchestration nature, involving parameter parsing and external script calls, suggests medium complexity.
*   **Migration Bucket:** B4 - Redesign (inferred from `file_analysis`'s `migration_stance: "REPLATFORM"` for Cloud Composer/Airflow). This indicates a significant architectural change from a shell script to a managed orchestration service.

## 3. Target Architecture

The target architecture for this job will leverage Google Cloud's data ecosystem:

*   **Orchestration:** Google Cloud Composer (Apache Airflow)
    *   The `r_ausd_bp_ta_bpr_bcp.ksh` script's orchestration logic will be translated into an Airflow DAG written in Python. This DAG will manage the execution flow, parameter passing, logging, and error handling.
*   **Data Processing:** Google BigQuery
    *   The underlying data extraction and staging logic, currently performed by `k_ausd_bp_ta_bpr_bcp.ksh`, is assumed to be migrated to BigQuery SQL or PySpark jobs managed by the Airflow DAG.
*   **Logging & Monitoring:** Cloud Logging and Cloud Monitoring
    *   Airflow's native integration with Cloud Logging will replace the custom shell-based logging. Cloud Monitoring will be used for alerts and operational insights.

## 4. Data Flow & Lineage

The current data flow is an orchestration pattern:

`r_ausd_bp_ta_bpr_bcp.ksh` (Orchestrator) → `k_ausd_bp_ta_bpr_bcp.ksh` (Core Script) → Data Systems (implied)

*   `r_ausd_bp_ta_bpr_bcp.ksh` is the entry point, responsible for:
    *   Parsing `Stichtag` (`-s`) and `Wiederanlaufwert` (`-l`) parameters.
    *   Initializing the environment by sourcing helper scripts (`. $HOME/.dw_init`, error handling, parameter handling, date handling utilities).
    *   Setting up logging and error traps.
    *   Invoking the core script: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_bcp.ksh` with parameters (`-j $JobKennung`, `-s $p_stichtag`, `-f ${DW_EintragsNr}`, `-l ${p_wiederanlaufWert}`).
*   `k_ausd_bp_ta_bpr_bcp.ksh` is the "core script" that performs the actual data extraction and staging. Details of its internal logic are not available in this analysis, but its invocation is a critical part of the overall job.

**Target Data Flow (Airflow DAG):**

The Airflow DAG will represent this flow as a series of tasks:

1.  **Parameter Parsing/Validation Task:** Parses DAG run configuration parameters (equivalent to `-s` and `-l`).
2.  **Environment Setup Task:** Configures necessary environment variables or Airflow connections.
3.  **Core Processing Task:** Invokes the migrated logic of `k_ausd_bp_ta_bpr_bcp.ksh` (likely a BigQuery Operator for SQL, or a Dataproc/SparkSubmit Operator for PySpark). This task will receive the `stichtag` and `wiederanlaufwert` as arguments.
4.  **Logging/Monitoring Integration:** Airflow automatically handles logging to Cloud Logging, and tasks can integrate with Cloud Monitoring for custom metrics.

## 5. Transformation Logic

The KornShell script's logic will be re-implemented as a Python-based Airflow DAG.

**Original Logic Breakdown (KornShell) and Target Transformation (Airflow):**

*   **Parameter Handling (`getopts`, `ParamList`, `usage` function):**
    *   The `getopts` loop for `h`, `s`, `l` parameters will be replaced by Airflow's native DAG run configuration parameters. The `usage` function's description will be incorporated into the DAG's documentation.
    *   Default values for parameters (e.g., `p_wiederanlaufWert=0`, `p_stichtag=$v_sysdate` if not set) will be handled by Airflow parameter defaults or Python logic.
    *   Parameter validation (`pruefeParameterGesetzt`) will be implemented as Python validation logic within an Airflow task.
*   **Environment Initialization (`. $HOME/.dw_init`):**
    *   This will be replaced by Airflow connections, variables, and potentially a base Docker image for the Airflow worker that has necessary tools/libraries pre-installed. Specific environment variables will be set as Airflow variables.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):**
    *   These shell utility functions will be refactored into Python functions within the Airflow DAG or shared Python modules.
    *   Date handling functions (`DWDate_Gib_Zeitraum`) will be replaced by Python's `datetime` module.
    *   Custom error and logging functions (`DWMSG_MeldeFehler`, `DWMSG_Logdateiname`, etc.) will be replaced by Airflow's standard logging (to Cloud Logging) and error handling mechanisms.
*   **Error Handling (`set -e`, `trap`):**
    *   Airflow provides robust error handling, retries, and failure notifications. The `trap` logic will be replaced by Airflow's task-level `retries`, `retry_delay`, `on_failure_callback` mechanisms, and Python `try-except` blocks.
*   **Core Script Invocation (`${Name_Kernskript} ...`):**
    *   The invocation of `k_ausd_bp_ta_bpr_bcp.ksh` will become a dedicated Airflow task. The type of operator will depend on the migration strategy for `k_ausd_bp_ta_bpr_bcp.ksh` (e.g., `BigQueryOperator` if converted to SQL, `DataprocSparkSubmitOperator` if converted to PySpark). The parameters will be passed using Airflow's Jinja templating.
*   **Logging (`print`, `tee`):**
    *   All `print` statements and `tee` redirects will be removed, as Airflow automatically captures stdout/stderr of tasks and sends them to Cloud Logging.
*   **Exit Status (`exit 0`, `exit $ErrNr`):**
    *   Task success/failure in Airflow is determined by the exit code of the executed command or the success/failure of the Python code within the task.

## 6. External Dependencies

The original script has several external dependencies:

*   **Sourced Environment/Utility Scripts (local files):**
    *   `. $HOME/.dw_init`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    *   **Replacement Strategy:** These will be replaced by Python modules or libraries, Airflow variables, connections, or configuration files that align with GCP best practices. Environment variable initialization can be handled by setting environment variables for the Airflow worker or within the DAG code.
*   **Core Processing Script (local file):**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_bcp.ksh`
    *   **Replacement Strategy:** This script's logic needs to be fully migrated to a BigQuery-native solution (e.g., BigQuery SQL, stored procedures, or PySpark running on Dataproc). The Airflow DAG will then invoke this migrated BigQuery or Dataproc job using appropriate Airflow operators (e.g., `BigQueryOperator`, `DataprocSparkSubmitOperator`).
*   **Underlying Data Sources/Targets:**
    *   The description mentions "DWH" (Data Warehouse) and "Forderungsscoring" (FOS-Tabelle). These are the source/target data systems that `k_ausd_bp_ta_bpr_bcp.ksh` interacts with.
    *   **Replacement Strategy:** These will be migrated to BigQuery tables, Cloud Storage, or other GCP data services. Airflow connections will manage access credentials.

## 7. Unresolved / Risks

*   **Unresolved Targets:** The `lineage_assembled_jobs` analysis indicated `unresolved_targets: []`, meaning no explicit unresolved external dependencies were found at the job assembly level.
*   **Details of `k_ausd_bp_ta_bpr_bcp.ksh`:** The most significant unresolved aspect is the detailed functionality of the `k_ausd_bp_ta_bpr_bcp.ksh` script. This "core script" is responsible for the actual data extraction and staging. Without its source code and analysis, the full end-to-end migration design, especially for the data transformation part, remains incomplete. It is assumed that this script will be migrated to BigQuery SQL or PySpark.
*   **Missing Complexity/Automation Rate:** The absence of `file_complexity` and `automation_rate` data for `r_ausd_bp_ta_bpr_bcp.ksh` could indicate a gap in the automated analysis, potentially underestimating effort if manual review for these aspects becomes necessary. However, the `REPLATFORM` stance already flags it as B4 (Redesign), implying high manual effort.
*   **Environment Variable Resolution:** The dynamic nature of `${BERT_DIR_ROOT}` needs to be fully understood and mapped to appropriate Airflow variables or configuration in the target environment.
*   **Character Encoding:** The presence of `ausgew?hlter` in the original script comments suggests potential character encoding issues that need to be addressed during migration to ensure correct handling of German umlauts and special characters in logging and data.

## 8. Build Plan

The build plan focuses on re-platforming the orchestration script to an Airflow DAG:

1.  **Analyze `k_ausd_bp_ta_bpr_bcp.ksh`:**
    *   **Action:** Obtain and thoroughly analyze the source code of `k_ausd_bp_ta_bpr_bcp.ksh` to understand its data sources, transformation logic, and target tables.
    *   **Output:** Migration Design Document for `k_ausd_bp_ta_bpr_bcp.ksh` (likely to BigQuery SQL or PySpark).
2.  **Design Airflow DAG for `r_ausd_bp_ta_bpr_bcp.ksh`:**
    *   **Action:** Create a Python Airflow DAG file (`r_ausd_bp_ta_bpr_bcp_dag.py`) that captures the orchestration logic.
    *   **Language:** Python (for Airflow DAG).
3.  **Implement Parameter Handling:**
    *   **Action:** Translate `getopts` logic into Airflow DAG run parameters and Python validation.
    *   **Language:** Python.
4.  **Refactor Utility Functions:**
    *   **Action:** Convert shell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) into reusable Python functions or modules.
    *   **Language:** Python.
5.  **Implement Logging and Error Handling:**
    *   **Action:** Utilize Airflow's native logging to Cloud Logging and implement error callbacks as needed.
    *   **Language:** Python.
6.  **Integrate Core Processing:**
    *   **Action:** Create an Airflow task that invokes the *migrated* `k_ausd_bp_ta_bpr_bcp.ksh` logic. This will use the appropriate Airflow operator (e.g., `BigQueryOperator` or `DataprocSparkSubmitOperator`) based on step 1's outcome.
    *   **Language:** Python (for Airflow operator), BigQuery SQL/PySpark (for core logic).
7.  **Define Airflow Variables/Connections:**
    *   **Action:** Set up Airflow variables for paths (e.g., `BERT_DIR_ROOT`) and Airflow connections for data sources/targets.
    *   **Language:** Airflow Configuration (UI/CLI/Terraform).
8.  **Testing:**
    *   **Action:** Develop unit tests for Python utility functions and integration tests for the Airflow DAG on a Cloud Composer development environment.
    *   **Language:** Python.
9.  **Deployment:**
    *   **Action:** Deploy the Airflow DAG to the Cloud Composer environment.
    *   **Tool:** `gcloud composer environments storage dags import` or equivalent CI/CD pipeline.