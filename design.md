# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh

## 1. Purpose & Scope

This KornShell script, `r_ausd_v_ta_discount_rr.ksh`, serves as a wrapper and orchestration mechanism for a core data reconciliation process. Its primary function is to prepare the execution environment, parse command-line parameters, manage logging, and establish robust error handling before invoking a central script, `k_ausd_v_ta_discount_rr.ksh`, responsible for the actual contract data reconciliation for the `ta_discount_rr` table. This script itself does not contain the core business logic for data transformation but rather orchestrates its execution.

The scope of this migration design document covers the replatforming of this KornShell orchestrator script to a Google Cloud Platform (GCP) equivalent, specifically an Airflow DAG on Cloud Composer, with the aim of integrating it into a modern data pipeline architecture.

## 2. Source Inventory

The job consists of a single primary component:

*   **File Name**: `r_ausd_v_ta_discount_rr.ksh`
*   **Relative Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh`
*   **Technology**: KornShell (ksh)
*   **Category**: shell
*   **Complexity Tier**: medium
*   **Automation Bucket**: semi_auto
*   **Primary Purpose**: Pipeline Orchestrator
*   **Migration Stance**: REPLATFORM
*   **GCP Target Hint**: Cloud Composer (Airflow)
*   **Summary**: This script orchestrates the execution of a core data reconciliation script for the 'ta_discount_rr' table, handling environment setup, parameter parsing, error logging, and status reporting.

The script also has implicit dependencies on several sourced utility scripts and a core processing script, whose full contents are not available in this analysis but are identified:
*   `. $HOME/.dw_init` (environment initialization)
*   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling functions)
*   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter handling support)
*   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling support)
*   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh` (the core processing script)

## 3. Target Architecture

The target architecture for this job will leverage Google Cloud Platform services to replatform the orchestration and data processing:

*   **Orchestration**: The `r_ausd_v_ta_discount_rr.ksh` script will be migrated to an **Apache Airflow DAG** running on **Cloud Composer**. This will manage the sequence of tasks, parameter passing, and monitoring.
*   **Core Logic**: The invoked core script, `k_ausd_v_ta_discount_rr.ksh`, once analyzed, will be migrated to an appropriate GCP service. Given the likely SQL nature implied by its path (`SQL/aktuell/aufbereitung`), it will likely be converted to **BigQuery SQL** executed via a `BigQueryOperator` in Airflow, or potentially a **Dataflow job** if complex transformations are involved.
*   **Logging**: All logging activities, currently directed to a local log file, will be integrated with **Cloud Logging** for centralized log management and monitoring.
*   **Environment Management**: Environment variables and configurations currently managed by `.dw_init` and `BERT_DIR_ROOT` will be managed within the Airflow environment, possibly using Airflow Variables, Connections, or secrets managed by **Secret Manager**.

## 4. Data Flow & Lineage

The original data flow is as follows:

1.  **Environment Setup**: The script sources `$HOME/.dw_init` to initialize the environment and other helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) for common utilities.
2.  **Parameter Parsing**: Command-line arguments are parsed using `getopts`.
3.  **Job & Logging Initialization**: Job-specific identifiers, system date, and log file paths are determined using `DWMSG_` functions. A log entry is created.
4.  **Error Handling Setup**: Shell `trap` commands are set up to invoke `DWMSG_Fehlerbehandlung` on `INT` (interrupt) and `ERR` (shell error).
5.  **Core Script Invocation**: The primary action is the execution of `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh` with specific job and entry number parameters. Output is redirected to the generated log file.
6.  **Status Reporting**: Upon successful completion, a success message is logged, and `DWMSG_SetzeStatusOK` is called.

In the target GCP architecture, this will translate to:

*   An **Airflow DAG** will define the workflow.
*   **Initialization**: Initial Airflow tasks (e.g., PythonOperators) will handle environment setup and parameter retrieval (e.g., from Airflow Variables or passed at DAG trigger).
*   **Core Logic Execution**: A specific Airflow task (e.g., `BigQueryOperator` or `DataflowFlexTemplateOperator`) will execute the migrated `k_ausd_v_ta_discount_rr.ksh` logic.
*   **Logging & Monitoring**: All task logs will be automatically captured by Cloud Logging. Airflow's built-in monitoring and alerting will replace custom `DWMSG_` error reporting.
*   **Dependencies**: The Airflow DAG will explicitly define task dependencies to ensure correct execution order.

## 5. Transformation Logic

The transformation logic for `r_ausd_v_ta_discount_rr.ksh` will involve a complete rewrite from KornShell to Python, orchestrated by an Airflow DAG.

**Original KornShell Components and Proposed Airflow Transformation:**

*   **Script Initialization & Metadata**: (`ProgName`, `ProgVersion`, `usage()` function)
    *   **Airflow**: These will become metadata within the Airflow DAG definition (e.g., `dag_id`, `description`) and Python functions/docstrings for usage information.
*   **Environment Sourcing**: (`. $HOME/.dw_init`, `BERT_DIR_ROOT`)
    *   **Airflow**: Environment variables can be configured at the Cloud Composer environment level, or specific values fetched from Airflow Variables or Secret Manager within PythonOperators. The functions provided by the sourced scripts (e.g., `DWMSG_`) will be re-implemented as Python utilities or integrated into Airflow's native capabilities.
*   **Parameter Parsing**: (`getopts` loop for `-h`, `ParamList`, `ErrNr`, `ErrArg`)
    *   **Airflow**: Command-line parameter parsing will be replaced by Airflow DAG parameters or Jinja templated fields for operator arguments. The `-h` (help) functionality will be part of the DAG's documentation.
*   **Error Handling & Traps**: (`set -eu`, `trap`, `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`)
    *   **Airflow**: `set -eu` behavior is inherent in Python's error handling. Airflow's `on_failure_callback` mechanisms and robust task retry configurations will replace shell traps. Error reporting (`DWMSG_MeldeFehler`) will be integrated with Airflow logging to Cloud Logging, and potentially custom alerts via Cloud Monitoring.
*   **Job & Log File Management**: (`DW_EintragsNr`, `JobKennung`, `v_sysdate`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `LogDatei`)
    *   **Airflow**: Airflow provides native job identification (DAG Run IDs). Log file generation will be replaced by Airflow's integrated logging to Cloud Logging. Status reporting (`DWMSG_SetzeStatusOK`) will be managed by Airflow task status.
*   **Core Script Invocation**: (`${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr} >> $LogDatei 2>&1`)
    *   **Airflow**: This will be transformed into an Airflow operator, whose type depends on the migration of `k_ausd_v_ta_discount_rr.ksh`. If `k_ausd_v_ta_discount_rr.ksh` becomes BigQuery SQL, a `BigQueryOperator` would be used. If it becomes a Python script for Dataflow, a `DataflowFlexTemplateOperator` would be appropriate. The parameters (`-j`, `-f`) will be passed as operator arguments. Output redirection will be handled by Airflow's logging.

## 6. External Dependencies

The `lineage_assembled_jobs` for this run did not explicitly list external systems directly referenced by `r_ausd_v_ta_discount_rr.ksh`. The script primarily depends on:

*   **Internal Utility Scripts**: `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`. These are not external systems but internal framework components. Their functionalities will be absorbed and re-implemented using Python within the Airflow DAG or as custom Airflow operators/hooks.
*   **Core Processing Script**: `k_ausd_v_ta_discount_rr.ksh`. The actual external system interactions (e.g., database connections for `ta_discount_rr`) are expected to be within this core script. The migration of this script will involve identifying and replacing its external dependencies with appropriate GCP services (e.g., BigQuery, Cloud Storage, Cloud SQL).

## 7. Unresolved / Risks

*   **`k_ausd_v_ta_discount_rr.ksh` Analysis (High Risk)**: The content and specific dependencies of the core script are currently unknown. Its functionality (e.g., pure SQL, complex shell logic, external system calls) will dictate its target GCP service and the complexity of its migration. This is the single biggest unknown and risk.
*   **Utility Script Rewrites (Medium Risk)**: The exact logic within `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` needs to be fully understood to ensure accurate and complete re-implementation in Python or replacement with Airflow/GCP native features.
*   **`BERT_DIR_ROOT` Resolution (Low Risk)**: The dynamic resolution of paths via `BERT_DIR_ROOT` implies a reliance on the existing execution environment. This will need to be explicitly configured in Cloud Composer (e.g., as Airflow Variables or environment variables in the Composer environment).
*   **"Semi-Auto" Migration Bucket**: The `semi_auto` classification confirms that this migration will require significant manual effort, detailed design, and development, beyond automated code conversion.

## 8. Build Plan

1.  **Analyze `k_ausd_v_ta_discount_rr.ksh`**:
    *   Retrieve the content of `k_ausd_v_ta_discount_rr.ksh`.
    *   Perform static analysis to understand its function, dependencies, and I/O.
    *   Determine the optimal GCP target service (e.g., BigQuery SQL, Dataflow Python) for its migration.
    *   **Output**: Migration design for `k_ausd_v_ta_discount_rr.ksh` (language dependent on analysis).

2.  **Re-implement Utility Functions (Python)**:
    *   Analyze `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`.
    *   Rewrite essential functionalities of these scripts as Python modules or Airflow custom operators/helpers. This will replace the `DWMSG_` functions.
    *   **Output**: Python modules for utility functions.

3.  **Design Airflow DAG for `r_ausd_v_ta_discount_rr.ksh` (Python)**:
    *   Define the Airflow DAG structure, including `dag_id`, schedule, owner, and default arguments.
    *   Define Airflow tasks for:
        *   Environment setup/parameter retrieval.
        *   Executing the migrated `k_ausd_v_ta_discount_rr.ksh` logic (using the appropriate operator determined in step 1).
        *   Implementing logging and error handling using Airflow's native mechanisms and integration with Cloud Logging.
    *   **Output**: Airflow DAG Python file (`r_ausd_v_ta_discount_rr_dag.py`).

4.  **Develop Airflow Tasks (Python/BQSQL/etc.)**:
    *   Implement the Python code for any custom operators or Python functions used within the DAG.
    *   Develop the migrated core logic (e.g., BigQuery SQL scripts, Dataflow Python code) based on the analysis in step 1.
    *   **Output**: Python code for Airflow tasks, migrated core logic files.

5.  **Testing**:
    *   Unit tests for individual Python modules and functions.
    *   Integration tests for the Airflow DAG, simulating execution on Cloud Composer.
    *   Data validation tests to ensure migrated logic produces identical results to the legacy system.

6.  **Deployment**:
    *   Deploy the Airflow DAG and associated Python code to a Cloud Composer environment.
    *   Configure any necessary Airflow Variables, Connections, or Secret Manager entries.