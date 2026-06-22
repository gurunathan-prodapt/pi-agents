# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh

## 1. Purpose & Scope

This job is encapsulated by the KornShell script `r_ausd_v_ta_bp_ref.ksh`, named "Vertragsdatenabgleich" (Contract Data Reconciliation). Its primary purpose is to act as a wrapper or orchestrator for a core data processing script. It handles environment setup, command-line parameter parsing, robust error handling, logging, and ultimately invokes the main data reconciliation process for the `ta_bp_ref` table. The script ensures that the core reconciliation logic (`k_ausd_v_ta_bp_ref.ksh`) is executed within a controlled and monitored environment. The scope of this migration is to re-platform this orchestration logic to Google Cloud's BigQuery ecosystem, specifically leveraging Cloud Composer (Airflow) for workflow management.

## 2. Source Inventory

The job consists of a single primary source file:

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh`
    *   **Technology**: KornShell (ksh)
    *   **Category**: Shell Script, Wrapper Script, Pipeline Orchestrator
    *   **Complexity Tier**: Medium. This is inferred from its role as an orchestrator, handling multiple responsibilities like environment setup, parameter parsing, error management, and invoking a subordinate process.
    *   **Automation Bucket**: Semi-Automated (B2). The `file_analysis` hinted at Cloud Composer (Airflow) as a GCP target for orchestration, suggesting a replatforming effort that is semi-automated rather than fully manual or fully automated.
    *   **Purpose**: Orchestrates the execution of a core script (`k_ausd_v_ta_bp_ref.ksh`) responsible for reconciling contract data for the `ta_bp_ref` table. It sets up the execution context, manages logging, and implements error traps.
    *   **Key Functionality**:
        *   Sourcing environment variables from `$HOME/.dw_init`.
        *   Loading shared error handling (`f_alis_msgerr.ksh`) and utility functions (`h_alis_parameter.ksh`, `h_alis_date.ksh`).
        *   Parsing command-line arguments `-h`, `-s`, `-l` using `getopts`.
        *   Initializing job-specific variables like `JobKennung` (BERT_V_TA_BP_REF) and `v_sysdate`.
        *   Setting up logging mechanisms using `DWMSG_` functions, directing output to `LogDatei`.
        *   Implementing `trap` for `INT` (interrupt) and `ERR` (general errors) signals to ensure proper error reporting and exit.
        *   Executing the main data reconciliation script `k_ausd_v_ta_bp_ref.ksh` with specific parameters (`-j $JobKennung -f ${DW_EintragsNr}`).
        *   Logging success messages upon successful completion.

## 3. Target Architecture

The target platform for this job is Google Cloud Platform, utilizing BigQuery and Cloud Composer (Airflow) for orchestration.

*   **Orchestration**: The KornShell wrapper script (`r_ausd_v_ta_bp_ref.ksh`) will be re-platformed into an **Apache Airflow DAG** hosted on Google Cloud Composer. This DAG will manage the execution flow, parameter passing, logging, and error handling.
*   **Core Logic**: The core data reconciliation script (`k_ausd_v_ta_bp_ref.ksh`) invoked by the wrapper will need to be migrated to a suitable BigQuery-compatible technology. This could involve:
    *   **BigQuery SQL**: If the core script primarily executes SQL logic.
    *   **Python with BigQuery client libraries**: For more complex data manipulation or external integrations.
    *   **Dataflow/Spark**: If the reconciliation involves large-scale data transformations.
    *   The detailed design for `k_ausd_v_ta_bp_ref.ksh` is outside the scope of this document but is a critical dependency.
*   **Logging & Monitoring**:
    *   Logging will be integrated with **Cloud Logging**. Airflow's native logging capabilities will ensure all task logs are captured.
    *   Monitoring will leverage **Cloud Monitoring** and Airflow's built-in UI for DAG status, task duration, and errors.
*   **Environment Variables & Configuration**: Environment initialization (`. $HOME/.dw_init`) and other configuration will be managed via Airflow Variables, Connections, or by configuring the Composer environment.
*   **Error Handling**: The existing `DWMSG_` error handling functions will be replaced by Airflow's robust error handling mechanisms, including retries, alerting (e.g., via Cloud Monitoring alerts or Airflow callbacks), and explicit task dependencies.

## 4. Data Flow & Lineage

The current script acts as an orchestration layer. The data flow primarily involves:

1.  **Initialization**: The `r_ausd_v_ta_bp_ref.ksh` script starts, loading environment variables and utility functions.
2.  **Parameter Processing**: Command-line arguments are parsed.
3.  **Logging Setup**: Dynamic log file naming and initial job entry logging.
4.  **Core Script Execution**: The script executes `k_ausd_v_ta_bp_ref.ksh`. The output and errors of this core script are redirected to the log file managed by the wrapper.
5.  **Error Trapping**: `INT` and `ERR` signals are trapped to ensure consistent error reporting.
6.  **Status Update**: Upon completion (success or error), the script updates status messages in the log and exits.

In the target BigQuery/Cloud Composer architecture, this flow will translate to:

*   **Airflow DAG (r_ausd_v_ta_bp_ref)**: This DAG will be the orchestrator.
    *   **Setup Task**: An initial task to set up any required environment variables or configurations (equivalent to `. $HOME/.dw_init`).
    *   **Parameter Task**: A task to parse or validate input parameters for the overall job (if any external parameters are still needed).
    *   **Core Logic Task (k_ausd_v_ta_bp_ref)**: A task that executes the migrated `k_ausd_v_ta_bp_ref.ksh` logic. This could be a BigQueryOperator, a DataflowTemplateOperator, or a PythonOperator calling a Python script that interacts with BigQuery.
    *   **Logging & Monitoring**: Airflow's native logging to Cloud Logging. Metrics on task success/failure, duration, etc., will be available in Cloud Monitoring.
    *   **Error Handling**: Airflow task dependencies and retry mechanisms will manage failures. On-failure callbacks can trigger alerts.
    *   **Success Task**: A final task to mark successful completion and perform any cleanup, similar to the `DWMSG_SetzeStatusOK` call.

The current `lineage_edges` showed no direct `READS` or `WRITES` for the `r_ausd_v_ta_bp_ref.ksh` script itself, which is consistent with its wrapper role. The data interactions will primarily be handled by the invoked `k_ausd_v_ta_bp_ref.ksh` script (which is not part of the current analysis for its internal logic but is a critical dependency).

## 5. Transformation Logic

The KornShell script `r_ausd_v_ta_bp_ref.ksh` itself performs orchestration rather than data transformation. The transformation logic will focus on converting its shell orchestration patterns to Airflow DAGs:

*   **Shell Script to Airflow DAG Structure**:
    *   The overall script execution (`main` logic) will become the DAG definition.
    *   The `usage` function and `getopts` parameter parsing will be replaced by Airflow DAG parameters (e.g., `params` in DAG definition, or using `{% raw %}{{ ds }}{% endraw %}` for date-related parameters).
    *   Environment sourcing (`. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, etc.) will be replaced by Airflow Connections, Variables, or by ensuring the Composer environment has the necessary setup.
    *   The sequence of operations (initialization -> parameter parsing -> logging setup -> core script execution -> status update) will define the task flow and dependencies within the Airflow DAG.
    *   The invocation of `k_ausd_v_ta_bp_ref.ksh` will become a task in the Airflow DAG. The type of operator (BigQueryOperator, PythonOperator, etc.) will depend on the migration strategy for `k_ausd_v_ta_bp_ref.ksh`.
*   **Logging & Error Handling**:
    *   Shell `print` statements and redirection (`>> $LogDatei 2>&1`) will be replaced by standard Python logging within Airflow tasks, which automatically integrates with Cloud Logging.
    *   The `DWMSG_MeldeFehler`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK` functions will be re-implemented using Airflow's built-in logging and status management capabilities, potentially combined with custom Python operators for specific messaging.
    *   The `trap` commands will be implicitly handled by Airflow's task failure mechanisms (retries, on_failure_callbacks).

## 6. External Dependencies

Based on the initial `lineage_assembled_jobs` query, there are no explicit `external_systems` listed for this job. However, the source code and its static analysis reveal several implicit dependencies:

1.  **Sourced Shell Scripts / Utilities**:
    *   `$HOME/.dw_init`: An environment initialization script.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utility.
    *   **Replacement Strategy**: These utilities need to be either:
        *   Re-implemented in Python as part of the Airflow DAG helper functions.
        *   Their functionalities absorbed directly into the Airflow DAG logic using Airflow's native capabilities (e.g., Airflow Variables for environment settings, Python for date handling and parameter validation).
2.  **Core Script Invocation**:
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh`: This is the main core processing script invoked by the wrapper.
    *   **Replacement Strategy**: This script is a critical dependency. Its migration path must be determined separately. It will likely be converted to BigQuery SQL, Python (running on Dataflow/Compute Engine), or another suitable GCP data processing service. The Airflow DAG will then invoke this migrated core logic using an appropriate Airflow operator.
3.  **Underlying Data Source/Target**:
    *   The script mentions "Tabelle `ta_bp_ref`" for data reconciliation. This implies a database where this table resides.
    *   **Replacement Strategy**: `ta_bp_ref` and any related tables will be migrated to **BigQuery**. The core script `k_ausd_v_ta_bp_ref.ksh` (once migrated) will interact with BigQuery.

## 7. Unresolved / Risks

*   **Core Script (`k_ausd_v_ta_bp_ref.ksh`) Migration**: The detailed transformation logic for the core script invoked by this wrapper is unknown. Its complexity and migration strategy will significantly impact the Airflow DAG design (e.g., choice of Airflow operator). This is the primary unresolved item.
*   **`ParamList="s:l:"` usage**: The script parses `-s` and `-l` parameters but the provided code snippet does not show their usage. It's crucial to understand how these parameters influence the core script's behavior or any other downstream logic to ensure accurate migration.
*   **`DWMSG_` Functions**: The exact implementations of `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK` are not known. Their functionalities need to be thoroughly analyzed and replicated in Airflow (e.g., custom Python functions for specific logging/messaging requirements, or reliance on Airflow's default logging).
*   **Exit Codes**: The script exits with specific error codes (`193`, `192`, `1`). Airflow tasks typically succeed or fail. Mapping these specific exit codes to Airflow task states or custom alerting logic needs consideration.
*   **`BERT_DIR_ROOT`**: This is an environment variable. Its value and contents are critical for resolving paths to helper scripts and the core script. Its equivalent in GCP (e.g., Airflow Variable) needs to be defined.

## 8. Build Plan

The build plan will focus on creating an Airflow DAG for the wrapper script and identifying the interface for the core logic.

1.  **Analyze `k_ausd_v_ta_bp_ref.ksh`**:
    *   **Action**: Perform detailed analysis of the core reconciliation script (`k_ausd_v_ta_bp_ref.ksh`) to determine its data sources, transformation logic, and optimal migration strategy to BigQuery (e.g., BigQuery SQL, Python/Dataflow).
    *   **Output**: Migration design for `k_ausd_v_ta_bp_ref.ksh`, specifying target language and GCP service.
2.  **Airflow DAG Development (r_ausd_v_ta_bp_ref)**:
    *   **Action**: Create a Python-based Airflow DAG in Cloud Composer.
    *   **Language**: Python
    *   **Components**:
        *   **DAG Definition**: Define the DAG structure, schedule (if any), and default arguments.
        *   **Initialization Task**: A PythonOperator or BashOperator (if simple environment commands are needed) to replicate `dw_init` functionality and set up required Airflow Variables/Connections.
        *   **Parameter Handling**: Integrate parameter parsing (if needed) using Airflow's native parameter passing or `PythonOperator` to validate parameters.
        *   **Logging/Messaging Functions**: Implement Python functions or use `PythonOperator` to replicate the `DWMSG_` functionalities for logging job start/end, errors, and status updates, integrating with Cloud Logging.
        *   **Core Logic Task**: An operator (e.g., `BigQueryOperator`, `DataflowTemplateOperator`, `BashOperator` if `k_ausd_v_ta_bp_ref.ksh` is migrated to a GCP-runnable script) to invoke the migrated `k_ausd_v_ta_bp_ref.ksh` logic, passing necessary parameters like `JobKennung` and `DW_EintragsNr`.
        *   **Error Handling**: Configure Airflow's retry mechanisms and `on_failure_callback` functions for alerting.
        *   **Success Task**: A final `PythonOperator` or `BashOperator` to signal successful completion.
    *   **Output**: `r_ausd_v_ta_bp_ref_dag.py` (Airflow DAG Python file).
3.  **Migrate Utility Functions**:
    *   **Action**: Convert `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` to Python helper functions or leverage existing Airflow/Python libraries.
    *   **Language**: Python
    *   **Output**: Python utility modules, integrated into the DAG or as separate shared libraries.
4.  **Deployment**:
    *   **Action**: Deploy the Airflow DAG and any associated Python modules to Cloud Composer.
    *   **Output**: Operational Airflow DAG.