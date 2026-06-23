# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_v_ta_bp_ref.ksh`, serves as a wrapper or orchestration script for a contract data reconciliation job. Its primary purpose is to:
*   Initialize the runtime environment by sourcing common setup files (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
*   Parse and validate command-line parameters, providing usage information and handling errors for unknown or missing arguments.
*   Establish a robust error handling and logging mechanism, including `INT` and `ERR` traps to manage script interruptions and failures.
*   Orchestrate the execution of the core data processing logic, which resides in a separate script: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh`.
*   Log job metadata, output from the core script, and final status messages to a dynamically generated log file.

The job's internal name is `Vertragsdatenabgleich`, and it is specifically designed for reconciling contract data within the `ta_bp_ref` table.

## 2. Source Inventory
The job consists of a single primary component file.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh`
    *   **Technology:** KornShell script
    *   **Category:** `shell`
    *   **Tool:** `KornShell`
    *   **Summary:** This ksh script acts as a wrapper for a core data processing script, handling environment setup, parameter parsing, error logging, and orchestration of the main data reconciliation process for the 'ta_bp_ref' table.
    *   **Purpose:** Orchestration/ETL wrapper
    *   **Complexity Tier:** Not explicitly defined in `file_complexity` (no rows returned), but based on its role as a wrapper, it exhibits medium complexity due to environment setup, parameter parsing, error handling, and invocation of a child script.
    *   **Migration Bucket:** `semi_auto` (B2)

## 3. Target Architecture
The migration target platform is Google BigQuery. Given that this script is primarily an orchestrator that invokes another script, the target architecture will likely involve:
*   **Orchestration Layer:** Google Cloud Composer (Apache Airflow) for scheduling and managing the overall workflow. This will replace the KornShell wrapper's orchestration capabilities.
*   **Data Processing Layer:** BigQuery for the core data reconciliation logic. The invoked script `k_ausd_v_ta_bp_ref.ksh` (which is not part of this specific job but is a dependency) is expected to contain the SQL or data manipulation logic that would be migrated to BigQuery SQL, possibly as stored procedures or standard SQL queries executed via Composer.
*   **Logging and Monitoring:** Google Cloud Logging and Cloud Monitoring will replace the custom `DWMSG_*` logging framework used in the original script.
*   **Environment Configuration:** Environment variables and configuration will be managed through Composer's environment variables or external configuration files (e.g., in Cloud Storage).

## 4. Data Flow & Lineage
The lineage for this specific job (`r_ausd_v_ta_bp_ref.ksh`) as determined by `lineage_edges` is currently empty. However, from the source code, the operational data flow is clearly defined:

1.  **Initialization:** The script starts by sourcing several utility KornShell scripts (`$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) to set up the environment, error handling, parameter parsing, and date functions. These scripts are external dependencies.
2.  **Parameter Processing:** Command-line arguments (`-h`, `-s`, `-l`) are parsed using `getopts`. Errors related to parameters (unknown or missing arguments) result in logging and script exit.
3.  **Logging Setup:** Job-specific identifiers (`JobKennung`, `DW_EintragsNr`, `LogDatei`) are generated using the `DWMSG_*` functions. Traps for `INT` (interrupt) and `ERR` (generic error) signals are established to ensure proper error handling and logging before exit.
4.  **Core Script Invocation:** The script's main action is to invoke the core data processing script:
    `"${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh" -j $JobKennung -f ${DW_EintragsNr} >> $LogDatei 2>&1`
    This implies a direct `INVOKES` relationship from `r_ausd_v_ta_bp_ref.ksh` to `k_ausd_v_ta_bp_ref.ksh`. The standard output and error of the invoked script are redirected to the current job's log file.
5.  **Status Reporting:** Upon successful completion of the core script, a success message is printed and recorded in the log file, and `DWMSG_SetzeStatusOK` is called. Finally, traps are removed, and the script exits with status `0`.

The `ta_bp_ref` table is explicitly mentioned as the subject of the contract data reconciliation, suggesting it is a primary data target or source for the invoked `k_ausd_v_ta_bp_ref.ksh`.

## 5. Transformation Logic
This script primarily handles orchestration and boilerplate logic; it does not contain direct data transformation logic. Its functions are:

*   **Environment Management:** Sourcing external `.ksh` files for common functions and environment variables. This will be replaced by Composer's environment or dedicated Python modules/libraries.
*   **Parameter Validation:** Parsing and validating command-line options (`-h`, `-s`, `-l`). In BigQuery/Composer, this would translate to Airflow DAG parameters or environment variables, with validation handled by Python code.
*   **Error Handling:** Implementing `INT` and `ERR` traps and using `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung` functions. This will be replaced by Composer's built-in error handling, retry mechanisms, and Cloud Logging for exceptions.
*   **Logging:** Dynamic log file generation (`DWMSG_Logdateiname`), entry creation (`DWMSG_ErzeugeEintrag`), and status updates (`DWMSG_SetzeStatusOK`). This will be fully migrated to Google Cloud Logging, where logs are centrally managed and accessible.
*   **Orchestration:** Invoking the child script `k_ausd_v_ta_bp_ref.ksh`. In the BigQuery ecosystem, this would be represented as a task in an Airflow DAG that executes the migrated core logic (e.g., a BigQueryOperator executing SQL or a Dataflow job).

The actual "transformation logic" for reconciling `ta_bp_ref` is expected to be within the invoked `k_ausd_v_ta_bp_ref.ksh` script, which is outside the scope of this specific `lineage_run_id`.

## 6. External Dependencies
The `external_systems` analysis for this job returned empty. However, the script itself indicates several dependencies:

*   **External Utility Scripts (within the legacy system):**
    *   `$HOME/.dw_init`: An initialization script for the user's environment.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utility.
    *   **Replacement Strategy:** These will be replaced by Python modules or libraries within the Cloud Composer environment. Common functions for logging, parameter handling, and date manipulation will be re-implemented using Python best practices and integrated with Google Cloud services (e.g., `logging` module, `argparse`, `datetime`).

*   **Core Processing Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh`: The actual script containing the reconciliation logic.
    *   **Replacement Strategy:** This script will need to be migrated separately. Its logic (likely SQL) will be translated to BigQuery SQL, potentially as a stored procedure or a series of SQL statements executed by an Airflow BigQueryOperator. The invocation from `r_ausd_v_ta_bp_ref.ksh` will become a task dependency in the Airflow DAG.

*   **Database Interaction:** While not explicitly shown in this wrapper script, the mention of `ta_bp_ref` table for "Vertragsdatenabgleich" strongly implies interaction with a database for reading and writing contract data.
    *   **Replacement Strategy:** This database is assumed to be part of the legacy environment (e.g., Oracle, DB2). The equivalent data will be present in BigQuery. The `k_ausd_v_ta_bp_ref.ksh` script (when migrated) will read from and write to BigQuery tables.

## 7. Unresolved / Risks
*   **Unresolved Targets:** The `unresolved_targets` analysis for this job returned empty.
*   **Core Script (`k_ausd_v_ta_bp_ref.ksh`) Logic:** The content and complexity of `k_ausd_v_ta_bp_ref.ksh` are unknown. This is the critical piece containing the actual data reconciliation logic for `ta_bp_ref`. Its migration complexity will heavily influence the overall job. Without its analysis, the full scope of transformation is not yet understood.
*   **Sourced Utility Scripts:** The exact functionalities of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` were not analyzed in detail. Re-implementing these in Python requires understanding all their features.
*   **Parameter Usage:** The script defines `-s` and `-l` parameters but doesn't use them. This needs to be confirmed if they are indeed vestigial or implicitly used by the invoked script.
*   **Migration Bucket `semi_auto` (B2):** This indicates that some manual effort will be required, likely for redesigning the orchestration logic, re-implementing utility functions, and integrating with Cloud services, rather than a direct automated conversion.

## 8. Build Plan
The migration of this job will involve translating the KornShell wrapper script into an Apache Airflow DAG orchestrated by Google Cloud Composer, with its core logic executed within BigQuery.

1.  **Define Airflow DAG Structure (Python):**
    *   Create a new Python file for the Airflow DAG (e.g., `r_ausd_v_ta_bp_ref_dag.py`).
    *   Define DAG parameters that correspond to the legacy script's command-line arguments (if any become relevant).
    *   **Language:** Python

2.  **Re-implement Utility Functions (Python):**
    *   Create Python modules or functions to replace the functionality of `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh`.
    *   Integrate with Python's standard `logging` module and potentially Cloud Logging for error and information messages.
    *   **Language:** Python

3.  **Migrate Core Logic (`k_ausd_v_ta_bp_ref.ksh`) (BigQuery SQL / Python):**
    *   **Prerequisite:** Analyze and migrate the `k_ausd_v_ta_bp_ref.ksh` script. This will likely result in one or more BigQuery SQL queries, possibly wrapped in a BigQuery Stored Procedure, or a Python script leveraging the BigQuery API.
    *   **Language:** BigQuery SQL / Python

4.  **Create Airflow Tasks:**
    *   **Start Task:** A `BashOperator` or `PythonOperator` to handle initial setup (if any) and parameter processing, mimicking the initial wrapper logic.
    *   **Execute Core Logic Task:** An `BigQueryOperator` to execute the migrated BigQuery SQL queries/stored procedures from `k_ausd_v_ta_bp_ref.ksh`, or a `PythonOperator` that calls the Python-based data processing logic. Pass job-specific parameters (like `JobKennung`, `DW_EintragsNr`) to these tasks.
    *   **Logging and Error Handling:** Leverage Airflow's native logging to Cloud Logging. Implement Python-based error handling within the DAG for more granular control if needed.
    *   **Success Task:** A `PythonOperator` to log the final success status.
    *   **Language:** Python (for Airflow DAG definition and operators)

5.  **Deployment:**
    *   Deploy the `r_ausd_v_ta_bp_ref_dag.py` to Google Cloud Composer.
    *   **Tool:** `gcloud composer environments storage dags import` or equivalent CI/CD pipeline.

This build plan is dependent on the analysis and migration of the `k_ausd_v_ta_bp_ref.ksh` script, which is currently an unknown.