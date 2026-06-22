# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh
## 1. Purpose & Scope
This job, `r_ausd_bp_ta_iccid_vertrag.ksh`, is an orchestration script (KornShell) responsible for the initial provisioning of selected base products for BERT. Its primary function is to prepare a cutoff-date extraction of the contract cache in the Data Warehouse (DWH) and make this data available for "Forderungsscoring". It handles command-line parameter parsing for a "Stichtag" (key date) and an optional "Wiederanlaufwert" (restart value), includes robust error handling, logging, and then delegates the core data processing to another KornShell script, `k_ausd_bp_ta_iccid_vertrag.ksh`.

## 2. Source Inventory
The job is composed of a single primary source file:
*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh`
    *   **Technology**: KornShell (ksh)
    *   **Category**: shell
    *   **Tool**: KornShell
    *   **File Purpose**: Orchestration Script
    *   **Complexity Tier**: medium
    *   **Migration Flags**: None
    *   **Automation Bucket**: semi_auto

## 3. Target Architecture
Given the script's orchestration nature and the target platform being BigQuery, the recommended target architecture component for this orchestrator is **Cloud Composer (Airflow)**.

*   The existing KornShell script will be replatformed into an Airflow DAG written in Python.
*   The parameterized execution logic for `Stichtag` and `Wiederanlaufwert` will be translated into Airflow DAG parameters (e.g., using `{% ds %}` or custom DAG arguments).
*   The invoked kernel script (`k_ausd_bp_ta_iccid_vertrag.ksh`) will need its own migration to a BigQuery-compatible format (e.g., BigQuery SQL script, Dataflow job, or PySpark on Dataproc, depending on its internal logic). This orchestrator's role will be to invoke that migrated component.
*   Logging and error handling will leverage Airflow's native logging capabilities and potentially integrate with Google Cloud Logging.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_iccid_vertrag.ksh` script acts as an orchestrator.
*   **Inputs**:
    *   Command-line parameters: `-s <DDMMYYYY>` (Stichtag), `-l <restart_value>`.
    *   System date (if Stichtag is not provided).
    *   Environment variables and functions sourced from helper scripts (`$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
*   **Process**:
    1.  Initialize environment and load utility functions.
    2.  Parse command-line arguments.
    3.  Validate parameters and initialize default values (e.g., `p_wiederanlaufWert=0` if not provided, `p_stichtag=v_sysdate` if not provided).
    4.  Set up logging and error traps.
    5.  Invoke the core data processing script: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_iccid_vertrag.ksh` with the parsed parameters (`-j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert}`).
    6.  Log success or handle errors.
*   **Outputs**:
    *   Log file (`$LogDatei`).
    *   Execution of the `k_ausd_bp_ta_iccid_vertrag.ksh` script, which is responsible for the actual data transformation and output. The output of this script is redirected to `$LogDatei`.

## 5. Transformation Logic
The `r_ausd_bp_ta_iccid_vertrag.ksh` script's transformation logic primarily concerns workflow control and parameter management, not direct data transformation.

*   **Parameter Handling**: The script uses `getopts` to parse `-s` (Stichtag) and `-l` (Wiederanlaufwert). This will be replaced by Airflow's parameter passing mechanisms (e.g., `{{ ds }}` for `execution_date` or custom `params` in `dags.py`).
*   **Date Determination**: The logic to get the system date (`DWDate_Gib_Zeitraum`) and default `p_stichtag` if not provided will be translated to Python `datetime` operations within the Airflow DAG.
*   **Error Handling and Logging**: The `DWMSG_` functions and `trap` statements will be replaced by Airflow's task-level error handling, retry mechanisms, and integrated logging to Cloud Logging.
*   **Orchestration**: The core action, `"${Name_Kernskript}"`, will be translated into an Airflow task (e.g., a `BashOperator` if `k_ausd_bp_ta_iccid_vertrag.ksh` remains a shell script or a `BigQueryOperator`, `DataflowOperator`, etc., if it's migrated to a GCP service).

## 6. External Dependencies
The `lineage_assembled_jobs` reported no external systems (`external_systems: []`). However, the `references_out` section from `file_analysis` and the reverse-engineered design indicate several dependencies:

*   **Sourced Environment**: `. $HOME/.dw_init` (initializes environment variables). This will be managed by the Airflow environment configuration or KubernetesPodOperator environment variables.
*   **Utility Scripts/Functions**:
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error messaging).
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing helpers).
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling helpers).
    These KornShell utility scripts will need to be re-implemented in Python as part of the Airflow DAG or as common Python modules accessible to the DAG.
*   **Core Data Processing Script**: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_iccid_vertrag.ksh`. This is the most critical dependency and will require its own migration plan, likely to BigQuery SQL, Dataflow, or PySpark, as it performs the actual data operations. The Airflow DAG will then invoke this migrated component.

## 7. Unresolved / Risks
The `lineage_assembled_jobs` reported no unresolved targets (`unresolved_targets: []`).
*   **Risk: Complexity of `k_ausd_bp_ta_iccid_vertrag.ksh`**: The main risk lies in the complexity and migration path of the `k_ausd_bp_ta_iccid_vertrag.ksh` script, which is invoked by this orchestrator. Its internal logic (e.g., SQL queries, data manipulation) will dictate the appropriate BigQuery-native migration strategy.
*   **Risk: Utility Script Translation**: The conversion of the KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) to Python for Airflow compatibility needs to ensure functional equivalence.
*   **Semi-Automatic Migration**: The `semi_auto` bucket indicates that some manual effort will be required for the conversion, particularly in re-implementing the shell logic into a Python-based Airflow DAG.

## 8. Build Plan
The migration plan for `r_ausd_bp_ta_iccid_vertrag.ksh` to Google Cloud Platform will involve:

1.  **Analyze `k_ausd_bp_ta_iccid_vertrag.ksh`**: Perform a detailed analysis of the core processing script `k_ausd_bp_ta_iccid_vertrag.ksh` to determine its migration strategy (e.g., direct BigQuery SQL, Dataflow, Dataproc PySpark). This will dictate the Airflow task type.
2.  **Translate Utility Functions to Python**: Convert the functionality of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` into Python modules or functions that can be used within the Airflow environment.
3.  **Design Airflow DAG**: Create a Python-based Airflow DAG (`r_ausd_bp_ta_iccid_vertrag_dag.py`) that implements the orchestration logic:
    *   Define DAG parameters for `stichtag` and `wiederanlaufwert`.
    *   Implement date defaulting logic using Python.
    *   Define a task for each logical step (e.g., parameter validation, invoking the core processing).
4.  **Implement Airflow Tasks**:
    *   Create a task to validate input parameters.
    *   Create a task to invoke the migrated `k_ausd_bp_ta_iccid_vertrag.ksh` component. This task will depend on the migration of `k_ausd_bp_ta_iccid_vertrag.ksh` (e.g., `BigQueryOperator`, `BashOperator`, `DataflowPythonOperator`).
    *   Implement Airflow's native logging for execution status and errors.
5.  **Testing**: Thoroughly test the Airflow DAG with various parameters and edge cases to ensure functional equivalence with the original KornShell script.
6.  **Deployment**: Deploy the Airflow DAG to Cloud Composer.