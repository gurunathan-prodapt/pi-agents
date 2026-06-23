# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_bp_ta_bpr_apn.ksh`, serves as an orchestration job responsible for the initial provisioning of selected base products (e.g., FAX, Data24) for the BERT system. Its primary function is to parse command-line arguments for a reference date (Stichtag) and a restart value (Wiederanlaufwert), set up the execution environment, and then invoke a core processing script, `k_ausd_bp_ta_bpr_apn.ksh`, with the prepared parameters. The job aims to generate a snapshot of the contract cache in the DWH and make it available for credit scoring (Forderungsscoring). It also includes logic to handle historical data, ensuring records are selected based on validity and load dates relative to the Stichtag.

## 2. Source Inventory
The job is composed of a single primary source file, which acts as an orchestrator for a larger workflow.

*   **File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh`
    *   **Technology:** KornShell (ksh)
    *   **Complexity Tier:** Medium (estimated, as `file_complexity` data was not available, but semi-automated migration suggests moderate complexity for orchestration and parameter handling.)
    *   **Automation Bucket:** semi_auto
    *   **Summary:** Orchestrates the initial provision of selected base products for BERT, parses command-line arguments, sets up the environment, and calls a core processing script.
    *   **Key Dependencies:**
        *   Sourced utility scripts:
            *   `$HOME/.dw_init` (environment initialization)
            *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
            *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing helpers)
            *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date handling helpers)
        *   Invoked core script:
            *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` (core processing logic)

## 3. Target Architecture
The target platform for this migration is Google BigQuery for data storage and processing, and Apache Airflow for workflow orchestration.

*   **Orchestration:** The KornShell script `r_ausd_bp_ta_bpr_apn.ksh` will be migrated to an **Airflow DAG written in Python**. This DAG will manage the execution flow, parameter passing, and error handling.
*   **Data Processing:** The core logic currently residing in `k_ausd_bp_ta_bpr_apn.ksh` (the invoked script) and any associated SQL will be migrated to **BigQuery SQL queries or BigQuery Stored Procedures**. Any complex procedural logic not suitable for SQL will be implemented using **Python scripts executed via Airflow's PythonOperator**.
*   **Data Storage:** All source and target tables, as identified in the broader lineage, will reside in **BigQuery datasets**.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_bpr_apn.ksh` script primarily functions as an orchestrator, preparing execution parameters and invoking a core script. It does not directly interact with data sources or targets.

*   **Inputs (to `r_ausd_bp_ta_bpr_apn.ksh`):**
    *   Command-line arguments: `Stichtag` (reference date, DDMMYYYY format, optional) and `Wiederanlaufwert` (restart value, optional, defaults to 0).
    *   Environment variables set by `$HOME/.dw_init` and potentially others.
*   **Processing (`r_ausd_bp_ta_bpr_apn.ksh`):**
    *   Initializes environment and error handling.
    *   Parses input parameters.
    *   Determines `p_stichtag`: If not provided via command line, it defaults to the current system date (`v_sysdate`). Original code had a commented out section to get `MIN(sysdate,maxladedatum)` from `DWH$TA_C_VERTRAG`. This should be reviewed during migration.
    *   Initializes logging via `DWMSG_*` functions.
    *   Invokes the core script `k_ausd_bp_ta_bpr_apn.ksh` with `JobKennung`, `p_stichtag`, `DW_EintragsNr`, and `p_wiederanlaufWert`.
*   **Outputs (from `r_ausd_bp_ta_bpr_apn.ksh`):**
    *   Execution of `k_ausd_bp_ta_bpr_apn.ksh`.
    *   Logging information to `$LogDatei`.
*   **Downstream Data Flow (from `k_ausd_bp_ta_bpr_apn.ksh` - inferred):** The core script `k_ausd_bp_ta_bpr_apn.ksh` is expected to read from source tables (e.g., `DWH$TA_C_VERTRAG` based on the commented `FOSHoleLadedatum` call) and write to target tables in BigQuery for the Forderungsscoring system. Specific table names are not available directly from this script's lineage.

## 5. Transformation Logic
The transformation logic within `r_ausd_bp_ta_bpr_apn.ksh` is focused on control flow and parameter management.

*   **Parameter Handling:**
    *   The `getopts` mechanism for `s:` (Stichtag) and `l:` (Wiederanlaufwert) will be replaced by Airflow DAG parameters or Python argument parsing (e.g., `argparse`).
    *   Defaulting `p_wiederanlaufWert` to 0 will be handled in Python.
*   **Date Determination:**
    *   The `DWDate_Gib_Zeitraum` function for getting `v_sysdate` will be replaced by Python's `datetime` module.
    *   The conditional logic for `p_stichtag` (using command-line input or `v_sysdate`) will be directly translated to Python conditional statements. The commented-out logic regarding `maxladedatum` from `DWH$TA_C_VERTRAG` needs review: if it's required functionality, it will need to be re-implemented in BigQuery SQL and fetched via a BigQuery hook in Airflow.
*   **Environment Setup:**
    *   Sourcing `.dw_init`: Environment variables (like `BERT_DIR_ROOT`) and paths will be configured within the Airflow environment, possibly via Airflow Variables or Connections, or by defining them within the Python DAG itself.
    *   Utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`): These will be re-implemented as Python functions or classes, or replaced by native Airflow/Python logging, parameter, and date handling mechanisms.
*   **Error Handling and Logging:**
    *   The `DWMSG_*` functions and `trap` statements will be replaced by Airflow's native logging capabilities, task retry mechanisms, and error callbacks. Airflow handles task failures and retries inherently, and logs are automatically collected.
*   **Core Script Invocation:**
    *   The line `${Name_Kernskript} -j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert} >> $LogDatei 2>&1` will be replaced by an Airflow task. The specific operator will depend on the migration strategy for `k_ausd_bp_ta_bpr_apn.ksh`. If it becomes BigQuery SQL, a `BigQueryOperator` or `BigQueryExecuteQueryOperator` could be used. If it becomes a Python script, a `PythonOperator` would be appropriate.

## 6. External Dependencies
The `lineage_assembled_jobs` analysis indicated no direct external system dependencies for this specific job. This implies that any interactions with databases or other external systems are handled by the invoked `k_ausd_bp_ta_bpr_apn.ksh` script or other downstream processes not directly part of this orchestrator.

*   **Identified:** None directly from `r_ausd_bp_ta_bpr_apn.ksh`.
*   **Replacement Strategy:** If `k_ausd_bp_ta_bpr_apn.ksh` has external dependencies (e.g., an Oracle database), those connections will be established in Airflow using `Connections` and appropriate operators (e.g., `OracleToGCSOperator`, `BigQueryOperator`).

## 7. Unresolved / Risks

*   **Core Script Logic (`k_ausd_bp_ta_bpr_apn.ksh`):** The most significant risk is that the detailed logic of the invoked core script is unknown. This script is where the actual data extraction and transformation likely occur. A separate, detailed analysis and migration design for `k_ausd_bp_ta_bpr_apn.ksh` is critical.
*   **Missing Complexity Analysis:** The `file_complexity` table returned no rows for `r_ausd_bp_ta_bpr_apn.ksh`. This means specific complexity signals and migration flags (e.g., for dynamic SQL, complex data types, etc.) are unknown, which could lead to underestimating the migration effort.
*   **Utility Script Migration:** The internal logic and any implicit dependencies within `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, and `.dw_init` need to be fully understood and re-implemented or replaced with BigQuery/Airflow native functions.
*   **German Language:** Comments and variable names are in German, requiring careful review to ensure correct interpretation of business logic during migration.
*   **Commented-out Code:** The `AL??` comments around `FOSHoleLadedatum` suggest potentially incomplete or deactivated logic in the original script. This needs clarification on whether this functionality is still required or can be disregarded.

## 8. Build Plan
The migration will focus on re-implementing the orchestration logic in Airflow and preparing for the downstream core processing script migration.

1.  **Migrate `r_ausd_bp_ta_bpr_apn.ksh` to an Airflow DAG:**
    *   **Target Language:** Python (Airflow DAG).
    *   **Description:** Create a Python file (`r_ausd_bp_ta_bpr_apn_dag.py`) for an Airflow DAG.
        *   Define DAG parameters for `stichtag` (defaulting to current date if not provided) and `wiederanlaufwert` (defaulting to 0).
        *   Implement parameter validation logic.
        *   Replace `DWMSG_*` logging and error handling with Airflow's built-in `logging` and task retry mechanisms.
        *   Create a task (e.g., `BashOperator` or `PythonOperator`) to invoke the migrated `k_ausd_bp_ta_bpr_apn.ksh` logic, passing the resolved parameters. The exact operator will depend on the migration of `k_ausd_bp_ta_bpr_apn.ksh`.
        *   Ensure proper Airflow task dependencies and failure handling.

2.  **Migrate Utility Scripts:**
    *   **Target Language:** Python.
    *   **Description:** Re-implement the functionality of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` as Python utility functions or integrate directly into the Airflow DAG if simple enough. Environment setup from `.dw_init` will be managed via Airflow environment configurations.

3.  **Migrate `k_ausd_bp_ta_bpr_apn.ksh` (Core Processing Script):**
    *   **Target Language:** BigQuery SQL / Python.
    *   **Description:** (This requires a separate detailed design.) Analyze `k_ausd_bp_ta_bpr_apn.ksh` to extract data transformation logic.
        *   **SQL components:** Convert any SQL statements to BigQuery Standard SQL, potentially creating BigQuery Stored Procedures or views.
        *   **Procedural components:** Re-write non-SQL data manipulation, file handling, or complex logic in Python.
        *   The Airflow DAG from step 1 will then call these migrated BigQuery or Python components.