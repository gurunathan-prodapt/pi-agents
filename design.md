# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh

## 1. Purpose & Scope

This job, `r_aurd_rechstan.ksh`, is a KornShell script designed to orchestrate the generation of invoice data snapshots for demand scoring. Its primary function is to:
*   Parse input parameters, specifically a reference date (`Stichtag`) and a restart value (`Wiederanlaufwert`).
*   Handle date calculations, defaulting the reference date to the system date if not provided.
*   Initialize environment variables and integrate with a legacy logging and error handling framework.
*   Execute a core processing script, `k_aurd_rechstan.ksh`, passing the derived parameters to it.
*   Log the job's progress and final status.

The script acts as a wrapper, with the actual data extraction and transformation logic residing in the invoked `k_aurd_rechstan.ksh`. The overall business purpose is to provide contractual data extracts from the Data Warehouse (DWH) to a demand scoring system.

## 2. Source Inventory

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh`
    *   **Technology:** KornShell Script
    *   **Category:** Shell
    *   **Tool:** KornShell
    *   **Summary:** Orchestrates invoice data snapshot generation, handles parameters, dates, logging, and invokes a core processing script.
    *   **Purpose:** ETL Orchestrator / Wrapper
    *   **Complexity Tier:** Unknown (metadata not available)
    *   **Automation Bucket:** Unknown (metadata not available)
    *   **Key Internal Invocation:** `k_aurd_rechstan.ksh` (core processing script)

## 3. Target Architecture

The migration target platform is Google Cloud Platform, specifically BigQuery for data storage and processing, and Python within an orchestration framework (e.g., Cloud Composer/Airflow) for job control.

*   **Orchestration Layer:** The `r_aurd_rechstan.ksh` wrapper logic will be re-implemented as a Python script (e.g., `r_aurd_rechstan.py`). This Python script will handle parameter parsing, date logic, and invoke the core data processing steps within BigQuery. This Python script will be managed by an Airflow DAG.
*   **Data Processing Layer:** The logic within `k_aurd_rechstan.ksh` (currently unknown) is expected to be migrated to BigQuery. This may involve:
    *   **BigQuery Tables:** For storing the "invoice data snapshots" and potentially intermediate datasets.
    *   **BigQuery Views:** For exposing curated data.
    *   **BigQuery Stored Procedures/SQL Scripts:** For performing the data extraction, transformation, and loading (ETL) operations.
*   **Logging & Monitoring:** The legacy `DWMSG_*` logging framework will be replaced with Cloud Logging. Job status and relevant metrics can be published to Cloud Monitoring or a dedicated BigQuery audit table.

## 4. Data Flow & Lineage

The current data flow primarily involves the orchestration of a core data processing component.

1.  **Start:** The `r_aurd_rechstan.ksh` script (to be migrated to `r_aurd_rechstan.py` via an Airflow DAG) is initiated, potentially with command-line parameters (`-s Stichtag`, `-l Wiederanlaufwert`).
2.  **Environment & Utilities:** The script sources environment variables (from `.dw_init`) and utility scripts for error handling, parameter parsing, and date functions. These will be replaced by Python equivalents and GCP environment configurations.
3.  **Parameter Resolution:**
    *   `p_wiederanlaufWert` is defaulted to `0` if not provided.
    *   `v_sysdate` (current system date) is obtained.
    *   `p_stichtag` is defaulted to `v_sysdate` if not provided.
    *   Parameters are validated.
4.  **Logging Initialization:** Custom logging setup (`DWMSG_*` functions) is performed. This will be replaced by Cloud Logging.
5.  **Core Process Invocation:** The script executes `${BERT_DIR_ROOT}/aufbereitung/bin/k_aurd_rechstan.ksh` with the resolved parameters.
    *   **Crucially, the detailed data flow from `k_aurd_rechstan.ksh` to its sources and targets is currently unknown.** It is expected to read contract cache data from the DWH and produce invoice data snapshots.
6.  **End:** The wrapper logs the successful completion of the job or an error.

The complete data flow will be fully defined once the `k_aurd_rechstan.ksh` script's logic is analyzed.

## 5. Transformation Logic

The `r_aurd_rechstan.ksh` script itself contains no direct data transformation logic. Its transformation logic is limited to:

*   **Parameter Mapping:** Mapping command-line arguments to internal script variables.
*   **Date Calculation:** Deriving a `Stichtag` (reference date) based on input or system date.
*   **Default Value Assignment:** Setting `p_wiederanlaufwert` to `0` if not specified.

The core data transformation logic is encapsulated within the `k_aurd_rechstan.ksh` script. This script is responsible for:

*   Reading data related to "Vertrags-Cache im DWH".
*   Applying filters (`Gueltig_von <= Stichtag < Gueltig_bis` and `LADEDATUM < Stichtag`).
*   Producing "invoice data snapshots" for "Forderungsscoring".

**Migration Approach for Transformation:**
The parameter parsing and date logic will be directly translated into Python as shown in the pseudocode by the MCP tool. The placeholder for `run_core_job` in the Python orchestrator will eventually invoke BigQuery SQL or stored procedures that implement the logic found in `k_aurd_rechstan.ksh`.

## 6. External Dependencies

The following external dependencies have been identified:

*   **Legacy Environment Initialization:** `$HOME/.dw_init`
    *   **Replacement:** This needs to be evaluated for specific environment variables set. These variables will be replaced by Airflow/Cloud Composer environment variables, GCP Secret Manager for sensitive data, or directly embedded configuration within the Python script/BigQuery assets.
*   **Legacy Framework Scripts:**
    *   `f_alis_msgerr.ksh`: Error messaging utility.
    *   `h_alis_parameter.ksh`: Parameter parsing utility.
    *   `h_alis_date.ksh`: Date handling utility.
    *   `DWDate_Gib_Zeitraum`: Custom function for date retrieval.
    *   `pruefeParameterGesetzt`: Custom function for parameter validation.
    *   `DWMSG_*` functions: Custom logging and error handling functions.
    *   **Replacement:** These custom utility functions and scripts will be re-implemented in Python, leveraging Python's standard library for date and parameter handling, and integrated with Cloud Logging for messaging and error reporting.
*   **Core Processing Script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_aurd_rechstan.ksh`
    *   **Replacement:** This is the most critical dependency. Its content must be obtained and analyzed. The data processing logic contained within will be migrated to BigQuery SQL, views, or stored procedures.
*   **System Date:** The script relies on the system's current date.
    *   **Replacement:** Python's `datetime` module will be used to obtain the current date in the target Python script.

## 7. Unresolved / Risks

*   **Missing Core Logic:** The most significant unresolved item is the content of `k_aurd_rechstan.ksh`. Without this script, the actual data sources, transformations, and target schemas cannot be definitively identified, and thus the core data processing migration cannot be fully designed or implemented. This is a **B4 (Redesign)** item until resolved.
*   **Custom Framework Complexity:** The legacy `DWMSG_*` and `h_alis_*` utility functions are custom implementations. Their full complexity and functionality need to be understood to ensure accurate re-implementation or replacement with GCP-native services.
*   **Legacy DWH Tables:** The script refers to "Vertrags-Cache im DWH". The specific tables and their schemas in the legacy DWH that `k_aurd_rechstan.ksh` interacts with are currently unknown.
*   **Error Codes and Logic:** The script uses custom error codes (e.g., `ErrNr=192`, `ErrNr=193`). A mapping or redesign of these error codes to a cloud-native error handling approach is required.

## 8. Build Plan

The migration will proceed in stages, with a critical dependency on obtaining the `k_aurd_rechstan.ksh` script.

**Phase 1: Wrapper Orchestration Migration (Partial)**

1.  **Design `r_aurd_rechstan.py`:** Based on the provided pseudocode, refine the Python script for parameter parsing, date handling, and job orchestration.
    *   **Language:** Python
    *   **Output:** `r_aurd_rechstan.py`
2.  **Implement Utility Functions:** Develop Python equivalents for `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, and other `DWMSG_*` functions, integrating them with Cloud Logging.
    *   **Language:** Python
    *   **Output:** `utils.py` (or similar library)
3.  **Create Airflow DAG:** Develop an Airflow DAG that invokes `r_aurd_rechstan.py`. This DAG will initially contain a placeholder for the core data processing task.
    *   **Language:** Python
    *   **Output:** `r_aurd_rechstan_dag.py`

**Phase 2: Core Data Processing Migration (Blocked, Requires `k_aurd_rechstan.ksh`)**

1.  **Acquire `k_aurd_rechstan.ksh`:** Obtain the source code for the core processing script.
    *   **Action:** Manual step, liaison with source system owners.
2.  **Analyze `k_aurd_rechstan.ksh`:** Understand its data sources, transformation logic, and target tables.
3.  **Design BigQuery ETL:** Based on the analysis, design the BigQuery SQL, views, or stored procedures that implement the data processing logic.
    *   **Language:** BigQuery SQL
    *   **Output:** `k_aurd_rechstan_core.sql` (or multiple SQL scripts/procedures)
4.  **Integrate Core Logic into DAG:** Update the Airflow DAG to include tasks that execute the BigQuery ETL scripts. Replace the `run_core_job` placeholder in `r_aurd_rechstan.py` to trigger these BigQuery tasks.
    *   **Language:** Python (for DAG), BigQuery SQL (for ETL)

**Phase 3: Testing and Deployment**

1.  **Unit Testing:** Test `r_aurd_rechstan.py` and the utility functions.
2.  **Integration Testing:** Test the Airflow DAG with the BigQuery ETL, ensuring correct data flow and transformations.
3.  **End-to-End Testing:** Validate the entire process, from parameter invocation to final data output in BigQuery.
4.  **Deployment:** Deploy the Airflow DAG, Python scripts, and BigQuery assets to the production GCP environment.