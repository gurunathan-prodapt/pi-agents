# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh

## 1. Purpose & Scope
This shell script, `r_ausd_bp_ta_iccid_einzeln.ksh`, serves as an orchestration wrapper for an ETL job. Its primary purpose is to manage the initial provisioning of selected basic products (e.g., FAX, Data24) for the BERT system. This involves extracting contract cache data from the Data Warehouse (DWH) based on a specified snapshot date and preparing it for credit scoring (FOS). The script handles parameter parsing for the snapshot date (`-s`) and a restart value (`-l`), initializes an error handling framework, determines the processing date, and then invokes a core "kernel script" (`k_ausd_bp_ta_iccid_einzeln.ksh`) to perform the actual data processing. It also manages logging and job status.

The scope of this migration design document focuses on the `r_ausd_bp_ta_iccid_einzeln.ksh` wrapper script, outlining its conversion to a BigQuery-native solution and identifying the dependencies and requirements for the underlying kernel script.

## 2. Source Inventory
The job consists of a single primary source file.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh`
    *   **Technology:** KornShell Script (ksh)
    *   **Summary:** Orchestrates the initial provision of selected basic products for BERT by extracting contract cache data from DWH based on a snapshot date and making it available for credit scoring.
    *   **Purpose:** ETL Orchestrator / Wrapper
    *   **Complexity Tier:** Not available (missing `file_complexity` data)
    *   **Automation Bucket:** Not available (missing `automation_rate` data)
    *   **Description:** This script initializes the environment, parses command-line arguments for a snapshot date and a restart value, sets up error trapping and logging, and then executes a dependent "kernel script" (`k_ausd_bp_ta_iccid_einzeln.ksh`) with the processed parameters.

## 3. Target Architecture
The migrated solution will primarily leverage Google Cloud Platform (GCP) services, with BigQuery as the central data warehouse and processing engine.

*   **Orchestration:** Cloud Composer (Apache Airflow) will be used to orchestrate the overall workflow, including the execution of the migrated components.
*   **Main Logic (Wrapper):** The functionality of `r_ausd_bp_ta_iccid_einzeln.ksh` will be migrated into a BigQuery Stored Procedure. This procedure will handle parameter parsing, date determination, validation, and logging, similar to its original shell script function.
*   **Main Logic (Kernel):** The `k_ausd_bp_ta_iccid_einzeln.ksh` script (which contains the core data extraction and transformation logic) will need to be separately designed and implemented, likely as one or more BigQuery SQL scripts or views, or potentially a separate BigQuery Stored Procedure, depending on its complexity. This design document assumes this kernel script will be migrated to a BigQuery-native component.
*   **Logging and Auditing:** Dedicated BigQuery tables will be established for job logging and status tracking (`project.dataset.job_log`, `project.dataset.job_status`).
*   **Input Data:** The DWH source will be represented by BigQuery tables (e.g., `DWH_TA_C_VERTRAG` if it's the contract cache table mentioned in the comments).
*   **Output Data:** The "FOS-Tabelle" will be represented by a BigQuery table, where the processed data will be made available for downstream systems.

## 4. Data Flow & Lineage
The lineage of this job, based on the wrapper script and its description, is as follows:

1.  **Orchestration (Cloud Composer/Airflow):** Initiates the job, triggering the BigQuery Stored Procedure.
2.  **Wrapper BigQuery Stored Procedure:**
    *   Receives input parameters (`p_stichtag`, `p_wiederanlaufWert`).
    *   Initializes job variables, determines system date and snapshot date.
    *   Performs parameter validation.
    *   Logs job start and status updates to `project.dataset.job_log` and `project.dataset.job_status`.
    *   Invokes the "Kernel Logic" (a separate BigQuery component).
    *   Logs job success or failure.
3.  **Kernel Logic (BigQuery SQL/Stored Procedure):**
    *   **Reads From:** Data Warehouse (DWH) tables, specifically contract cache data (e.g., `DWH_TA_C_VERTRAG`). The selection criteria involve `Gueltig_von <= Stichtag < Gueltig_bis` and `LADEDATUM < Stichtag`. It also considers `DWH_VERTRAG_ID > Wiederanlaufwert` for restart scenarios.
    *   **Transforms:** Extracts, filters, and potentially transforms contract cache data. The details are in the `k_ausd_bp_ta_iccid_einzeln.ksh` script, which is out of scope for detailed analysis in *this* document.
    *   **Writes To:** The target "FOS-Tabelle" in BigQuery.
4.  **Logging & Status:** All stages of the process update central BigQuery logging tables.

**Execution Order:**
1.  Initialize environment and parameters.
2.  Determine snapshot date.
3.  Validate parameters.
4.  Log job start.
5.  Execute core data processing logic (Kernel Script equivalent).
6.  Log job completion/status.

## 5. Transformation Logic
The transformation logic contained within the `r_ausd_bp_ta_iccid_einzeln.ksh` wrapper script is primarily concerned with control flow and parameter management, rather than business data transformations.

*   **Parameter Handling:**
    *   Accepts `Stichtag` (snapshot date) in `DDMMYYYY` format via `-s`.
    *   Accepts `Wiederanlaufwert` (restart value) via `-l`.
    *   If `Wiederanlaufwert` is not provided, it defaults to `0`.
*   **Date Determination:**
    *   Obtains the current system date in `DDMMYYYY` format.
    *   If `Stichtag` is not provided via parameters, it defaults to the system date. (Note: The commented-out logic `MIN(sysdate,maxladedatum)` for `DWH_TA_C_VERTRAG` was not active in the provided script).
*   **Validation:** Checks if the `Stichtag` parameter is set. If not, an error is reported.
*   **Orchestration:** Constructs the command to execute the kernel script (`k_ausd_bp_ta_iccid_einzeln.ksh`) with the derived parameters (`JobKennung`, `Stichtag`, `DW_EintragsNr`, `Wiederanlaufwert`).
*   **Error Handling and Logging:** Sets up shell traps for error handling, writes job messages and status to a log file, and updates job status. This will be replaced by BigQuery error handling and logging to tables.

The actual data extraction and transformation (filtering records by `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `DWH_VERTRAG_ID`) is expected to reside within the kernel script, which requires its own detailed migration design.

## 6. External Dependencies
The `r_ausd_bp_ta_iccid_einzeln.ksh` script exhibits the following dependencies:

*   **Internal Shell Utilities:**
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helper.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling helper.
    *   **Replacement Strategy:** These shell utilities will be replaced by BigQuery-native functionalities. Environment variables will become BigQuery Stored Procedure parameters or configuration tables. Error handling and date functions will use BigQuery's built-in capabilities or custom UDFs/routines if complex. Parameter parsing is handled natively by stored procedure input parameters.
*   **Core Kernel Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh`: This is the main data processing component.
    *   **Replacement Strategy:** This script's functionality needs to be migrated to a BigQuery-native solution, likely a BigQuery Stored Procedure or a set of SQL statements that perform the actual data extraction, transformation, and loading. The BigQuery wrapper procedure will `CALL` this migrated component.
*   **Data Warehouse (DWH):** The script's purpose explicitly mentions extracting data from DWH.
    *   **Replacement Strategy:** DWH will be represented by BigQuery tables. The original source tables will be ingested into BigQuery.
*   **FOS-Tabelle (Target):** The script makes data available to a "FOS-Tabelle".
    *   **Replacement Strategy:** This will be a BigQuery table, serving as the output for the processed data.

## 7. Unresolved / Risks
*   **Missing Complexity/Automation Data:** The `file_complexity` and `automation_rate` for the source file were not available. This prevents a precise assessment of effort and migration strategy for the wrapper script. Assuming its current identification as a shell script, it falls into a semi-automated to manual migration bucket depending on its complexity and interaction patterns.
*   **Kernel Script (`k_ausd_bp_ta_iccid_einzeln.ksh`) Analysis:** The most significant unresolved item is the content and logic of the `k_ausd_bp_ta_iccid_einzeln.ksh` script. This "kernel" script is presumed to contain the actual business logic for data extraction and transformation from DWH to the FOS table. Without its analysis, the full data flow, transformations, and potential complexities are unknown. This script will require its own migration design.
*   **Data Model Clarity:** The exact schema and table names of "DWH contract cache data" and "FOS-Tabelle" are not explicitly defined beyond generic mentions. Detailed data dictionaries will be required.
*   **Restart Logic Details:** While the `Wiederanlaufwert` parameter is handled, the exact implementation of the restart logic within the kernel script (e.g., how it deletes entries >= `Wiederanlaufwert`) needs to be understood and replicated in BigQuery.
*   **Error Handling Nuances:** The shell script's `trap` mechanism provides specific error handling behavior. Replicating this precisely in a BigQuery Stored Procedure requires careful design for `RAISE ERROR` or similar constructs, and reliance on orchestration-level retry mechanisms.

## 8. Build Plan
The migration will proceed in an iterative fashion, focusing on the wrapper script first, then addressing the kernel logic.

1.  **Define BigQuery Schemas:**
    *   Create `project.dataset.job_log` table (for job execution logs).
    *   Create `project.dataset.job_status` table (for overall job status tracking).
    *   Identify and define target schemas for DWH source tables (e.g., `DWH_TA_C_VERTRAG`) and the target `FOS_Tabelle` in BigQuery.
2.  **Migrate Wrapper Logic to BigQuery Stored Procedure:**
    *   **Language:** BigQuery SQL Stored Procedure.
    *   **Name:** `project.dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper`.
    *   **Functionality:**
        *   Accepts `p_stichtag` (STRING) and `p_wiederanlaufWert` (STRING) as input parameters.
        *   Implements the logic for defaulting `p_wiederanlaufWert` and `p_stichtag`.
        *   Derives the system date using `CURRENT_DATE()` and `FORMAT_DATE()`.
        *   Includes parameter validation checks.
        *   Inserts log entries into `project.dataset.job_log` for job start, parameter validation, and completion.
        *   Updates `project.dataset.job_status`.
        *   Includes a `CALL` statement for the future BigQuery component that will represent the `k_ausd_bp_ta_iccid_einzeln.ksh` script.
        *   Implements BigQuery error handling (e.g., `EXCEPTION` blocks, `RAISE ERROR`).
3.  **Design and Migrate Kernel Script (`k_ausd_bp_ta_iccid_einzeln.ksh`):**
    *   Perform a dedicated analysis and design for `k_ausd_bp_ta_iccid_einzeln.ksh`.
    *   This will likely involve creating one or more BigQuery SQL scripts, views, or another BigQuery Stored Procedure (`project.dataset.sp_ausd_bp_ta_iccid_einzeln_kernel`).
    *   Ensure the data extraction logic (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, `DWH_VERTRAG_ID` filtering) is accurately translated.
    *   Implement the restart logic (deletion/upsert based on `p_wiederanlaufWert`).
4.  **Integrate with Orchestration (Cloud Composer/Airflow):**
    *   Create an Airflow DAG to schedule and execute the `sp_ausd_bp_ta_iccid_einzeln_wrapper` BigQuery Stored Procedure.
    *   Pass parameters (Stichtag, Wiederanlaufwert) to the Stored Procedure via Airflow operators.
    *   Configure retry mechanisms and alerts within the DAG.
5.  **Testing and Validation:**
    *   Unit test the BigQuery Stored Procedure.
    *   Integration test the entire data pipeline from Airflow through BigQuery to the target tables.
    *   Validate data accuracy and completeness against the legacy system.
6.  **Deployment:** Deploy BigQuery procedures, tables, and the Airflow DAG to the production environment.