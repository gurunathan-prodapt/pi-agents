# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_bp_ta_bpr_instance.ksh`. The script serves as an orchestration wrapper for the initial provisioning of selected basic products for BERT. Its primary function is to generate a snapshot of contract cache data from the Data Warehouse (DWH) and make it available for "Forderungsscoring." It handles parameter parsing, environment setup, date determination, and invokes a core processing kernel script (`k_ausd_bp_ta_bpr_instance.ksh`) to perform the actual data extraction and processing. The job also includes restart/resume capabilities based on a `Wiederanlaufwert` (restart value) and logs its execution status and errors.

## 2. Source Inventory
The job consists of a single KornShell script:
- **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh`
  - **Technology**: KornShell (shell script)
  - **Category**: Shell
  - **Purpose**: Orchestration script
  - **Complexity Tier**: Medium
  - **Automation Bucket**: Semi-auto

## 3. Target Architecture
The target platform is Google Cloud Platform, primarily leveraging BigQuery for data processing and storage, and Cloud Composer (Airflow) for orchestration.

-   **Main Script Migration**: The `r_ausd_bp_ta_bpr_instance.ksh` orchestration script will be migrated into a BigQuery Stored Procedure. This procedure will handle parameter validation, date logic, logging, and the invocation of the migrated kernel logic.
-   **Kernel Script Migration**: The core business logic residing in `k_ausd_bp_ta_bpr_instance.ksh` will be migrated into a separate BigQuery Stored Procedure or a series of SQL statements within the main procedure, depending on its complexity and data flow.
-   **Logging and Monitoring**: The script's extensive logging and error handling mechanisms will be replaced by inserts into dedicated BigQuery log/audit tables. GCP Cloud Logging can also be integrated for comprehensive monitoring.
-   **Parameter Management**: Command-line parameters will be converted into input parameters for the BigQuery Stored Procedure. Defaulting logic will be handled within the procedure.
-   **External Utility Scripts**: Sourced utility scripts (for error handling, parameter validation, date handling) will be re-implemented as BigQuery UDFs, helper SQL procedures, or integrated directly into the main stored procedure where appropriate.
-   **Orchestration**: The overall job execution will be orchestrated using Cloud Composer (Airflow), defining a DAG that calls the BigQuery Stored Procedure.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_bpr_instance.ksh` script acts as an orchestrator.
-   **Inputs**:
    -   `p_stichtag` (processing date `DDMMYYYY`)
    -   `p_wiederanlaufWert` (restart value, default 0)
    -   Environment variables and values from sourced configuration/utility scripts.
-   **Internal Flow**:
    1.  Environment initialization by sourcing `.dw_init`.
    2.  Loading of utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
    3.  Parameter parsing using `getopts`.
    4.  Defaulting `p_wiederanlaufWert` to `0` if not provided.
    5.  Determining `v_sysdate` (system date).
    6.  Defaulting `p_stichtag` to `v_sysdate` if not provided.
    7.  Validation of parameters using `pruefeParameterGesetzt`.
    8.  Initialization of logging variables and setup of `trap` handlers.
    9.  Invocation of the kernel script `k_ausd_bp_ta_bpr_instance.ksh` with derived parameters.
    10. Logging of job status (START, SUCCESS, ERROR).
-   **Outputs**:
    -   Log messages written to a dynamic log file.
    -   Updates to an internal job status (via `DWMSG_SetzeStatusOK`, `DWMSG_MeldeFehler`).
    -   The kernel script `k_ausd_bp_ta_bpr_instance.ksh` is expected to produce data outputs (e.g., to "FOS-Tabelle") and consumes inputs (e.g., "DWH_VERTRAG_ID").

## 5. Transformation Logic
The transformation logic within `r_ausd_bp_ta_bpr_instance.ksh` is primarily for orchestration and parameter handling.
-   **Parameter Initialization**: `p_stichtag` and `p_wiederanlaufWert` are extracted from command-line arguments. `p_wiederanlaufWert` defaults to `0` if not set.
-   **Date Determination**: `v_sysdate` is obtained, and `p_stichtag` defaults to `v_sysdate` if not explicitly provided.
-   **Parameter Validation**: A helper function `pruefeParameterGesetzt` validates required parameters. Errors lead to logging and script termination.
-   **Error Handling**: The script uses `trap` commands to catch signals and errors, invoking `DWMSG_Fehlerbehandlung` for centralized error logging.
-   **Kernel Script Invocation**: The core action is calling `k_ausd_bp_ta_bpr_instance.ksh` with parameters: `-j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert}`.
-   **Success Handling**: Upon successful execution of the kernel script, a success message is logged, and the job status is set to OK.

The actual data transformations (filtering, selection based on `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, `DWH_VERTRAG_ID`) are presumed to reside within the `k_ausd_bp_ta_bpr_instance.ksh` kernel script. These will need to be translated into set-based BigQuery SQL operations.

## 6. External Dependencies
The script has the following external dependencies:

-   **Environment Initialization**: `. $HOME/.dw_init`
    -   **Replacement**: Environment variables and their values should be managed through Cloud Composer's environment variables or fetched from a BigQuery configuration table.
-   **Error Handling Script**: `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
    -   **Replacement**: Functionality to be integrated into the BigQuery Stored Procedure using `EXCEPTION WHEN ERROR THEN` blocks and logging to BigQuery audit tables.
-   **Parameter Handling Script**: `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
    -   **Replacement**: Logic to be integrated directly into the BigQuery Stored Procedure's parameter validation section.
-   **Date Handling Script**: `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
    -   **Replacement**: BigQuery's native date and time functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`, `PARSE_DATE()`) will be used.
-   **Kernel Script**: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh`
    -   **Replacement**: This is a critical dependency. This kernel script must be migrated separately, likely into its own BigQuery Stored Procedure (`k_ausd_bp_ta_bpr_instance_sql`). The main orchestrator procedure will then `CALL` this kernel procedure.
-   **External Systems**: Although not directly referenced in the assembled job's `external_systems` output for the main script, the underlying data sources and targets (e.g., DWH, "FOS-Tabelle") are implicit external dependencies of the kernel script. These will be mapped to BigQuery tables.

## 7. Unresolved / Risks
-   **Shell `trap` commands**: The `trap` mechanism in KornShell for signal handling and error management does not have a direct BigQuery equivalent. This needs to be carefully reimplemented using BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks for error handling and Cloud Composer's capabilities for job failure/retry mechanisms.
-   **Sourcing External Scripts**: The pattern of sourcing multiple `.ksh` files for common functions requires careful re-evaluation. These functions should either be translated into BigQuery UDFs or integrated directly into the main stored procedure to avoid external file dependencies in BigQuery.
-   **Kernel Script Complexity**: The design assumes that the core logic in `k_ausd_bp_ta_bpr_instance.ksh` can be effectively translated into BigQuery SQL. If this script contains complex procedural logic, it might require a more involved migration to, for example, a series of BigQuery scripting blocks or even a Dataflow/Spark job if it's very complex and set-based operations are not feasible.
-   **Dynamic Filenames**: The log file name is constructed dynamically. In BigQuery, logging will target a dedicated table, removing the need for dynamic file creation.

## 8. Build Plan
The migration will follow these steps:

1.  **Define BigQuery Schema**:
    *   Create a `job_log` table in BigQuery to capture job execution details, status, parameters, and error messages.
    *   Create any necessary configuration tables to store environment-specific settings or parameters currently managed by sourced shell scripts.
    *   Ensure target data tables (e.g., for "FOS-Tabelle") are defined in BigQuery.
2.  **Migrate Utility Functions**:
    *   Translate common date utility functions (from `h_alis_date.ksh`) into BigQuery UDFs or embed them as `FORMAT_DATE`, `PARSE_DATE` calls.
    *   Re-implement parameter validation and error messaging logic (from `h_alis_parameter.ksh`, `f_alis_msgerr.ksh`) within the BigQuery stored procedures.
3.  **Migrate Kernel Script**:
    *   **Phase 1 (Design)**: Analyze `k_ausd_bp_ta_bpr_instance.ksh` to identify its data sources, transformations, and target. Design its migration to a BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_bpr_instance_sql`) that performs the data selection, filtering, and insertion.
    *   **Phase 2 (Build)**: Develop the `k_ausd_bp_ta_bpr_instance_sql` BigQuery Stored Procedure, ensuring it handles the `Stichtag` and `Wiederanlaufwert` parameters correctly and performs the expected data operations (e.g., based on `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, `DWH_VERTRAG_ID`).
4.  **Migrate Orchestration Script (r_ausd_bp_ta_bpr_instance.ksh)**:
    *   **Phase 1 (Design)**: Design the BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_bpr_instance`) that replaces the main KornShell script.
    *   **Phase 2 (Build)**: Implement the BigQuery Stored Procedure with the following elements:
        *   Input parameters for `p_stichtag` and `p_wiederanlaufWert`.
        *   Internal variables for `ProgName`, `ProgVersion`, `JobKennung`, `DW_EintragsNr`, `v_sysdate`, etc.
        *   Logic for defaulting `p_wiederanlaufWert` and `p_stichtag`.
        *   Parameter validation and error handling using `IF` conditions and `RAISE` statements.
        *   Logging of job start, parameters, and status messages into the `job_log` table.
        *   A `BEGIN...EXCEPTION WHEN ERROR THEN...END` block to wrap the call to `k_ausd_bp_ta_bpr_instance_sql` and handle errors, logging them to `job_log`.
        *   The `CALL` statement to invoke `project.dataset.k_ausd_bp_ta_bpr_instance_sql`.
5.  **Develop Cloud Composer (Airflow) DAG**:
    *   Create an Airflow DAG that schedules and orchestrates the execution of the BigQuery Stored Procedure.
    *   The DAG should pass the necessary parameters (e.g., `stichtag`, `wiederanlaufWert`) to the BigQuery Stored Procedure.
    *   Integrate Airflow's logging and monitoring with GCP services.

**Language for Build**: BigQuery SQL (for stored procedures and UDFs), Python (for Cloud Composer DAG).