# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh

## 1. Purpose & Scope

This job, `r_ausd_bp_ta_iccid_einzeln.ksh`, is an orchestration script primarily responsible for the initial provisioning of selected base products (e.g., FAX, Data24) for the BERT system. Its core function is to prepare a cutoff-date-based extraction of contract cache data from the Data Warehouse (DWH) and make it available for "Forderungsscoring" (FOS).

The script acts as a wrapper, handling parameter parsing, environment setup, error logging, and job status management. The actual data processing and business logic are delegated to a "kernel script": `k_ausd_bp_ta_iccid_einzeln.ksh`.

Key functionalities include:
- Parsing `Stichtag` (cutoff date) and `Wiederanlaufwert` (restart value) parameters.
- Initializing the restart value to `0` if not provided.
- Defaulting the `Stichtag` to the current system date if not explicitly set.
- Managing job logging and status updates using a proprietary `DWMSG` framework.
- Invoking the kernel script with the determined parameters.

## 2. Source Inventory

The job consists of a single KornShell script.

| File Path                                                       | Technology | Category | Complexity Tier | Automation Bucket |
| :-------------------------------------------------------------- | :--------- | :------- | :-------------- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh` | KornShell  | shell    | medium          | semi_auto         |

## 3. Target Architecture

The target architecture on Google Cloud Platform (GCP) will leverage BigQuery for data storage and processing, and potentially Cloud Composer (Airflow) or Cloud Workflows for orchestration, given the script's wrapper nature.

-   **Main Component**: The KornShell wrapper script will be migrated into a BigQuery Stored Procedure. This procedure will handle parameter validation, logging, and act as the orchestrator.
-   **Logging/Audit**: The `DWMSG` framework's logging functionalities will be replaced by dedicated audit/log tables within BigQuery. These tables will capture job execution details, parameters, status, and error messages.
-   **Date Handling**: Shell date functions will be replaced by native BigQuery date functions.
-   **Parameter Management**: Command-line parameters (`-s`, `-l`) will translate directly into BigQuery Stored Procedure input parameters.
-   **Orchestration**: The invocation of the "kernel script" (`k_ausd_bp_ta_iccid_einzeln.ksh`) will be replaced by calling a corresponding BigQuery Stored Procedure that encapsulates the kernel's logic. If the kernel script's logic is complex and involves multiple steps, it might be further broken down into separate BigQuery SQL scripts or views.
-   **Error Handling**: Shell `trap` mechanisms and `set -e` will be replaced by BigQuery's `BEGIN...EXCEPTION...END` blocks for error management within the stored procedure, combined with logging to the BigQuery audit tables. Workflow-level retries can be configured in the orchestrator (e.g., Cloud Composer).

**BigQuery Components:**
-   `project.dataset.bereitstellung_basisprodukte_bert`: BigQuery Stored Procedure for the main wrapper logic.
-   `project.dataset.k_ausd_bp_ta_iccid_einzeln`: BigQuery Stored Procedure for the core business logic (to be migrated from the kernel script).
-   `project.dataset.job_run_log`: Table for logging job execution details.
-   `project.dataset.job_error_log`: Table for logging job errors.
-   `project.dataset.job_metadata_log`: Table for storing job metadata (e.g., log file names, stichtag info).
-   `project.dataset.job_status_log`: Table for tracking job status.

## 4. Data Flow & Lineage

The current script acts as an orchestration layer.
-   **Input**: The script takes `Stichtag` (cutoff date, `DDMMYYYY`) and `Wiederanlaufwert` (restart value, integer) as command-line parameters. If `Stichtag` is not provided, it defaults to the current system date.
-   **Processing (Wrapper)**:
    1.  Initialize environment and load helper functions.
    2.  Parse input parameters.
    3.  Validate parameters.
    4.  Log job start and metadata using the `DWMSG` framework.
    5.  Invoke the kernel script (`k_ausd_bp_ta_iccid_einzeln.ksh`) with the processed parameters.
    6.  Log job completion or error status.
-   **Processing (Kernel)**: The `k_ausd_bp_ta_iccid_einzeln.ksh` (not part of this specific design, but identified as invoked) is expected to contain the core logic for:
    -   Selecting records from DWH (DWH_VERTRAG_ID, LADEDATUM, Gueltig_von, Gueltig_bis are mentioned).
    -   Potentially deleting existing entries in the FOS table based on `Wiederanlaufwert`.
    -   Writing processed data to a FOS-related target table.
-   **Output**:
    -   Log messages to a file (`LogDatei`).
    -   Job status updates through the `DWMSG` framework.
    -   Data written by the kernel script to the "FOS-Tabelle".

**Migration Data Flow:**
-   **Orchestrator**: The BigQuery Stored Procedure `project.dataset.bereitstellung_basisprodukte_bert` will be called with the `p_stichtag` and `p_wiederanlaufWert` parameters.
-   **Logging**: The stored procedure will insert records into `project.dataset.job_run_log`, `project.dataset.job_error_log`, `project.dataset.job_metadata_log`, and `project.dataset.job_status_log` for tracking and auditing.
-   **Core Logic Invocation**: The `project.dataset.bereitstellung_basisprodukte_bert` stored procedure will call `project.dataset.k_ausd_bp_ta_iccid_einzeln` (another stored procedure representing the kernel script's migration) with the relevant parameters.
-   **Data Sources/Targets**: The `project.dataset.k_ausd_bp_ta_iccid_einzeln` stored procedure will be responsible for reading from the DWH (e.g., tables like `DWH_VERTRAG_ID`) and writing to the target FOS table in BigQuery.

## 5. Transformation Logic

The current script (`r_ausd_bp_ta_iccid_einzeln.ksh`) primarily handles orchestration and parameter management. It contains no direct data transformation or aggregation logic. Its "transformation" involves:

-   **Parameter Defaulting**:
    -   `p_wiederanlaufWert` defaults to `0` if not provided.
    -   `p_stichtag` defaults to `v_sysdate` (current system date) if not provided.
-   **Parameter Validation**: Ensures `p_stichtag` is set.
-   **Environment Setup**: Sourcing various helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). These helpers define functions and set environment variables crucial for the script's execution.
-   **Job Metadata Management**: Utilizes the `DWMSG_` functions to generate job numbers, log file names, create log entries, and set job status.

**Migration to BigQuery Stored Procedure (`project.dataset.bereitstellung_basisprodukte_bert`):**
-   **Parameter Handling**: The BigQuery Stored Procedure will accept `p_stichtag` (STRING) and `p_wiederanlaufWert` (INT64) as input.
    -   `v_wiederanlaufWert` will be set based on `p_wiederanlaufWert` with a default of `0`.
    -   `v_stichtag` will be determined from `p_stichtag` or `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
-   **Validation**: An `IF` condition checks if `v_stichtag` is set. If not, an error is signaled (`SIGNAL SQLSTATE '45000'`) and logged to `job_error_log`.
-   **Logging**: `INSERT` statements will be used to populate `job_run_log`, `job_metadata_log`, and `job_status_log` tables at various stages (start, metadata update, success, error).
-   **Core Logic Delegation**: The actual invocation of the kernel script will be replaced by a `CALL` statement to the `project.dataset.k_ausd_bp_ta_iccid_einzeln` BigQuery Stored Procedure, passing the processed parameters.
-   **Error Handling**: A `BEGIN...EXCEPTION WHEN ERROR THEN...END` block will wrap the call to the kernel stored procedure to catch errors and log them appropriately before signaling a job failure.

## 6. External Dependencies

The `lineage_assembled_jobs` analysis indicated no explicit external systems were identified for this job. However, based on the script's content and comments:

-   **Environment Initialization**: The script sources `$HOME/.dw_init`. This file likely sets environment variables crucial for the script's execution, including potentially database connection details, directories, etc.
    -   **Replacement**: In BigQuery, environment variables will be replaced by:
        -   Direct values within the Stored Procedure.
        -   Configuration tables in BigQuery for general settings.
        -   Parameters for dynamic values.
-   **Helper Scripts**:
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing helper.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling helper.
    -   **Replacement**: These functionalities will be reimplemented using BigQuery SQL's native capabilities (e.g., `FORMAT_DATE`, `CURRENT_DATE`, `ASSERT`, `IF` statements) and by inserting data into dedicated logging tables.
-   **Invoked Kernel Script**: `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh`. This is the most significant dependency.
    -   **Replacement**: This kernel script must be migrated separately into its own BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_iccid_einzeln`). This new stored procedure will contain the actual data extraction, transformation, and loading logic.
-   **Data Sources (DWH)**: The kernel script is expected to read from "DWH" tables (e.g., `DWH_VERTRAG_ID`).
    -   **Replacement**: These will be migrated to BigQuery tables, maintaining their schema and data.
-   **Data Targets (FOS-Tabelle)**: The kernel script is expected to write to a "FOS-Tabelle".
    -   **Replacement**: This will be a BigQuery table, structured to receive the output of the kernel's processing.

## 7. Unresolved / Risks

-   **`k_ausd_bp_ta_iccid_einzeln.ksh` (Kernel Script)**: The content and complexity of this script are currently unknown. Its migration will be a separate, critical task that will determine the bulk of the actual data transformation logic. This is the primary unresolved item.
-   **`DWMSG_*` Framework**: The exact implementation details of the `DWMSG` logging and error handling functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`, `DWMSG_MeldeFehler`, `pruefeParameterGesetzt`) need to be thoroughly analyzed to ensure their functionality is accurately replicated in BigQuery logging tables and error handling.
-   **`DWDate_Gib_Zeitraum` Function**: The specific date calculations performed by `DWDate_Gib_Zeitraum` need to be mapped precisely to BigQuery's date functions to ensure identical behavior.
-   **`Wiederanlaufwert` Logic**: The comment "die Eintraege bzgl. Werten >= diesem Wert werden geloescht" implies a specific "delete and re-insert" or "upsert" strategy for the FOS table. This logic resides in the kernel script and needs careful migration to BigQuery, potentially using `MERGE` statements or a combination of `DELETE` and `INSERT`.
-   **Error Handling Parity**: The shell `trap` commands for `INT`, `STOP`, `CONT`, and `ERR` signals provide robust process-level error handling. While BigQuery Stored Procedures offer `EXCEPTION` blocks, complete replication of signal handling might require external orchestration (e.g., Cloud Composer with its monitoring and restart capabilities).
-   **`Stichtag` Default Logic**: The script's comment suggests `MIN(sysdate, maxladedatum)` but the active code uses `sysdate`. The final BigQuery implementation should reflect the *active* code's logic unless a business decision dictates otherwise.

## 8. Build Plan

The migration will involve the following steps and BigQuery components:

1.  **Define BigQuery Logging & Audit Tables (DDL)**:
    -   `project.dataset.job_run_log`
    -   `project.dataset.job_error_log`
    -   `project.dataset.job_metadata_log`
    -   `project.dataset.job_status_log`
    -   Language: BigQuery SQL (DDL)

2.  **Migrate `r_ausd_bp_ta_iccid_einzeln.ksh` to a BigQuery Stored Procedure**:
    -   Create `project.dataset.bereitstellung_basisprodukte_bert` stored procedure.
    -   Implement parameter parsing and defaulting logic.
    -   Implement parameter validation using `ASSERT` or `IF` statements.
    -   Integrate `INSERT` statements for logging into the newly created audit tables.
    -   Implement `BEGIN...EXCEPTION WHEN ERROR THEN...END` for robust error handling.
    -   **Language**: BigQuery SQL

3.  **Migrate `k_ausd_bp_ta_iccid_einzeln.ksh` (Kernel Script)**:
    -   Analyze the kernel script to understand its data sources, transformations, and target.
    -   Design and create `project.dataset.k_ausd_bp_ta_iccid_einzeln` BigQuery Stored Procedure or a series of SQL scripts/views.
    -   Migrate data manipulation logic (SELECT, DELETE, INSERT/MERGE) to BigQuery SQL.
    -   **Language**: BigQuery SQL

4.  **Integrate Kernel Stored Procedure Call**:
    -   Modify `project.dataset.bereitstellung_basisprodukte_bert` to call `project.dataset.k_ausd_bp_ta_iccid_einzeln` with the appropriate parameters.
    -   **Language**: BigQuery SQL

5.  **Data Migration**:
    -   Migrate data from legacy DWH tables (e.g., `DWH_VERTRAG_ID`) to BigQuery tables.
    -   Migrate data from legacy FOS-Tabelle to a BigQuery table.
    -   **Language**: Various (e.g., BigQuery Data Transfer Service, `LOAD DATA`, `INSERT INTO ... SELECT FROM` statements, Cloud Dataflow/Dataproc if complex transformations are needed during initial load).

6.  **Orchestration (Optional, for advanced scheduling/retries)**:
    -   If complex scheduling, dependency management, or external system interactions are required, consider implementing an Airflow DAG in Cloud Composer to orchestrate the execution of the BigQuery Stored Procedures.
    -   **Language**: Python (for Airflow DAGs)

7.  **Testing**:
    -   Develop unit and integration tests for both BigQuery Stored Procedures to ensure functional equivalence with the legacy shell scripts.
    -   **Language**: BigQuery SQL, Python (for orchestration testing)