# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh

## 1. Purpose & Scope
The job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh`, titled "Bereitstellung Basisprodukte BERT" (Provisioning Basic Products BERT), serves the business purpose of initially provisioning selected base products (e.g., FAX, Data24) for the BERT system. Its function is to create a cutoff-date (Stichtag) snapshot of the contract cache within the Data Warehouse (DWH) and make this data available for "Forderungsscoring" (demand scoring). This KornShell script primarily acts as an orchestration layer, responsible for parsing command-line parameters, handling logging, managing error conditions, and invoking a core processing script that contains the actual data transformation logic.

## 2. Source Inventory

*   **File**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh`
    *   **Technology**: KornShell (shell)
    *   **Complexity Tier**: medium
    *   **Automation Bucket**: semi_auto
    *   **Description**: This script is an orchestrator that performs the following:
        *   Parses command-line arguments `-s` (Stichtag/cutoff date) and `-l` (Wiederanlaufwert/restart value).
        *   Initializes environment variables (via sourcing `$HOME/.dw_init`).
        *   Integrates a custom error handling and logging framework (via sourcing `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
        *   Determines the processing date, defaulting to the system date if not provided.
        *   Performs basic parameter validation.
        *   Sets up job logging and error trapping.
        *   Executes the core business logic script `k_ausd_bp_ta_apn_vertrag.ksh`, passing all relevant parameters.
        *   Records job completion status.
*   **Implied Core Logic Script**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh`
    *   **Technology**: Likely KornShell with embedded SQL or calls to SQL clients (specifics unknown from current analysis).
    *   **Description**: This script, invoked by the wrapper, is assumed to contain the actual data extraction, transformation, and loading (ETL) logic for the contract cache, including selecting data from DWH based on the cutoff date and restart value, and provisioning it for "Forderungsscoring."

## 3. Target Architecture

The migrated solution will reside within Google Cloud Platform, primarily utilizing BigQuery for data processing and storage, and Cloud Composer for orchestration.

*   **BigQuery Stored Procedures**:
    *   `project.dataset.ausd_bp_ta_apn_vertrag_wrapper`: This BigQuery Stored Procedure will replace the `r_ausd_bp_ta_apn_vertrag.ksh` script. It will handle input parameter parsing, defaulting logic, job logging, and error handling, culminating in a call to the core processing stored procedure.
    *   `project.dataset.k_ausd_bp_ta_apn_vertrag`: This BigQuery Stored Procedure will replace the `k_ausd_bp_ta_apn_vertrag.ksh` script. It will encapsulate the core data transformation, filtering, and loading logic, reading from source tables and writing to the target table.
*   **BigQuery Tables for Logging and Auditing**:
    *   `project.dataset.job_registry`: A dedicated table to store metadata for each job execution, including unique job identifiers, run parameters, start/end timestamps, and final status.
    *   `project.dataset.job_log`: A detailed log table to capture messages, warnings, and errors generated during job execution, linked to `job_registry`.
*   **Cloud Composer (Apache Airflow)**:
    *   An Airflow DAG will orchestrate the execution of the `project.dataset.ausd_bp_ta_apn_vertrag_wrapper` BigQuery Stored Procedure. This provides robust scheduling, dependency management, retry mechanisms, and monitoring capabilities.

## 4. Data Flow & Lineage

**Legacy Data Flow:**
1.  **Orchestration**: The `r_ausd_bp_ta_apn_vertrag.ksh` script is executed, possibly via a scheduler (not specified in lineage).
2.  **Parameter Processing**: It receives optional command-line parameters `-s <DDMMYYYY>` (cutoff date) and `-l <value>` (restart value).
    *   If `-s` is omitted, the `Stichtag` defaults to the system date.
    *   If `-l` is omitted, `p_wiederanlaufWert` defaults to `0`.
3.  **Environment & Utilities**: Sources `~/.dw_init` for environment setup, and utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) for common functions.
4.  **Logging**: Initializes job-specific logging, captures job metadata, and sets up error traps.
5.  **Core Processing**: Invokes `k_ausd_bp_ta_apn_vertrag.ksh` with parsed parameters. This core script is responsible for:
    *   **Source**: Reading contract data from DWH source tables (e.g., `DWH_VERTRAG_ID`). The exact tables are part of the `k_ausd_bp_ta_apn_vertrag.ksh` logic.
    *   **Transformation**: Filtering data based on `Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag` and `DWH_VERTRAG_ID > Wiederanlaufwert`.
    *   **Target**: Provisioning processed contract data to a target table (referred to as "FOS-Tabelle" in the comments) for "Forderungsscoring". This typically involves deleting existing data for the processed keys and inserting new records.
6.  **Status Update**: `r_ausd_bp_ta_apn_vertrag.ksh` updates job status in its logging framework upon completion of the core script.

**Target Data Flow (BigQuery):**
1.  **Orchestration**: A Cloud Composer DAG triggers the execution of the `project.dataset.ausd_bp_ta_apn_vertrag_wrapper` BigQuery Stored Procedure, passing `p_stichtag` and `p_wiederanlaufWert`.
2.  **Wrapper Procedure (`ausd_bp_ta_apn_vertrag_wrapper`)**:
    *   Receives input parameters.
    *   Defaults `p_stichtag` (if null) to `CURRENT_DATE()` and `p_wiederanlaufWert` (if null) to `'0'`.
    *   Inserts job start details into `project.dataset.job_registry` and `project.dataset.job_log`.
    *   **Calls Core Procedure**: Executes `CALL project.dataset.k_ausd_bp_ta_apn_vertrag(v_jobkennung, v_stichtag, CAST(v_job_nr AS STRING), v_restart_value);`.
    *   Updates job completion status (OK/ERROR) in `project.dataset.job_registry` and `project.dataset.job_log`.
3.  **Core Procedure (`k_ausd_bp_ta_apn_vertrag`)**:
    *   **Source**: Reads from BigQuery DWH contract source tables (e.g., `project.dataset.dwh_contract_cache`). *Requires detailed analysis of the original `k_ausd_bp_ta_apn_vertrag.ksh` for specific table names and schemas.*
    *   **Transformation**: Applies BigQuery SQL logic for filtering and data manipulation based on input `stichtag` and `restart_value`.
    *   **Target**: Writes the resulting contract data to a BigQuery target table (e.g., `project.dataset.fos_contract_cache`) for "Forderungsscoring" applications. This will involve `DELETE` and `INSERT` or `MERGE` operations.

## 5. Transformation Logic

The `r_ausd_bp_ta_apn_vertrag.ksh` script's transformation logic is primarily focused on control flow and parameter handling:

*   **Parameter Processing**:
    *   `getopts` for parsing `-s` and `-l`.
    *   Conditional assignment for `p_wiederanlaufWert`: `if [[ -z "$p_wiederanlaufWert" ]]; then p_wiederanlaufWert=0; fi`
    *   Conditional assignment for `p_stichtag`: `if [[ -z "$p_stichtag" ]]; then p_stichtag=$v_sysdate; fi`
*   **Date Determination**: `DWDate_Gib_Zeitraum 1 'D' 'DDMMYYYY' v_sysdate dummy` to get system date.
*   **Validation**: `pruefeParameterGesetzt Stichtag p_stichtag` for essential parameters.
*   **Orchestration**: Calls `Name_Kernskript` (`k_ausd_bp_ta_apn_vertrag.ksh`) with derived parameters.

**BigQuery Implementation of Transformation Logic:**

The BigQuery stored procedure `project.dataset.ausd_bp_ta_apn_vertrag_wrapper` will implement this logic as follows:

*   **Parameter Passing**: Directly accepts `p_stichtag` and `p_wiederanlaufWert` as `IN STRING` arguments.
*   **Defaulting Logic**:
    ```sql
    IF p_wiederanlaufWert IS NULL OR TRIM(p_wiederanlaufWert) = '' THEN
      SET v_restart_value = '0';
    ELSE
      SET v_restart_value = p_wiederanlaufWert;
    END IF;

    IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
      SET v_stichtag = v_sysdate; -- v_sysdate derived from FORMAT_DATE('%d%m%Y', CURRENT_DATE())
    ELSE
      SET v_stichtag = p_stichtag;
    END IF;
    ```
*   **Date Generation**: `SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());`
*   **Validation**: `IF v_stichtag IS NULL OR TRIM(v_stichtag) = '' THEN ... SIGNAL SQLSTATE '45000' ... END IF;` for mandatory parameters.
*   **Error Handling**: `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks to catch and log errors, providing a robust error management framework.
*   **Logging**: `INSERT INTO job_registry` and `INSERT INTO job_log` statements will replace the custom `DWMSG_*` functions.
*   **Core Logic Invocation**: `CALL project.dataset.k_ausd_bp_ta_apn_vertrag(...)` will trigger the BigQuery stored procedure that contains the actual data processing.

The detailed transformation logic for data filtering and manipulation (e.g., `Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`) will be implemented within the `project.dataset.k_ausd_bp_ta_apn_vertrag` stored procedure using standard BigQuery SQL.

## 6. External Dependencies

The initial lineage analysis revealed no external system dependencies (e.g., Oracle, SFTP, S3) for this specific wrapper script. However, several internal legacy dependencies will be replaced:

*   **Sourced Environment/Utility Scripts**:
    *   `$HOME/.dw_init`: This will be replaced by environment configurations within Cloud Composer, BigQuery project/dataset settings, or explicit configuration tables if dynamic settings are required.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`: These custom shell utility functions will be replaced by:
        *   Native BigQuery SQL functions (e.g., `FORMAT_DATE`, `CURRENT_DATE()`).
        *   Dedicated BigQuery stored procedures or user-defined functions for complex custom logic.
        *   Direct `INSERT` statements into BigQuery logging tables (`job_registry`, `job_log`).
*   **Downstream Core Processing Script**:
    *   `k_ausd_bp_ta_apn_vertrag.ksh`: This is the most significant internal dependency. It must be migrated entirely to a BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_apn_vertrag`) to achieve an end-to-end BigQuery solution.
*   **Data Sources (DWH)**: The "DWH" where the contract cache resides is implicitly a data source. Assuming the "DWH" is not already BigQuery, the relevant tables will need to be ingested into BigQuery. This could involve:
    *   **Batch Ingestion**: Using Cloud Storage and BigQuery Data Transfer Service, or Cloud Data Fusion/Dataflow for ETL from source DWH.
    *   **Federated Queries**: If the DWH is accessible via external connections (e.g., Cloud SQL, external databases), BigQuery federated queries could be an option (though often less performant than ingestion).
*   **Data Target ("FOS-Tabelle")**: The target table for "Forderungsscoring" will be a BigQuery table (e.g., `project.dataset.fos_contract_cache`). Downstream systems requiring this data will connect directly to BigQuery.

## 7. Unresolved / Risks

*   **Core Script (`k_ausd_bp_ta_apn_vertrag.ksh`) Complexity**: The content of the core script `k_ausd_bp_ta_apn_vertrag.ksh` is crucial and currently unanalyzed in detail.
    *   **Risk**: If this script contains highly complex shell scripting, calls to non-SQL executables, or intricate file system operations, its migration to a BigQuery Stored Procedure might be challenging or require a hybrid approach involving other GCP services (e.g., Cloud Functions, Cloud Run, Dataflow with Python).
    *   **Mitigation**: A dedicated analysis of `k_ausd_bp_ta_apn_vertrag.ksh` is required to determine its migration strategy (BQSP, Python, etc.).
*   **Data Source Definition**: The specific source tables and schemas within the "DWH" for the contract cache are not explicit in the wrapper script.
    *   **Risk**: Incorrect identification of source tables could lead to data integrity issues.
    *   **Mitigation**: Requires analysis of `k_ausd_bp_ta_apn_vertrag.ksh` to identify source tables and their schemas, followed by defining ingestion processes for these tables into BigQuery.
*   **"Forderungsscoring" Target Schema**: The exact schema and data requirements of the "FOS-Tabelle" for "Forderungsscoring" are not fully detailed.
    *   **Risk**: Mismatched schema or data format could break downstream consumers.
    *   **Mitigation**: Collaboration with the "Forderungsscoring" team is necessary to define the target BigQuery table schema and ensure compatibility.
*   **Job Control/Environment Sourcing**: The legacy `DW_EintragsNr` and sourcing of `.dw_init` represent a custom job control and environment management system.
    *   **Risk**: Replicating this exact behavior in a BigQuery-native context can be tricky.
    *   **Mitigation**: The proposed BigQuery logging tables and structured stored procedures provide a BigQuery-native equivalent. Any specific environment variables or configurations previously managed by `.dw_init` will need to be explicitly set in the Cloud Composer environment or passed as parameters.
*   **`semi_auto` Migration Bucket**: The script's `semi_auto` bucket confirms that direct, fully automated conversion is not expected. Manual review and adaptation of the BigQuery SQL pseudocode (especially for the core script) will be required.

## 8. Build Plan

The migration will involve generating the following BigQuery components in a structured order:

1.  **BigQuery DDL for Logging Tables**:
    *   **File**: `job_registry.sql`
    *   **Language**: BigQuery DDL
    *   **Description**: Create the `project.dataset.job_registry` table.
    *   **File**: `job_log.sql`
    *   **Language**: BigQuery DDL
    *   **Description**: Create the `project.dataset.job_log` table.

2.  **BigQuery Stored Procedure - Wrapper Logic**:
    *   **File**: `ausd_bp_ta_apn_vertrag_wrapper.sql`
    *   **Language**: BigQuery SQL
    *   **Description**: Implement the orchestration and parameter handling logic of `r_ausd_bp_ta_apn_vertrag.ksh` as a BigQuery Stored Procedure. This procedure will accept `p_stichtag` and `p_wiederanlaufWert` as arguments and manage job logging and error handling.
    *   **Content**: (As generated by the `shellscript_to_bqsql_design` tool in section 5)

3.  **BigQuery Stored Procedure - Core Logic**:
    *   **File**: `k_ausd_bp_ta_apn_vertrag.sql`
    *   **Language**: BigQuery SQL (to be developed/reverse-engineered from the original ksh script)
    *   **Description**: Implement the core data processing logic of `k_ausd_bp_ta_apn_vertrag.ksh` as a BigQuery Stored Procedure. This will involve the actual SQL queries for data extraction, transformations (filtering, joins), and loading into the target `project.dataset.fos_contract_cache` table.

4.  **BigQuery DDL for Target Data Table**:
    *   **File**: `fos_contract_cache.sql`
    *   **Language**: BigQuery DDL
    *   **Description**: Create the `project.dataset.fos_contract_cache` table, which will serve as the output for "Forderungsscoring." The schema for this table must be derived from the requirements of the downstream system.

5.  **Cloud Composer DAG**:
    *   **File**: `ausd_bp_ta_apn_vertrag_dag.py`
    *   **Language**: Python
    *   **Description**: An Airflow DAG to schedule and trigger the `project.dataset.ausd_bp_ta_apn_vertrag_wrapper` BigQuery Stored Procedure. This DAG will define the schedule, default parameters, and potential dependencies with other processes.

This plan ensures a staged migration, addressing infrastructure (logging tables), then the wrapper logic, followed by the core processing, and finally the orchestration.