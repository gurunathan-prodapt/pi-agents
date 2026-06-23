# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_bp_ta_iccid_einzeln.ksh` to Google Cloud Platform, specifically targeting BigQuery for data processing and storage, and potentially Cloud Composer/Workflows for orchestration.

The primary purpose of this job is to orchestrate the initial provision of selected basic products (e.g., FAX, Data24) for the BERT system. It extracts contract cache data from a Data Warehouse (DWH) based on a specified snapshot date and makes this data available for credit scoring (Forderungsscoring - FOS). The script acts as a wrapper, handling parameter parsing, environment setup, logging, and then delegating the core data extraction and transformation logic to a separate kernel script.

## 2. Source Inventory
The job is composed of a single main KornShell script.

| File Path                                                                   | Technology | Tier          | Automation Bucket | Summary                                                                                                                                                                                                                                   |
| :-------------------------------------------------------------------------- | :--------- | :------------ | :---------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh` | KornShell  | (Not found, assume Medium) | semi_auto         | Orchestrates the initial provision of selected basic products for BERT by extracting contract cache data from DWH based on a snapshot date and making it available for credit scoring. It's a wrapper for a kernel script. |

## 3. Target Architecture
The migrated solution will leverage Google Cloud services for a modern, scalable, and manageable data pipeline.

*   **Orchestration:** Cloud Composer (Apache Airflow) or Cloud Workflows will manage the execution, scheduling, and dependency handling of the migrated components.
*   **Data Processing:** BigQuery stored procedures will encapsulate the core business logic previously contained within the KornShell kernel script.
*   **Data Storage:** BigQuery datasets and tables will serve as the persistent storage for both source data (migrated DWH tables) and target data for the FOS system.
*   **Logging and Monitoring:** Cloud Logging will capture execution logs, and Cloud Monitoring will provide operational visibility and alerting.
*   **Parameter Management:** Parameters will be passed as arguments to BigQuery stored procedures or configured within the orchestration layer.
*   **Audit/Metadata:** A dedicated BigQuery audit table will replace the filesystem-based logging for job status and execution metadata.

**BigQuery Structure:**
*   **Source Tables:** BigQuery tables corresponding to the original DWH tables (e.g., `DWH_VERTRAG_ID` mentioned in the script's usage).
*   **Target Tables:** BigQuery tables for the FOS system.
*   **Audit Table:** `project.dataset.job_audit_log` (as suggested by the tool) to store job execution details, status, and parameters.

## 4. Data Flow & Lineage
The original script's data flow involves:
1.  **Parameter Input:** The script accepts `Stichtag` (processing date) and `Wiederanlaufwert` (restart value) as command-line arguments.
2.  **Environment Setup & Utilities:** It sources common KornShell utility scripts for environment initialization, error handling, parameter parsing, and date manipulation.
3.  **Core Logic Delegation:** The wrapper script invokes a kernel script, `k_ausd_bp_ta_iccid_einzeln.ksh`, passing the parsed parameters. This kernel script is responsible for the actual data extraction from the DWH.
4.  **Data Extraction:** The kernel script is expected to read contract cache data from DWH tables. The script's description mentions `Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag` as selection criteria.
5.  **Data Output:** The extracted and processed data is then made available for credit scoring (FOS), implying a write operation to a target system or table.
6.  **Logging & Status:** The wrapper script manages logging to a file and sets the job status.

**Migrated Data Flow (BigQuery centric):**
1.  **Orchestration Trigger:** A Cloud Composer DAG or Cloud Workflow is triggered, passing parameters like `p_stichtag` and `p_wiederanlaufWert` to a BigQuery stored procedure.
2.  **Wrapper Stored Procedure:** `project.dataset.ausd_bp_ta_iccid_einzeln_wrapper` (corresponding to `r_ausd_bp_ta_iccid_einzeln.ksh`) is called.
    *   This procedure handles parameter validation, default values, and logs the job's start to the `job_audit_log` table.
    *   It then calls the core logic stored procedure.
3.  **Core Logic Stored Procedure:** `project.dataset.k_ausd_bp_ta_iccid_einzeln` (corresponding to `k_ausd_bp_ta_iccid_einzeln.ksh`) executes the actual data extraction and transformation.
    *   This procedure will perform `SELECT` queries against BigQuery tables (migrated DWH data) based on the input `stichtag` and `wiederanlaufWert`.
    *   It will transform and load the data into the target BigQuery table for FOS.
4.  **Logging & Error Handling:** Both wrapper and core logic stored procedures will utilize `INSERT` statements into `project.dataset.job_audit_log` for logging various stages, status, and any errors. BigQuery's `EXCEPTION` handling will manage errors.

## 5. Transformation Logic
The transformation logic resides primarily within the invoked kernel script, `k_ausd_bp_ta_iccid_einzeln.ksh`, which is not directly available in the current scope. However, based on the wrapper script and its description, the transformation will involve:

*   **Parameter Processing:**
    *   `p_stichtag` (processing date, `DDMMYYYY` format): Used as a filter for `Gueltig_von`, `Gueltig_bis`, and `LADEDATUM` fields. If not provided, it defaults to the current system date.
    *   `p_wiederanlaufWert` (restart value): Filters records where `DWH_VERTRAG_ID > p_wiederanlaufWert`.
*   **Data Selection Criteria:**
    *   `Gueltig_von <= Stichtag < Gueltig_bis`
    *   `LADEDATUM < Stichtag`
*   **Output:** The script prepares data for "Forderungsscoring" (credit scoring). The exact schema of the output is not specified but will need to be defined based on the kernel script's functionality.

**Migration Approach for Transformation:**
The `shellscript_to_bqsql_design` tool output provides an excellent foundation for the wrapper script's migration. The core transformation within `k_ausd_bp_ta_iccid_einzeln.ksh` will need to be reverse-engineered and converted into BigQuery SQL.

*   **Wrapper Script (`r_ausd_bp_ta_iccid_einzeln.ksh`) → BigQuery Stored Procedure:**
    *   The parameter parsing and validation logic (`getopts`, `if [[ -z ... ]]`, `pruefeParameterGesetzt`) will be translated to BigQuery SQL using stored procedure parameters, `IFNULL`, `TRIM`, and `SELECT ERROR()` for validation.
    *   Date determination (`DWDate_Gib_Zeitraum`) will use BigQuery's `CURRENT_DATE()`, `FORMAT_DATE()`, and `IFNULL` for defaults.
    *   The execution of the kernel script will be replaced by a `CALL` statement to the corresponding BigQuery stored procedure.
    *   Logging (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`) will be replaced by `INSERT` statements into the `job_audit_log` table.
    *   Error handling (`trap`) will be managed by BigQuery's `BEGIN...EXCEPTION...END` blocks, also updating the audit log.

*   **Kernel Script (`k_ausd_bp_ta_iccid_einzeln.ksh`) → BigQuery Stored Procedure:**
    *   The SQL logic within this script (which performs the actual data extraction and manipulation) will be converted to BigQuery SQL syntax. This will involve mapping source DWH table structures to BigQuery, converting any proprietary SQL functions, and optimizing queries for BigQuery's columnar storage.
    *   The output target (FOS) will be a BigQuery table.

## 6. External Dependencies
The `lineage_assembled_jobs` record indicated `external_systems: []`. However, the script's description and the design tool's analysis suggest interaction with a Data Warehouse (DWH) and output for Credit Scoring (FOS).

*   **Legacy DWH (Data Warehouse):** The script extracts "contract cache data from DWH".
    *   **Replacement:** This DWH will be migrated to BigQuery. The source tables (e.g., those containing `DWH_VERTRAG_ID`, `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`) will become BigQuery tables.
*   **FOS (Forderungsscoring) System:** The script makes data available "for credit scoring".
    *   **Replacement:** The target for this data will be a BigQuery table, from which the downstream FOS system can consume data. If FOS is an external application, an integration mechanism like Cloud Storage exports or BigQuery Data Transfer Service may be used.
*   **Filesystem Utilities:** The script relies on sourcing other `.ksh` files for common functions (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
    *   **Replacement:** These functionalities will be incorporated directly into the BigQuery stored procedures or replaced by native BigQuery functions and patterns (e.g., BigQuery's date functions for `h_alis_date.ksh`, structured logging for `f_alis_msgerr.ksh`).
*   **Logging:** The script uses filesystem-based logging.
    *   **Replacement:** Replaced by BigQuery `job_audit_log` table and Cloud Logging.

## 7. Unresolved / Risks
*   **Kernel Script Logic:** The detailed transformation logic within `k_ausd_bp_ta_iccid_einzeln.ksh` was not part of this analysis. This is the main unresolved component and represents the highest risk. Its content must be thoroughly analyzed and designed for BigQuery.
*   **Database Interactions:** The script mentions DWH and FOS, but no specific database names or tables were resolved through lineage. This needs to be clarified to accurately map source and target tables in BigQuery.
*   **Complex Shell Logic:** While the wrapper script is relatively straightforward, if the kernel script contains complex shell-specific logic (e.g., string manipulations, file processing not easily translatable to SQL), it might require components beyond BigQuery stored procedures, such as Python scripts running in Cloud Functions, Cloud Run, or Dataproc.
*   **Performance:** The original script's performance characteristics are unknown. The BigQuery migration should account for potential performance bottlenecks and leverage BigQuery's optimization capabilities.
*   **Data Volume & Frequency:** The expected data volume and execution frequency are not specified. This influences the choice of orchestration and BigQuery table partitioning/clustering strategies.

## 8. Build Plan
The build plan will consist of the following steps:

1.  **Define BigQuery Schema for Audit Log:**
    *   Create `project.dataset.job_audit_log` table in BigQuery.
2.  **Migrate DWH Source Tables to BigQuery:**
    *   Identify all DWH tables read by `k_ausd_bp_ta_iccid_einzeln.ksh` and migrate them to BigQuery.
3.  **Define BigQuery Schema for FOS Target Tables:**
    *   Identify and create target tables in BigQuery for the credit scoring (FOS) output.
4.  **Develop `k_ausd_bp_ta_iccid_einzeln` BigQuery Stored Procedure:**
    *   **Language:** BigQuery SQL (for transformations and data manipulation).
    *   Reverse-engineer the logic from the original `k_ausd_bp_ta_iccid_einzeln.ksh`.
    *   Implement the data extraction, filtering (`Stichtag`, `Wiederanlaufwert`), and loading into the FOS target table.
    *   Include error handling and logging to `job_audit_log`.
5.  **Develop `ausd_bp_ta_iccid_einzeln_wrapper` BigQuery Stored Procedure:**
    *   **Language:** BigQuery SQL.
    *   Implement parameter parsing and validation.
    *   Integrate date determination.
    *   Include calls to the `k_ausd_bp_ta_iccid_einzeln` procedure.
    *   Implement comprehensive logging and error handling to `job_audit_log`.
6.  **Develop Orchestration (Cloud Composer/Workflows):**
    *   **Language:** Python (for Cloud Composer DAG) or YAML/JSON (for Cloud Workflows).
    *   Create a DAG/workflow to schedule the execution of the `ausd_bp_ta_iccid_einzeln_wrapper` BigQuery stored procedure.
    *   Define parameters and their default values within the DAG/workflow configuration.
7.  **Unit and Integration Testing:**
    *   Thoroughly test each BigQuery stored procedure.
    *   Test the end-to-end flow via the orchestration layer.
8.  **Deployment:**
    *   Deploy BigQuery schemas and stored procedures.
    *   Deploy the Cloud Composer DAG or Cloud Workflow.