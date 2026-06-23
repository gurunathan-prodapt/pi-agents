# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh

## 1. Purpose & Scope

This job, `r_ausd_bp_ta_msisdn_his.ksh`, is an orchestration script responsible for the initial provisioning of selected basic products (e.g., FAX, Data24) for the BERT system. Its primary business purpose is to generate a snapshot of the contract cache within the Data Warehouse (DWH) and make this data available for "Forderungsscoring" (demand scoring).

The script acts as a wrapper, handling command-line parameter parsing (like cutoff dates and restart values), initializing the execution environment by sourcing common utility scripts, setting up job-specific logging, and managing error handling. Its core function is to invoke a downstream KornShell script, `k_ausd_bp_ta_msisdn_his.ksh`, which is expected to contain the actual data extraction and provisioning logic.

The scope of this migration design document covers the transformation of this KornShell wrapper script and the conceptual approach for migrating its invoked core script to the BigQuery platform.

## 2. Source Inventory

The primary source file for this job is:

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh`**
    *   **Technology:** KornShell Script (ksh)
    *   **Role:** Job orchestration, parameter parsing, logging, and invocation of core processing logic.
    *   **Complexity Tier:** Not explicitly classified in `file_complexity`, but assessed as "Medium" due to its orchestration role, parameter handling, and reliance on external helper scripts.
    *   **Automation Bucket:** Semi-automatic (B2)
    *   **Purpose Note:** "Job assembled from 1 component(s); stage dist: medium=1"

**Dependent Source Files (sourced or invoked by `r_ausd_bp_ta_msisdn_his.ksh`):**

*   `$HOME/.dw_init` (Environment initialization)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error messaging utility)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter parsing utility)
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date handling utility)
*   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_msisdn_his.ksh` (Core processing script - *content unknown, critical for migration*)

## 3. Target Architecture

The target architecture for this job will leverage Google Cloud Platform (GCP) services, primarily BigQuery for data storage and transformation, and Cloud Composer (Airflow) for orchestration.

*   **BigQuery:**
    *   **Staging Area:** Temporary tables for intermediate results, if required by the core logic.
    *   **Target Tables:** Final destination tables for the "Forderungsscoring" system.
    *   **Audit Tables:** Dedicated BigQuery tables (e.g., `job_audit`, `job_error_log`) to replace the file-based logging mechanism and provide centralized job metadata and error tracking.
    *   **Stored Procedures:**
        *   **`ausd_bp_ta_msisdn_his_wrapper_sp`:** A BigQuery Stored Procedure to encapsulate the wrapper logic from `r_ausd_bp_ta_msisdn_his.ksh` (parameter handling, job auditing, and invocation of the core logic).
        *   **`ausd_bp_ta_msisdn_his_core_sp`:** A BigQuery Stored Procedure to encapsulate the data extraction and transformation logic currently residing in `k_ausd_bp_ta_msisdn_his.ksh`.
*   **Cloud Composer (Airflow):**
    *   **DAG (`r_ausd_bp_ta_msisdn_his_dag`):** An Airflow DAG will be used to schedule and orchestrate the execution of the `ausd_bp_ta_msisdn_his_wrapper_sp`. This DAG will handle passing parameters, monitoring job status, and potentially integrating with other GCP services or external systems.
*   **Cloud Logging & Monitoring:** All BigQuery and Airflow activities will be automatically logged to Cloud Logging, enabling centralized monitoring and alerting.

## 4. Data Flow & Lineage

The migrated job will follow this conceptual data flow and lineage:

1.  **Airflow DAG Trigger:** The `r_ausd_bp_ta_msisdn_his_dag` in Cloud Composer is triggered, either on a schedule or via an external event.
2.  **Wrapper Stored Procedure Invocation:** The Airflow DAG invokes the `project.dataset.ausd_bp_ta_msisdn_his_wrapper_sp` in BigQuery. It passes input parameters like `p_stichtag` (reference date) and `p_wiederanlaufWert` (restart value), typically derived from Airflow's execution context or job configuration.
3.  **Wrapper Logic (BigQuery SP):**
    *   The `ausd_bp_ta_msisdn_his_wrapper_sp` processes and validates the input parameters.
    *   It generates a job entry in the `project.dataset.job_audit` table, capturing job metadata.
    *   It then invokes the `project.dataset.ausd_bp_ta_msisdn_his_core_sp`, passing all necessary parameters.
    *   Upon successful completion of the core SP, it updates the `job_audit` table with a success status.
    *   Error handling within the wrapper SP will log to `project.dataset.job_error_log` and raise an exception, which Airflow will capture.
4.  **Core Logic (BigQuery SP):** The `ausd_bp_ta_msisdn_his_core_sp` (equivalent to `k_ausd_bp_ta_msisdn_his.ksh`) will perform the actual data processing:
    *   **Reads from DWH Contract Cache:** It will query source tables in BigQuery representing the legacy DWH contract cache. The exact tables and columns (e.g., `DWH_VERTRAG_ID`, `LADEDATUM`, `Gueltig_von`, `Gueltig_bis`) will be determined upon analysis of `k_ausd_bp_ta_msisdn_his.ksh`. The `p_wiederanlaufWert` will be used to filter `DWH_VERTRAG_ID` to support restartability.
    *   **Transformation:** Apply any necessary filtering and transformations based on the `p_stichtag` (e.g., `Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`).
    *   **Writes to Target:** Inserts the processed data into the designated BigQuery target tables for "Forderungsscoring".

## 5. Transformation Logic

**a. `r_ausd_bp_ta_msisdn_his.ksh` (Wrapper Script) to `ausd_bp_ta_msisdn_his_wrapper_sp` (BigQuery Stored Procedure)**

*   **Parameter Handling:**
    *   Legacy: `getopts` for `-s Stichtag` (DDMMYYYY) and `-l Wiederanlaufwert`.
    *   Target: `ausd_bp_ta_msisdn_his_wrapper_sp` will accept `p_stichtag STRING` and `p_wiederanlaufWert INT64` as input parameters.
*   **Defaulting `Wiederanlaufwert`:**
    *   Legacy: `if [[ -z "$p_wiederanlaufWert" ]] then p_wiederanlaufWert=0 fi`.
    *   Target: Handled via `IF p_wiederanlaufWert IS NULL THEN SET p_wiederanlaufWert = 0; END IF;` within the SP.
*   **`Stichtag` Determination:**
    *   Legacy: Defaults to `v_sysdate` if `-s` is not provided. Original comment suggested `MIN(sysdate, maxladedatum)` which was not implemented.
    *   Target: Defaults to `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` if `p_stichtag` is `NULL` or empty.
*   **Utility Script Replacement:**
    *   `. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`: Their functionalities will be replaced by native BigQuery SQL functions, BigQuery audit tables for logging, and Airflow's parameter passing and scheduling capabilities.
    *   `DWDate_Gib_Zeitraum`: Replaced by `CURRENT_DATE()` and `FORMAT_DATE()` functions.
    *   `pruefeParameterGesetzt`: Replaced by `IF ... RAISE USING MESSAGE` constructs.
*   **Logging and Error Handling:**
    *   Legacy: `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`, `DWMSG_Fehlerbehandlung`, `trap` statements, output to `$LogDatei`.
    *   Target: Insertions into `project.dataset.job_audit` and `project.dataset.job_error_log` tables. BigQuery's exception handling (`EXCEPTION WHEN ERROR THEN`) will manage errors, and Airflow will capture SP execution status and logs.
*   **Invocation of Core Logic:**
    *   Legacy: `${Name_Kernskript} -j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert}`
    *   Target: `CALL project.dataset.ausd_bp_ta_msisdn_his_core_sp(...)` within the wrapper SP.

**b. `k_ausd_bp_ta_msisdn_his.ksh` (Core Script) to `ausd_bp_ta_msisdn_his_core_sp` (BigQuery Stored Procedure)**

*   **Detailed analysis of `k_ausd_bp_ta_msisdn_his.ksh` is required to define precise transformation logic.**
*   **Assumed Logic (based on wrapper comments):**
    *   Reads from DWH contract cache tables (e.g., `DWH$TA_C_VERTRAG`).
    *   Applies date-based filtering: `Gueltig_von <= p_stichtag < Gueltig_bis AND LADEDATUM < p_stichtag`.
    *   Applies restart logic filter: `DWH_VERTRAG_ID > p_wiederanlaufWert`.
    *   Writes results to the "FOS-Tabelle" (Forderungsscoring table).
*   **Target Logic:** These SQL operations will be translated directly into BigQuery SQL statements within the `ausd_bp_ta_msisdn_his_core_sp`.

## 6. External Dependencies

The initial analysis did not identify explicit external systems in the `lineage_assembled_jobs` table.

*   **Sourced Shell Scripts:**
    *   `. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`: These are internal shell scripts that provide common functionalities. Their logic will be re-implemented using BigQuery SQL functions, stored procedures, and Airflow's Python capabilities. No direct external system replacement is needed; their functions are absorbed into the GCP ecosystem.
*   **"Forderungsscoring" Target:**
    *   This is the conceptual target for the processed data. The exact nature of this target (e.g., another database, flat files, API endpoint) is unknown.
    *   **Replacement Strategy:** Assuming "Forderungsscoring" will consume data from BigQuery, the target will be a BigQuery table(s) that can be accessed by the downstream system. If "Forderungsscoring" requires data in a specific file format (e.g., CSV, JSON), an Airflow task can be added to export the BigQuery table data to Cloud Storage, potentially triggering a Cloud Function for further processing or transfer.

## 7. Unresolved / Risks

*   **Core Script (`k_ausd_bp_ta_msisdn_his.ksh`) Logic:** The most significant unresolved item is the exact data extraction, transformation, and loading logic contained within `k_ausd_bp_ta_msisdn_his.ksh`. Without this, the detailed BigQuery `ausd_bp_ta_msisdn_his_core_sp` cannot be fully designed and implemented.
*   **"Forderungsscoring" Integration:** The precise method by which the "Forderungsscoring" system consumes the output data is unclear. This needs to be determined to ensure seamless integration post-migration.
*   **Historical `MIN(sysdate,maxladedatum)` Logic:** The wrapper script comments indicate an intention to use `MIN(sysdate,maxladedatum)` for `Stichtag` determination, but the implemented code always uses `sysdate`. It needs to be clarified if the `maxladedatum` logic is truly abandoned or if `k_ausd_bp_ta_msisdn_his.ksh` handles this. If the `maxladedatum` logic is critical, it must be implemented in the BigQuery migration.
*   **Error Code Mapping:** The legacy script uses specific error codes (e.g., 192, 193). A mapping strategy for these to BigQuery/Airflow error handling mechanisms should be established if granular error differentiation is required.
*   **`BERT_DIR_ROOT` Variable:** This environment variable is heavily used. Its value and contents need to be mapped to appropriate GCP project/dataset paths or configuration values.

## 8. Build Plan

1.  **Analyze `k_ausd_bp_ta_msisdn_his.ksh` (Manual Step):**
    *   **Action:** Obtain the content of `k_ausd_bp_ta_msisdn_his.ksh` and perform a detailed static analysis to identify source tables, transformation logic, and target tables.
    *   **Output:** Detailed specification of data sources, transformation rules, and target schema.
    *   **Language:** Manual analysis, documentation.

2.  **Define BigQuery Audit Tables (DDL):**
    *   **Action:** Create DDL for `project.dataset.job_audit` and `project.dataset.job_error_log` tables in BigQuery.
    *   **Output:** `job_audit_ddl.sql`, `job_error_log_ddl.sql`
    *   **Language:** BigQuery SQL

3.  **Develop `ausd_bp_ta_msisdn_his_core_sp` (BigQuery Stored Procedure):**
    *   **Action:** Implement the core data processing logic based on the analysis from Step 1.
    *   **Output:** `ausd_bp_ta_msisdn_his_core_sp.sql`
    *   **Language:** BigQuery SQL

4.  **Develop `ausd_bp_ta_msisdn_his_wrapper_sp` (BigQuery Stored Procedure):**
    *   **Action:** Implement the wrapper logic including parameter handling, audit logging, and invocation of `ausd_bp_ta_msisdn_his_core_sp`.
    *   **Output:** `ausd_bp_ta_msisdn_his_wrapper_sp.sql`
    *   **Language:** BigQuery SQL

5.  **Develop Airflow DAG (`r_ausd_bp_ta_msisdn_his_dag`):**
    *   **Action:** Create an Airflow DAG to schedule and execute `ausd_bp_ta_msisdn_his_wrapper_sp`.
    *   **Output:** `r_ausd_bp_ta_msisdn_his_dag.py`
    *   **Language:** Python (Airflow)

6.  **Target System Integration Plan:**
    *   **Action:** Based on the "Forderungsscoring" system's requirements, define the integration method (e.g., direct BigQuery access, GCS export, Pub/Sub notification).
    *   **Output:** Integration specification, potentially additional Airflow tasks or Cloud Functions.
    *   **Language:** Documentation, Python (Airflow/Cloud Functions) if needed.