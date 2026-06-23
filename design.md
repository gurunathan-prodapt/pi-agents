# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh

## 1. Purpose & Scope
This document outlines the migration design for the `r_ausd_v_ta_apn_ve.ksh` job, which is a KornShell framework script responsible for the reconciliation of contract data within the `ta_apn_ve` table. Its primary purpose is orchestration: it handles parameter parsing, environment initialization, logging, error trapping, and then invokes a core script, `k_ausd_v_ta_apn_ve.ksh`, which contains the actual data processing logic. The scope of this migration focuses on replatforming this orchestration layer to Google Cloud Platform (GCP), specifically using BigQuery Stored Procedures for the script's logic and potentially Cloud Composer for scheduling, and BigQuery for data storage.

## 2. Source Inventory
The job is primarily composed of one KornShell script that acts as an orchestrator.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh`
    *   **Technology:** KornShell (shell)
    *   **Purpose:** Pipeline Orchestrator
    *   **Complexity Tier:** Medium
    *   **Automation Bucket:** Semi-automatic (B2)
    *   **Description:** This script initializes the execution environment, parses command-line parameters, sets up logging and error handling, and orchestrates the execution of a core reconciliation script. It handles `INT` (interrupt) and `ERR` (shell error) traps.
    *   **Key Calls/Dependencies:**
        *   `INVOKES` `k_ausd_v_ta_apn_ve.ksh` (another KornShell script)
        *   `SOURCES` several common utility scripts (e.g., `. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`)
        *   Uses `DWMSG_` functions for logging and error reporting (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`).

*   **Implicitly Invoked Core Script (via `r_ausd_v_ta_apn_ve.ksh`):** `k_ausd_v_ta_apn_ve.ksh`
    *   **Technology:** KornShell
    *   **Purpose:** Appears to contain the core business logic.
    *   **Key Calls/Dependencies:**
        *   `EXECUTES_SQL` `D_AUSD_V_TA_APN_VE.SQL`

*   **Implicitly Executed SQL (via `k_ausd_v_ta_apn_ve.ksh`):** `D_AUSD_V_TA_APN_VE.SQL`
    *   **Technology:** SQL
    *   **Purpose:** Performs the actual data reconciliation.
    *   **Key Data Interactions:**
        *   `READS_TABLE` from `DWTK_MELDUNGEN`
        *   `READS_TABLE` from `PDS$TA_PDP_CONTEXT_ASSOC`
        *   `WRITES_TABLE` to `SOF$TA_APN_VE` (which is identified as `ta_apn_ve`)
        *   `WRITES_TABLE` to `VIA`
        *   `USES_PACKAGE` `DWPA_UTIL_SKRIPT`
        *   `USES_PACKAGE` `PA_ANALYZE`

## 3. Target Architecture
The target architecture leverages Google BigQuery for data storage and transformation logic, and potentially Cloud Composer (managed Apache Airflow) for job orchestration and scheduling.

*   **Orchestration Layer:**
    *   The `r_ausd_v_ta_apn_ve.ksh` wrapper logic will be migrated to a **BigQuery Stored Procedure**.
    *   If complex scheduling or external dependencies (like the original UC4 job scheduler) are required, a **Cloud Composer (Airflow) DAG** will be implemented to invoke this BigQuery Stored Procedure.

*   **Transformation Layer:**
    *   The core business logic from `k_ausd_v_ta_apn_ve.ksh` and `D_AUSD_V_TA_APN_VE.SQL` will be replatformed to **BigQuery SQL**. This will likely involve creating one or more BigQuery Stored Procedures for the `k_ausd_v_ta_apn_ve.ksh` equivalent and directly migrating the SQL logic from `D_AUSD_V_TA_APN_VE.SQL` into these procedures or as separate BigQuery queries.

*   **Data Storage:**
    *   Source tables `DWTK_MELDUNGEN`, `PDS$TA_PDP_CONTEXT_ASSOC` will be migrated to **BigQuery tables**.
    *   Target tables `SOF$TA_APN_VE` and `VIA` will be created/maintained as **BigQuery tables**.

*   **Logging & Monitoring:**
    *   The `DWMSG_` logging and error handling functionality will be replaced by:
        *   Inserts into a dedicated **BigQuery audit/job log table**.
        *   Leveraging **Cloud Logging** for procedure execution logs.
        *   **Cloud Monitoring** for alerts based on error conditions.

## 4. Data Flow & Lineage
The migrated job will follow this data flow and lineage:

1.  **External Scheduler (Legacy: UC4):** The original UC4 job scheduler invoked `r_ausd_v_ta_apn_ve.ksh`. In the target state, this role can be taken by **Cloud Composer (Airflow)** or **Cloud Scheduler** triggering the BigQuery Stored Procedure.
2.  **Orchestration BigQuery Stored Procedure:** The `r_ausd_v_ta_apn_ve` BigQuery Stored Procedure will execute.
    *   It will parse input parameters (similar to `getopts`).
    *   It will record job start, assign a job entry number, and set status in a **BigQuery audit table**.
    *   It will initiate logging to **Cloud Logging**.
    *   It will `CALL` the core reconciliation BigQuery Stored Procedure (equivalent to `k_ausd_v_ta_apn_ve.ksh`).
    *   Upon completion, it will update the job status in the **BigQuery audit table** to 'OK' or 'ERROR' based on the core procedure's outcome.
3.  **Core Reconciliation BigQuery Stored Procedure:** This procedure (equivalent to `k_ausd_v_ta_apn_ve.ksh` and `D_AUSD_V_TA_APN_VE.SQL`) will:
    *   Read data from **BigQuery tables**: `DWTK_MELDUNGEN` and `PDS$TA_PDP_CONTEXT_ASSOC`.
    *   Perform reconciliation and transformation logic.
    *   Write results to **BigQuery tables**: `SOF$TA_APN_VE` and `VIA`.
    *   Handle exceptions using `EXCEPTION WHEN ERROR THEN` blocks.

## 5. Transformation Logic
The transformation logic will be divided into two main parts:

1.  **Orchestration Logic (from `r_ausd_v_ta_apn_ve.ksh`):**
    *   **Parameter Parsing:** The `getopts` logic will be replaced by input parameters of the BigQuery Stored Procedure.
    *   **Environment Initialization:** Variables like `ProgName`, `ProgVersion`, `JobKennung`, `v_sysdate` will become `DECLARE` variables within the stored procedure. Sourcing of utility scripts (`. $HOME/.dw_init`, etc.) will be replaced by direct SQL logic or by ensuring equivalent functionalities (e.g., date formatting) are handled natively in BigQuery SQL.
    *   **Logging & Auditing:** The `DWMSG_` functions will be translated into `INSERT` and `UPDATE` statements against a new `job_log` BigQuery table. Standard output (`print`) will be captured by Cloud Logging.
    *   **Error Handling:** The `set -eu` and `trap` mechanisms will be replicated using BigQuery Stored Procedure `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks. Errors will be logged to the `job_log` table and `RAISE`d to indicate failure.
    *   **Core Script Invocation:** The execution of `k_ausd_v_ta_apn_ve.ksh` will be replaced by a `CALL` statement to its corresponding BigQuery Stored Procedure.

2.  **Core Reconciliation Logic (from `k_ausd_v_ta_apn_ve.ksh` and `D_AUSD_V_TA_APN_VE.SQL`):**
    *   The SQL logic within `D_AUSD_V_TA_APN_VE.SQL` (which includes `SELECT` from `DWTK_MELDUNGEN` and `PDS$TA_PDP_CONTEXT_ASSOC`, and `INSERT`/`MERGE` into `SOF$TA_APN_VE` and `VIA`) will be directly migrated to BigQuery SQL. This may involve minor syntax adjustments for BigQuery compatibility.
    *   The `USES_PACKAGE` `DWPA_UTIL_SKRIPT` and `PA_ANALYZE` indicate calls to stored procedures or functions in the legacy database. These will need to be re-implemented as BigQuery Stored Procedures or User-Defined Functions (UDFs), or their logic embedded directly if simple.

## 6. External Dependencies
*   **Legacy UC4 Job Scheduler:** This is an external system that invoked `r_ausd_v_ta_apn_ve.ksh`. In GCP, this will be replaced by **Cloud Composer (Airflow)** for complex workflows, or **Cloud Scheduler** for simpler cron-like schedules, triggering the BigQuery Stored Procedure.
*   **Legacy Shell Utility Scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These provide environment setup, error messaging, parameter handling, and date utilities. Their functionality will be integrated directly into the BigQuery Stored Procedure or replicated using native BigQuery SQL features.
*   **`DWMSG_` Functions:** These are custom logging and error reporting functions. They will be replaced by `INSERT`/`UPDATE` operations on a BigQuery audit log table and standard BigQuery error handling.
*   **Database Packages (`DWPA_UTIL_SKRIPT`, `PA_ANALYZE`):** These are used by the underlying SQL script. They will need to be migrated to BigQuery Stored Procedures or UDFs, or their logic re-implemented directly in the BigQuery SQL.

## 7. Unresolved / Risks
*   **Core Business Logic Details:** The detailed transformation logic within `k_ausd_v_ta_apn_ve.ksh` and `D_AUSD_V_TA_APN_VE.SQL` was not fully analyzed as part of this specific job's lineage run. A separate detailed analysis and migration effort will be required for these components to ensure accurate replication in BigQuery SQL.
*   **`DWMSG_` Function Implementation:** The exact implementation details of all `DWMSG_` functions are not fully known from the provided information. Their precise behavior needs to be mapped to BigQuery logging and auditing mechanisms.
*   **`DWPA_UTIL_SKRIPT` and `PA_ANALYZE` Packages:** The logic within these legacy database packages needs to be understood and recreated in BigQuery.
*   **External System Identity/Credentials:** The `LOGIN:DW.UNIX.ISBERT` and `HOST:DWHDWH1P/DWHDWH2P` references in the broader lineage suggest potential external database or system interactions managed by UC4. These will need to be re-evaluated for access methods and security in GCP, likely using service accounts and appropriate IAM roles.

## 8. Build Plan
The migration build plan will proceed in the following order:

1.  **Define BigQuery Audit Log Table DDL:** Create the `job_log` table in BigQuery to store job execution details, status, and errors.
2.  **Migrate Core SQL Logic:**
    *   Analyze `D_AUSD_V_TA_APN_VE.SQL` and any related `DWPA_UTIL_SKRIPT` and `PA_ANALYZE` logic.
    *   Create BigQuery Stored Procedures or UDFs for the `DWPA_UTIL_SKRIPT` and `PA_ANALYZE` functionalities.
    *   Convert `D_AUSD_V_TA_APN_VE.SQL` into one or more BigQuery SQL scripts or stored procedures.
    *   Language: BigQuery SQL.
3.  **Migrate `k_ausd_v_ta_apn_ve.ksh` Logic:**
    *   Create a BigQuery Stored Procedure that encapsulates the calls to the migrated `D_AUSD_V_TA_APN_VE.SQL` logic, handling any intermediate shell-based logic.
    *   Language: BigQuery SQL.
4.  **Migrate Orchestration Script `r_ausd_v_ta_apn_ve.ksh`:**
    *   Create a BigQuery Stored Procedure that implements the wrapper logic, including parameter handling, environment setup, and the new logging/error handling via the `job_log` table.
    *   This procedure will `CALL` the BigQuery Stored Procedure created for `k_ausd_v_ta_apn_ve.ksh`.
    *   Language: BigQuery SQL.
5.  **Develop Orchestration Layer (if needed):**
    *   If the job requires complex scheduling or integration with other GCP services, create an Apache Airflow DAG in Cloud Composer to trigger the main `r_ausd_v_ta_apn_ve` BigQuery Stored Procedure.
    *   Language: Python (for Airflow DAG).
6.  **IAM and Access Control:** Configure BigQuery IAM roles and service accounts to grant necessary permissions for procedure execution and data access.