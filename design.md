# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_bp_ta_cntrct_dist.ksh`, located at `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh`.
The job, named "Bereitstellung Basisprodukte BERT" (Provisioning Base Products BERT), is responsible for the initial provisioning of selected base products (e.g., FAX, Data24) for the BERT system. Its primary function is to create a cutoff-date extraction of contract cache data from the Data Warehouse (DWH) and make it available to the Forderungsscoring (FOS) system. The script acts as a wrapper, delegating the core business logic to a kernel script.
The migration aims to re-implement this functionality on the Google Cloud Platform, specifically utilizing BigQuery for data processing and storage, and potentially Cloud Composer/Workflows for orchestration.

## 2. Source Inventory
The primary source file for this job is:
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh`
  - **Technology:** KornShell Script
  - **Complexity Tier:** Not available from analysis database.
  - **Automation Bucket:** Not available from analysis database.
  - **Purpose:** ETL orchestrator. This script handles parameter parsing, date determination, error handling, logging, and invokes a "kernel" script for the main data processing.

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services:
- **Core Logic:** BigQuery Stored Procedures will implement the parameter handling, defaulting logic, validation, and orchestration of the primary data processing steps.
- **Logging & Monitoring:** Dedicated BigQuery tables (`job_control`, `job_log`, `job_error_log`, `job_message_log`) will replace the script's custom `DWMSG_*` logging framework. Cloud Logging and Cloud Monitoring will provide platform-level visibility.
- **Orchestration:** Cloud Composer (Apache Airflow) or Cloud Workflows will be used to schedule and orchestrate the BigQuery stored procedures, managing job execution flow and dependencies.
- **Data Storage:** All source and target data will reside in BigQuery tables.

## 4. Data Flow & Lineage
The original script (`r_ausd_bp_ta_cntrct_dist.ksh`) orchestrates the following flow:
1.  **Initialization:** Sources environment variables from `$HOME/.dw_init` and helper scripts for error handling, parameter parsing, and date functions.
2.  **Parameter Processing:** Parses command-line arguments `-s` (Stichtag/cutoff date) and `-l` (Wiederanlaufwert/restart value). Defaults the cutoff date to the system date if not provided and the restart value to 0 if not set.
3.  **Validation:** Ensures the `Stichtag` parameter is present.
4.  **Logging Setup:** Initializes job logging parameters using `DWMSG_*` functions and sets up error traps.
5.  **Kernel Script Invocation:** Invokes the core processing script, `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh`, passing validated parameters.
6.  **Status Reporting:** Logs the completion status via `DWMSG_SetzeStatusOK` upon successful execution.

**Migration Flow:**
The BigQuery stored procedure (`ausd_bp_ta_cntrct_dist_wrapper`) will act as the orchestrator.
- Input parameters will be passed directly to the stored procedure.
- Date calculation will use BigQuery's `CURRENT_DATE()` and formatting functions.
- Parameter validation will use BigQuery SQL `IF` statements and potentially `SIGNAL SQLSTATE` for error reporting.
- Logging will involve `INSERT` statements into dedicated BigQuery log tables.
- The invocation of the kernel script will be replaced by a `CALL` to a separate BigQuery stored procedure (`ausd_bp_ta_cntrct_dist_kernel`) that encapsulates the migrated logic of `k_ausd_bp_ta_cntrct_dist.ksh`.

The lineage for this specific job, as per the `lineage_edges` table, did not explicitly list dependencies for `r_ausd_bp_ta_cntrct_dist.ksh`. Therefore, the understanding of the data flow and dependencies is derived from static analysis of the script content.

## 5. Transformation Logic
The transformation logic within the wrapper script primarily involves:
-   **Parameter Handling:**
    -   Accepts `p_stichtag` (cutoff date in DDMMYYYY format) and `p_wiederanlaufWert` (restart value).
    -   `p_wiederanlaufWert` defaults to 0 if not provided. This will be handled by `IFNULL(p_wiederanlaufWert, 0)` in BQSQL.
    -   `p_stichtag` defaults to `v_sysdate` (current system date) if not provided. This will be handled by `IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate)` in BQSQL.
-   **Date Determination:** `DWDate_Gib_Zeitraum` is used to get the system date. In BigQuery, this will be replaced by `CURRENT_DATE()` and `FORMAT_DATE()` functions.
-   **Validation:** `pruefeParameterGesetzt` checks if `Stichtag` is set. In BigQuery, this will be an `IF` condition checking `v_stichtag IS NULL OR TRIM(v_stichtag) = ''`.
-   **Orchestration:** The script invokes `k_ausd_bp_ta_cntrct_dist.ksh` with parameters. This invocation will be replaced by a `CALL` statement to the migrated kernel logic within a BigQuery stored procedure.

The detailed transformation logic of the kernel script (`k_ausd_bp_ta_cntrct_dist.ksh`) is not part of this design, as its content was not provided or analyzed. Its migration will require a separate design document. However, based on the wrapper's description, the kernel script will likely involve:
-   Selecting records based on date range: `Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`.
-   Filtering by `DWH_VERTRAG_ID > Wiederanlaufwert` for restart scenarios.
-   Potentially deleting existing entries based on `Wiederanlaufwert`.
-   Providing contract cache data to Forderungsscoring.

## 6. External Dependencies
**Original Script Dependencies:**
-   **Local Environment Files:**
    -   `$HOME/.dw_init`: Sources environment variables.
    -   Helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`): Provide common functions for error handling, parameter parsing, and date manipulation.
-   **Kernel Script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh`: Contains the main business logic.
-   **System Commands:** `getopts`, `trap`, `print`, `tee`.

**Replacement in BigQuery:**
-   **Environment Files & Helper Scripts:** These will be replaced by BigQuery's native capabilities, such as stored procedure parameters, constant declarations, BigQuery functions, and potentially configuration tables for metadata.
-   **Kernel Script:** The logic of `k_ausd_bp_ta_cntrct_dist.ksh` must be migrated into a separate BigQuery stored procedure (`ausd_bp_ta_cntrct_dist_kernel`). If the kernel script involves file system operations or interactions with external systems not supported by BigQuery, these parts will need to be re-implemented using Cloud Functions, Cloud Run, or other appropriate GCP services orchestrated by Cloud Composer/Workflows.
-   **System Commands:** BigQuery SQL equivalents will be used (e.g., `IF`, `CASE` for conditionals; `CURRENT_DATE()`, `FORMAT_DATE()` for date functions; `INSERT` for logging). Shell traps will be replaced by BigQuery's `EXCEPTION WHEN ERROR THEN` blocks.
-   **External Systems (from lineage_assembled_jobs):** No explicit external systems were identified for this job in the `lineage_assembled_jobs` table. Any external systems accessed by the kernel script will need to be identified during its migration.

## 7. Unresolved / Risks
-   **Kernel Script Logic:** The complete business logic resides in `k_ausd_bp_ta_cntrct_dist.ksh`, whose content is currently unknown. This is the biggest unresolved item. Its migration will dictate the complexity and details of the `ausd_bp_ta_cntrct_dist_kernel` BigQuery stored procedure. Without it, the exact data sources (tables being read) and data targets (tables being written to) cannot be fully determined.
-   **Missing Complexity/Automation Rate:** `file_complexity` and `automation_rate` tables did not return data for this file. This suggests that the complexity and migration effort estimations are based purely on manual analysis, and the automation potential for this specific script is unquantified.
-   **Custom Logging Framework (`DWMSG_*`):** While a replacement BigQuery logging solution is proposed, ensuring full functional parity and historical log migration might require additional effort.
-   **Performance Optimization:** The `AL??` comments in the original script regarding `FOSHoleLadedatum` suggest potential performance considerations or alternative data sources that need to be understood during the migration of the kernel logic.

## 8. Build Plan
The migration will proceed in the following steps:

1.  **DDL Generation for Logging Tables (BigQuery DDL):**
    -   `project.dataset.job_control` (to track job status and metadata)
    -   `project.dataset.job_log` (for detailed job execution logs)
    -   `project.dataset.job_error_log` (for capturing error details)
    -   `project.dataset.job_message_log` (for general job messages)

2.  **`ausd_bp_ta_cntrct_dist_wrapper` Stored Procedure (BigQuery SQL):**
    -   Implement parameter parsing and defaulting logic.
    -   Integrate BigQuery date functions for `v_sysdate`.
    -   Implement parameter validation logic.
    -   Replace `DWMSG_*` functions with `INSERT` statements into the logging tables.
    -   Implement error handling using `BEGIN ... EXCEPTION WHEN ERROR THEN ... END`.
    -   Include a `CALL` statement to the future `ausd_bp_ta_cntrct_dist_kernel` stored procedure.

3.  **`k_ausd_bp_ta_cntrct_dist.ksh` Analysis and `ausd_bp_ta_cntrct_dist_kernel` Design:**
    -   **Crucial Step:** Analyze the source code of `k_ausd_bp_ta_cntrct_dist.ksh`.
    -   Identify all data sources (tables read), transformation logic, and data targets (tables written).
    -   Design the `ausd_bp_ta_cntrct_dist_kernel` as one or more BigQuery stored procedures, potentially with BigQuery views or functions, or if needed, a Python/PySpark job for more complex transformations, executed on Dataproc or Cloud Run.

4.  **`ausd_bp_ta_cntrct_dist_kernel` Stored Procedure / Job Implementation (BigQuery SQL / Python):**
    -   Implement the core business logic of the kernel script in BigQuery SQL.

5.  **Orchestration (Cloud Composer/Workflows):**
    -   Create a Cloud Composer DAG or Cloud Workflow definition to schedule and execute the `ausd_bp_ta_cntrct_dist_wrapper` stored procedure. This will manage parameters, retries, and overall workflow.

6.  **Testing:**
    -   Unit tests for individual BigQuery stored procedures.
    -   Integration tests for the entire workflow, including parameter passing and logging.
    -   Data validation to ensure migrated process produces identical results to the legacy system.

This build plan is iterative, with step 3 being a critical dependency for the detailed implementation of the core business logic.