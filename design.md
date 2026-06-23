# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh

## 1. Purpose & Scope
This KornShell script (`r_ausd_v_ta_cntrct_crs2.ksh`) serves as a wrapper for a data reconciliation process primarily targeting the `ta_cntrct_crs2` table. Its main function is to manage the execution environment, parse input parameters, initialize logging and error handling, and orchestrate the execution of a core processing script named `k_ausd_v_ta_cntrct_crs2.ksh`. The script itself does not contain business transformation logic but provides the operational framework for the reconciliation job. The overall job's purpose, as indicated by `purpose_note`, is "Job assembled from 1 component(s); stage dist: medium=1".

## 2. Source Inventory
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs2.ksh`
    *   **Technology:** KornShell (ksh)
    *   **Summary:** KornShell script serving as a wrapper for data reconciliation of the `ta_cntrct_crs2` table. It handles parameter parsing, environment setup, logging, error trapping, and then calls a core processing script.
    *   **Complexity Tier:** Undetermined (no specific tier information available from analysis, likely due to it being a wrapper).
    *   **Automation Bucket:** `semi_auto`

## 3. Target Architecture
The migration will target Google Cloud Platform (GCP), primarily utilizing BigQuery for data processing and storage, and Cloud Composer (Apache Airflow) for workflow orchestration.

*   **Main Wrapper/Orchestration Logic:** The `r_ausd_v_ta_cntrct_crs2.ksh` script's functionality will be migrated into a BigQuery Stored Procedure, provisioned as `project.dataset.Vertragsdatenabgleich`. This stored procedure will handle parameter parsing, logging initiation, and calling the core logic.
*   **Core Processing Logic:** The currently invoked script, `k_ausd_v_ta_cntrct_crs2.ksh`, is expected to be refactored into a separate BigQuery Stored Procedure, `project.dataset.k_ausd_v_ta_cntrct_crs2`, containing the actual data reconciliation SQL and transformations.
*   **Logging and Auditing:** The existing file-based logging and error handling will be replaced by dedicated BigQuery tables: `project.dataset.job_control` (for overall job status), `project.dataset.job_log` (for detailed log messages), and `project.dataset.job_error_log` (for specific error events).
*   **External Orchestration:** A Cloud Composer DAG (Airflow DAG) will be developed to trigger the BigQuery Stored Procedure, manage its execution, and provide an overall scheduling and monitoring interface. This DAG will also handle environment variables and any minimal shell-like operations that are not suitable for direct BigQuery SQL.

## 4. Data Flow & Lineage
The `r_ausd_v_ta_cntrct_crs2.ksh` script primarily manages control flow rather than direct data manipulation. Its core function is to invoke `k_ausd_v_ta_cntrct_crs2.ksh`.

*   **Execution Order:**
    1.  The Cloud Composer DAG initiates the BigQuery Stored Procedure `project.dataset.Vertragsdatenabgleich`.
    2.  `project.dataset.Vertragsdatenabgleich` performs initial setup, parameter validation, and inserts an entry into `project.dataset.job_control` (status: `RUNNING`).
    3.  `project.dataset.Vertragsdatenabgleich` calls the core logic stored procedure: `project.dataset.k_ausd_v_ta_cntrct_crs2`.
    4.  `project.dataset.k_ausd_v_ta_cntrct_crs2` executes the data reconciliation process, reading from and writing to relevant BigQuery tables, including the `ta_cntrct_crs2` table (specific tables and data flows depend on the unanalyzed core script).
    5.  Upon successful completion, `project.dataset.Vertragsdatenabgleich` updates `project.dataset.job_control` (status: `OK`) and inserts a success message into `project.dataset.job_log`.
    6.  In case of an error, `project.dataset.Vertragsdatenabgleich` logs the error to `project.dataset.job_error_log` and updates `project.dataset.job_control` (status: `ERROR`).

## 5. Transformation Logic
The transformation logic contained within `r_ausd_v_ta_cntrct_crs2.ksh` is purely operational and related to job control, not business data:

*   **Parameter Handling:** The script uses `getopts` for command-line arguments (`-h` for help, and placeholders `-s`, `-l`). This will be replaced by input parameters to the BigQuery Stored Procedure.
*   **Environment Loading:** Sourcing `.dw_init` and other utility scripts will be managed by setting appropriate environment variables in the Cloud Composer environment or passing required values as stored procedure parameters.
*   **Logging:** The calls to `DWMSG_*` functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`) will be translated into `INSERT` and `UPDATE` statements against the BigQuery audit tables (`job_control`, `job_log`, `job_error_log`).
*   **Error Trapping:** The `trap "..." INT` and `trap "..." ERR` constructs will be replaced by BigQuery Scripting's `BEGIN...EXCEPTION...END` blocks for robust error handling and status updates within the stored procedure.
*   **Date Generation:** The `date +%d%m%Y` command will be replaced with `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` in BigQuery SQL.

The actual data transformation for reconciling `ta_cntrct_crs2` is expected to reside within the `k_ausd_v_ta_cntrct_crs2.ksh` script, which will be migrated separately.

## 6. External Dependencies
The current script has no direct external system dependencies (e.g., Oracle, SFTP, S3) beyond its local filesystem and other shell scripts.

*   **Current Dependencies:**
    *   **Filesystem:** `$HOME/.dw_init`, utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`), and the core script `k_ausd_v_ta_cntrct_crs2.ksh`.
    *   **Logging:** Writes to dynamically named log files.
*   **Replacement in GCP:**
    *   **Environment & Utilities:** System-level configurations and utility functions will be either replicated as BigQuery native features (e.g., date functions, logging to tables) or managed by the orchestration layer (Cloud Composer environment variables, Python helper functions if needed).
    *   **Core Script:** Replaced by the `project.dataset.k_ausd_v_ta_cntrct_crs2` BigQuery Stored Procedure.
    *   **Logging:** Replaced by BigQuery audit tables (`project.dataset.job_control`, `project.dataset.job_log`, `project.dataset.job_error_log`).

## 7. Unresolved / Risks
*   **Core Logic Obscurity:** The detailed logic of `k_ausd_v_ta_cntrct_crs2.ksh` is critical but not analyzed in this scope. Any complex SQL, calls to other systems, or file operations within that script will require further analysis and specific migration plans.
*   **Completeness of Utility Script Functionality:** The full extent of functionality within the sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) is not fully known. Their migration to BigQuery equivalents or Python helpers needs careful evaluation.
*   **Missing Complexity Data:** The absence of `complexity_signals`, `tier`, and `migration_flags` from `file_complexity` means a complete picture of potential transformation challenges for this specific wrapper script is not available.
*   **Unused Parameters:** The script declares `-s` and `-l` parameters via `getopts` but does not explicitly use them in the visible code. Their intended purpose and impact on the overall job need to be determined to ensure a complete and accurate migration.

## 8. Build Plan
The migration will proceed in an iterative fashion, focusing first on the control flow and then on the invoked core logic.

1.  **Define BigQuery Audit & Logging Schema (BQSQL):**
    *   Create `CREATE TABLE` statements for `project.dataset.job_control`, `project.dataset.job_log`, and `project.dataset.job_error_log` in BigQuery.
    *   **Language:** BigQuery SQL

2.  **Develop BigQuery Stored Procedure for Wrapper Logic (BQSQL):**
    *   Translate the orchestration, parameter handling, and error trapping logic of `r_ausd_v_ta_cntrct_crs2.ksh` into a BigQuery Stored Procedure named `project.dataset.Vertragsdatenabgleich`.
    *   This procedure will accept `p_h`, `p_s`, `p_l` as input parameters.
    *   It will include `INSERT` and `UPDATE` statements to the audit tables.
    *   It will feature `BEGIN...EXCEPTION...END` blocks for error management.
    *   A placeholder `CALL project.dataset.k_ausd_v_ta_cntrct_crs2(JobKennung, DW_EintragsNr)` will be included.
    *   **Language:** BigQuery SQL

3.  **Analyze and Develop BigQuery Stored Procedure for Core Logic (BQSQL):**
    *   Perform detailed analysis of `k_ausd_v_ta_cntrct_crs2.ksh` to identify data sources, transformation rules, and target tables.
    *   Develop a BigQuery Stored Procedure `project.dataset.k_ausd_v_ta_cntrct_crs2` that encapsulates this core data reconciliation logic using BigQuery SQL.
    *   **Language:** BigQuery SQL

4.  **Implement Cloud Composer DAG for Orchestration (Python):**
    *   Create an Airflow DAG in Python that:
        *   Defines the schedule for the job.
        *   Sets up any necessary environment variables.
        *   Uses the BigQuery operator to call `project.dataset.Vertragsdatenabgleich`, passing appropriate parameters.
        *   Includes error handling and retry mechanisms as required.
    *   **Language:** Python (for Airflow DAG)

5.  **Configuration and Deployment:**
    *   Prepare and deploy the BigQuery audit tables and stored procedures.
    *   Deploy the Cloud Composer DAG to the GCP environment.
    *   Set up IAM permissions for BigQuery access from Cloud Composer.

6.  **Testing:**
    *   Conduct comprehensive unit testing of individual BigQuery stored procedures.
    *   Perform integration testing of the entire workflow via the Cloud Composer DAG, covering successful runs, parameter variations, and various error scenarios.