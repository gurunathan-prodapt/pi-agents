# Migration Design — k_ausd_v_ta_bp_ref.ksh

## 1. Purpose & Scope
The job `k_ausd_v_ta_bp_ref.ksh` is a control script primarily designed to orchestrate the execution of a SQL script (`d_ausd_v_ta_bp_ref.sql`) and manage job entries within a legacy environment. Its core functions include:
- Initializing the environment and loading helper scripts.
- Parsing input parameters: a job identifier (`-j`) and an entry number (`-f`).
- Performing parameter validation and error handling.
- Executing the SQL script `d_ausd_v_ta_bp_ref.sql` via a wrapper function (`starteSQLSkript`).
- Recording the number of processed records.
- Ignoring active jobs and deactivating older active jobs (as per comments).

The scope of this migration design is to convert the logic of this KornShell control script to a BigQuery-native solution, specifically a BigQuery Stored Procedure, ensuring equivalent functionality and integration with BigQuery data processing patterns.

## 2. Source Inventory
The primary source file for this job is `k_ausd_v_ta_bp_ref.ksh`.

| File Path                                                               | Technology  | Tier             | Automation Bucket | Notes                                                                                                                                                                                                                                                                         |
| :---------------------------------------------------------------------- | :---------- | :--------------- | :---------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh` | KornShell   | *Not Available*  | *Not Available*   | This is a shell script acting as an orchestration layer. It sources several utility `.ksh` files and executes a critical SQL script (`d_ausd_v_ta_bp_ref.sql`). The tier and automation bucket data were not available in `file_complexity` or `automation_rate` tables. |

**Key Internal Dependencies (sourced by `k_ausd_v_ta_bp_ref.ksh`):**
- `$HOME/.dw_init` (environment initialization)
- `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (error handling)
- `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (date utility)
- `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (parameter parsing)
- `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL*Plus routines)

**Key Invoked Dependency:**
- `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_bp_ref.sql` (the actual data processing SQL script)

## 3. Target Architecture
The target architecture in BigQuery will involve:
- **BigQuery Stored Procedure for Control Logic:** A main stored procedure, `r_ausd_vertrag_control`, will encapsulate the orchestration logic previously handled by `k_ausd_v_ta_bp_ref.ksh`. This procedure will accept input parameters, perform validation, manage logging, and invoke the core business logic.
- **BigQuery Stored Procedure for Business Logic:** The SQL logic residing in `d_ausd_v_ta_bp_ref.sql` will be migrated into a separate BigQuery Stored Procedure (e.g., `d_ausd_v_ta_bp_ref_logic`). This separation ensures modularity and allows for independent migration and testing of the core data transformation.
- **Error Logging Table:** A dedicated BigQuery table (e.g., `project.dataset.error_log`) to capture error messages, codes, and relevant context for auditing and troubleshooting.
- **Job Run Log Table:** A BigQuery table (e.g., `project.dataset.job_run_log`) to log job execution details, including parameters, processed record counts, and timestamps.
- **Orchestration (Optional but Recommended):** For scheduling and managing the execution of the `r_ausd_vertrag_control` stored procedure, a Cloud Composer (Airflow) DAG can be used. This provides robust scheduling, dependency management, and monitoring capabilities.

## 4. Data Flow & Lineage
Due to the absence of specific `lineage_edges` for `k_ausd_v_ta_bp_ref.ksh`, the data flow is derived from static analysis of the source code.

**Legacy Data Flow:**
1. **Invocation:** The `k_ausd_v_ta_bp_ref.ksh` script is invoked with parameters `-j <JobKennung>` and `-f <EintragsNr>`.
2. **Environment & Utilities:** The script sources environment variables (`. $HOME/.dw_init`) and helper KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3. **Parameter Parsing & Validation:** `getopts` is used to parse input parameters, and `pruefeParameterGesetzt` validates their presence.
4. **Table Name:** A variable `v_TabName` is set to `'ta_bp_ref'`.
5. **SQL Script Execution:** The `starteSQLSkript` function executes the SQL script `d_ausd_v_ta_bp_ref.sql`, passing `p_EintragsNr` and `p_JobKennung`. This SQL script is where the primary data reads/writes occur.
6. **Record Count:** After SQL execution, the script reads a record count from a temporary file (`tmpFile`) into `v_records`. This temp file is likely populated by `starteSQLSkript` or the invoked `d_ausd_v_ta_bp_ref.sql`.
7. **Job Management:** The script's comments indicate it ignores active jobs and deactivates older ones, suggesting interaction with a job metadata table.

**Target BigQuery Data Flow:**
1. **Cloud Composer (or equivalent) Trigger:** A Cloud Composer DAG orchestrates the execution of the BigQuery stored procedure.
2. **BigQuery Stored Procedure Invocation:** The `r_ausd_vertrag_control` BigQuery Stored Procedure is called with `p_JobKennung` and `p_EintragsNr` as parameters.
3. **Parameter Validation & Error Logging:** Inside `r_ausd_vertrag_control`, input parameters are validated. Errors are logged to `project.dataset.error_log` and potentially raise an exception.
4. **Business Logic Execution:** `r_ausd_vertrag_control` calls the `d_ausd_v_ta_bp_ref_logic` BigQuery Stored Procedure (migrated from `d_ausd_v_ta_bp_ref.sql`), passing necessary parameters. This procedure will perform the core data transformations and interact with BigQuery tables (READS/WRITES).
5. **Record Count & Job Logging:** After the business logic executes, `r_ausd_vertrag_control` performs a `COUNT(*)` on the relevant target table to get the number of processed records. This information, along with job details, is logged to `project.dataset.job_run_log`.
6. **Job Management:** Any logic related to ignoring active jobs or deactivating old jobs will be implemented within the `r_ausd_vertrag_control` stored procedure, interacting with relevant BigQuery metadata tables.

## 5. Transformation Logic
The transformation involves converting KornShell scripting constructs and external script invocations into BigQuery SQL Stored Procedure logic.

**Key Transformations:**

- **Environment Variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`):** These will be replaced by BigQuery Stored Procedure parameters, constants, or values from BigQuery configuration tables. Sensitive configurations should use Secret Manager.
- **Sourced Utility Scripts (`. $HOME/.dw_init`, etc.):** The specific functionalities provided by these helper scripts (error handling, date checks, parameter parsing, SQL*Plus routines) need to be reimplemented using BigQuery SQL's native capabilities (e.g., `IF`, `CASE`, `ASSERT`, built-in functions, error handling constructs) or dedicated BigQuery UDFs/Stored Procedures.
- **Parameter Parsing (`getopts`):** The input parameters `-j` and `-f` will directly map to `IN` parameters of the BigQuery Stored Procedure `r_ausd_vertrag_control` (e.g., `p_JobKennung STRING`, `p_EintragsNr STRING`).
- **Parameter Validation (`pruefeParameterGesetzt`):** This will be translated to `IF` conditions and `ASSERT` statements within the BigQuery Stored Procedure. If validation fails, `SIGNAL SQLSTATE '45000'` will be used to raise an error, and details will be logged to `project.dataset.error_log`.
- **Error Messaging (`DWMSG_MeldeFehler`, `echo "FEHLER:..."`):** Replaced by `INSERT` statements into `project.dataset.error_log` for structured logging, and potentially `SELECT` statements for immediate feedback if executed interactively.
- **`v_TabName` Variable:** This will become a `DECLARE` variable or a constant within the BigQuery Stored Procedure.
- **`starteSQLSkript` Function:** This invocation will be replaced by a `CALL` statement to the migrated BigQuery Stored Procedure representing the core SQL logic (`d_ausd_v_ta_bp_ref_logic`).
- **Temporary File for Record Count (`tmpFile`, `eval "v_records=`cat $tmpFile`"`):** This pattern will be replaced by a `DECLARE v_records INT64;` variable, and its value will be set using a `SELECT COUNT(*)` query against the target table after the data processing. The count will then be logged to `project.dataset.job_run_log`.
- **Job Management Logic (ignore/deactivate active jobs):** This implicit logic, if not already part of `d_ausd_v_ta_bp_ref.sql`, must be explicitly designed and implemented as DML operations within `r_ausd_vertrag_control` against appropriate BigQuery metadata tables.

## 6. External Dependencies
The job mainly relies on internal shell scripts and a companion SQL script. No traditional external systems (like Oracle, SFTP, S3) were identified in the `lineage_assembled_jobs` record.

- **Legacy Environment Configuration (`. $HOME/.dw_init`):** This is a critical dependency for environment setup. In BigQuery, this will be handled through explicit parameter passing, environment variables in the orchestration layer (e.g., Cloud Composer), or BigQuery-native configuration tables.
- **Legacy Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):** The functionalities of these scripts will be re-implemented directly in BigQuery SQL procedures or as BigQuery UDFs where appropriate.
- **Legacy SQL Script (`d_ausd_v_ta_bp_ref.sql`):** This script contains the primary data manipulation logic and is a core dependency. It will be migrated into its own BigQuery Stored Procedure (`d_ausd_v_ta_bp_ref_logic`).
- **Job Table:** The script interacts with a "job table" for registration and deactivation. This will be replaced by a BigQuery table acting as a job metadata store.

## 7. Unresolved / Risks

-   **Missing Complexity & Automation Rate Data:** The `file_complexity` and `automation_rate` tables did not contain entries for `k_ausd_v_ta_bp_ref.ksh`. This means the complexity tier, specific migration flags, and an automation bucket (B0-B4) for this file are unknown. Based on the need for a full re-implementation into a BigQuery Stored Procedure, this job will likely fall into **B3 (Manual)** or **B4 (Redesign)** bucket.
-   **Limited Lineage Data:** The `lineage_edges` query did not provide detailed direct dependencies for `k_ausd_v_ta_bp_ref.ksh` or its invoked SQL script. The data flow and dependencies are therefore primarily inferred from source code analysis and comments. This could lead to missed dependencies if the analysis was incomplete.
-   **Migration of `d_ausd_v_ta_bp_ref.sql`:** The design assumes that `d_ausd_v_ta_bp_ref.sql` will be separately migrated into a BigQuery Stored Procedure (`d_ausd_v_ta_bp_ref_logic`). The complexity and data flow of this SQL script are not detailed in this document and need a dedicated analysis.
-   **Dynamic Path Resolution (`BERT_DIR_ROOT`):** The script uses shell variables like `BERT_DIR_ROOT` to construct paths. The exact value of this variable needs to be determined and replicated as a BigQuery constant, parameter, or configured in the orchestration layer.
-   **`starteSQLSkript` Implementation:** The exact logic within the `starteSQLSkript` function (e.g., error handling, connection details, dynamic SQL generation, and temporary file population) is unknown without its source. Its functionality will need to be fully replicated in the BigQuery control stored procedure and the business logic stored procedure.
-   **"Active Jobs" / "Old Active Jobs" Logic:** The comments indicate logic to handle active jobs. The precise definition of "active" and the mechanism for "ignoring" or "deactivating" these jobs are crucial and must be fully understood and implemented in the BigQuery stored procedure, interacting with a dedicated job metadata table.

## 8. Build Plan

The migration will involve creating the following BigQuery artifacts:

1.  **BigQuery Stored Procedure: `project.dataset.d_ausd_v_ta_bp_ref_logic`**
    *   **Language:** BigQuery SQL
    *   **Description:** This procedure will contain the migrated business logic originally found in `d_ausd_v_ta_bp_ref.sql`. It will perform the actual data processing, including `READS` and `WRITES` to BigQuery tables. (Detailed design for this procedure requires analysis of `d_ausd_v_ta_bp_ref.sql`).

2.  **BigQuery Stored Procedure: `project.dataset.r_ausd_vertrag_control`**
    *   **Language:** BigQuery SQL
    *   **Description:** This procedure will encapsulate the control and orchestration logic of the original `k_ausd_v_ta_bp_ref.ksh` script.
        *   Accepts `p_JobKennung` and `p_EintragsNr` as input.
        *   Includes parameter validation and error handling.
        *   `CALL`s `project.dataset.d_ausd_v_ta_bp_ref_logic` for core data processing.
        *   Manages job metadata (ignoring/deactivating jobs) and logs job run details (including record count).

3.  **BigQuery Table: `project.dataset.error_log`**
    *   **Language:** BigQuery DDL
    *   **Description:** Table to store structured error messages, timestamps, error codes, and context from procedure executions.
    *   **Schema:** `error_ts TIMESTAMP, error_code INT64, error_arg STRING, job_kennung STRING, eintrags_nr STRING, script_name STRING, message STRING`.

4.  **BigQuery Table: `project.dataset.job_run_log`**
    *   **Language:** BigQuery DDL
    *   **Description:** Table to log details of each job execution, including input parameters and processed record counts.
    *   **Schema:** `run_ts TIMESTAMP, job_kennung STRING, eintrags_nr STRING, tab_name STRING, records_processed INT64`.

5.  **Cloud Composer DAG (Optional, but Recommended)**
    *   **Language:** Python
    *   **Description:** An Airflow DAG to schedule and trigger the `project.dataset.r_ausd_vertrag_control` BigQuery Stored Procedure, passing required parameters. This will handle the external orchestration aspect of the original KornShell script.