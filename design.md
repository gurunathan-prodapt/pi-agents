# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh

## 1. Purpose & Scope
This KornShell script, `k_ausd_v_ta_inv_def.ksh`, serves as a control script for a data preparation process. Its primary purpose is to manage the execution of a core SQL script (`d_ausd_v_ta_inv_def.sql`), handle parameter parsing, set up the execution environment, record job status, and deactivate older active jobs. It ensures that necessary parameters are provided before invoking the SQL data manipulation logic. The script aims to provide a robust wrapper for a critical data definition or data update task related to `ta_inv_def`.

## 2. Source Inventory
The job is composed of a single main script.
- **File Name:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh`
- **Technology:** KornShell (Shell script)
- **Summary:** KornShell script that acts as a control script for a data preparation process, handling parameter parsing, environment setup, and executing a SQL script for data manipulation.
- **Complexity Tier:** Medium (inferred from `purpose_note: stage dist: medium=1`, as `file_complexity` data was not available for this file).
- **Automation Bucket:** semi_auto

## 3. Target Architecture
The target architecture in BigQuery will involve:
- **BigQuery Stored Procedure:** The core logic of the shell script (parameter handling, job orchestration, error management) will be migrated to a BigQuery Stored Procedure, e.g., `project.dataset.r_ausd_vertrag_control`.
- **BigQuery Tables:**
    - `job_table`: To manage job statuses, active flags, and job metadata (replacing current job table functionality).
    - `job_error_log`: For structured error logging, replacing shell-based error messages and `DWMSG_MeldeFehler`.
    - `job_run_log`: To store run-specific details like records processed, replacing the temporary file mechanism.
    - `ta_inv_def_result`: A target table where the migrated SQL logic (`d_ausd_v_ta_inv_def.sql`) will write its results (this table name is inferred from the script's `v_TabName` variable and common ETL patterns).
- **Dataform/Cloud Composer (Optional):** If there are complex scheduling, external system interactions, or dependencies not fully covered by BigQuery Stored Procedures, an orchestration layer might be introduced using Dataform (for SQL-centric workflows) or Cloud Composer (for broader orchestration needs).

## 4. Data Flow & Lineage
The original script's data flow:
1.  **Environment Setup:** The script sources various utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). These provide helper functions and environment variables.
2.  **Parameter Parsing:** Command-line parameters (`-j job_kennung`, `-f entry_number`) are parsed using `getopts`.
3.  **Parameter Validation:** The `pruefeParameterGesetzt` helper function validates the presence of required parameters. If validation fails, an error is logged (`DWMSG_MeldeFehler`) and the script exits.
4.  **SQL Script Execution:** The script constructs the path to a critical SQL script: `d_ausd_v_ta_inv_def.sql`. It then calls `starteSQLSkript` (likely a wrapper for `sqlplus` or similar) to execute this SQL script, passing the parsed parameters.
5.  **Record Count & Logging:** After the SQL execution, the script reads a temporary file (`bert_k_ausd_v_ta_inv_def_$$.tmp`) to obtain the number of processed records, which is then assigned to `v_records`. An "ENDE Datenverarbeitung" message is printed.

**Migrated Data Flow in BigQuery:**
1.  **Stored Procedure Invocation:** The BigQuery Stored Procedure `r_ausd_vertrag_control` is called with `p_JobKennung` and `p_EintragsNr` as input parameters.
2.  **Parameter Validation:** The stored procedure internally validates parameters. Missing parameters trigger an `INSERT` into `job_error_log` and an early exit.
3.  **Job Management (Updates/Inserts):**
    -   Existing 'active' jobs with the same `job_kennung` but different `eintrags_nr` are updated in `job_table` to set `active_flag = FALSE`.
    -   The current job's details (`job_kennung`, `eintrags_nr`) are inserted into `job_table` with `active_flag = TRUE`.
4.  **Core SQL Logic Execution:** A nested BigQuery Stored Procedure, `d_ausd_v_ta_inv_def`, representing the migrated logic of the original `d_ausd_v_ta_inv_def.sql` script, is called. This procedure performs the actual data manipulation and writes to `ta_inv_def_result`.
5.  **Record Count & Logging:** After the SQL logic, `COUNT(*)` from `ta_inv_def_result` (or another appropriate source) is used to determine the `v_records`. This count, along with other run details, is inserted into `job_run_log`. Completion messages are generated.

## 5. Transformation Logic
The transformation logic focuses on migrating the shell script's control flow and parameter handling to a BigQuery Stored Procedure. The core data transformations are assumed to be within the `d_ausd_v_ta_inv_def.sql` script, which would be separately migrated to BigQuery SQL.

**Shell Script Constructs to BigQuery SQL/Stored Procedures:**
-   **Environment Variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`):** These will be replaced by:
    -   Stored procedure input parameters for dynamic values.
    -   Configuration tables or constants within the BigQuery Stored Procedure for static paths/values.
    -   Dataset/project names implicitly handled by BigQuery context.
-   **Parameter Parsing (`getopts`, `ParamList`):** Replaced by named input parameters to the BigQuery Stored Procedure (`p_JobKennung`, `p_EintragsNr`).
-   **Parameter Validation (`pruefeParameterGesetzt`, `if` conditions):** Translated to `IF...THEN...END IF;` blocks within the BigQuery Stored Procedure.
-   **Error Handling (`DWMSG_MeldeFehler`, `exit`):** Replaced by:
    -   `INSERT` statements into a dedicated `job_error_log` table.
    -   `SELECT FORMAT(...)` for output messages.
    -   `LEAVE` statement to exit the procedure on error.
-   **SQL Script Invocation (`starteSQLSkript`):** Replaced by a `CALL` to a separate BigQuery Stored Procedure (e.g., `project.dataset.d_ausd_v_ta_inv_def`) that encapsulates the migrated SQL logic.
-   **Temporary File Read (`cat $tmpFile`, `eval v_records=`):** Replaced by direct `SELECT COUNT(*)` from the target table or a result table within the BigQuery Stored Procedure to capture the number of processed records. This value can be stored in a `DECLARE` variable and then inserted into a `job_run_log`.
-   **Shell Utilities (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`):** The functionalities of these helper scripts need to be reimplemented or replaced by native BigQuery features or existing BigQuery utilities. Error messaging, date handling, and parameter handling logic will be integrated directly into the stored procedure. SQLPlus specific interactions will be removed as BigQuery SQL handles direct SQL execution.

## 6. External Dependencies
The original script has internal dependencies on other shell scripts and potentially the `sqlplus` utility.
-   `$HOME/.dw_init`: An environment initialization script.
-   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
-   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utilities.
-   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing utilities (though `getopts` is used directly in the main script, this might provide helpers for `pruefeParameterGesetzt`).
-   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: Routines for SQL script execution, likely a wrapper around `sqlplus` or similar database client.
-   SQL script: `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_inv_def.sql`.

**Replacement in BigQuery:**
-   **Environment Initialization:** Replaced by BigQuery dataset/project configuration, stored procedure parameters, or constants within the stored procedure.
-   **Error Messaging:** Replaced by structured logging into a BigQuery `job_error_log` table.
-   **Date Handling:** Replaced by BigQuery's native date/time functions (e.g., `CURRENT_TIMESTAMP()`, `DATE_TRUNC()`).
-   **Parameter Handling:** Replaced by BigQuery Stored Procedure input parameters and internal `IF` conditions.
-   **SQL Script Execution Wrapper:** Replaced by directly calling a nested BigQuery Stored Procedure that contains the migrated `d_ausd_v_ta_inv_def.sql` logic. The need for an external client like `sqlplus` is eliminated.
-   **Oracle/Legacy Database:** The implicit Oracle database interaction (via `sqlplus` wrapper) will be replaced by direct BigQuery SQL operations.

No other external systems (like SFTP, S3) were detected by the lineage analysis.

## 7. Unresolved / Risks
-   **Detailed Logic of `d_ausd_v_ta_inv_def.sql`:** The actual SQL logic within this script is critical. Its complexity and any database-specific features (e.g., PL/SQL, specific Oracle functions) will significantly impact the migration effort to BigQuery SQL. This document assumes it can be directly translated or refactored into a BigQuery Stored Procedure.
-   **`starteSQLSkript` Implementation:** The exact behavior of `starteSQLSkript` (e.g., transaction management, error handling within the SQL execution, specific `sqlplus` commands) is not fully known. Any sophisticated behavior needs careful investigation and reimplementation in BigQuery.
-   **"Job Table" Semantics:** The script references a "job table" and "deactivating older active jobs". The exact schema and business rules for this job table need to be fully understood to correctly replicate in BigQuery.
-   **Temporary File Content:** While assumed to be record count, any other content or format in `$DW_DIR_UTL/bert_k_ausd_v_ta_inv_def_$$.tmp` needs to be verified.
-   **`f_alis_msgerr.ksh` / `h_alis_date.ksh` / `h_alis_parameter.ksh` specifics:** While general replacements are identified, any complex or unique logic within these helper scripts must be fully analyzed and replicated in BigQuery SQL or Python if necessary.
-   **No `file_complexity` data:** The absence of `file_complexity` data could hide unforeseen challenges that would usually be flagged. The "medium" complexity is an estimate.

## 8. Build Plan
1.  **Migrate `d_ausd_v_ta_inv_def.sql` to BigQuery Stored Procedure (`project.dataset.d_ausd_v_ta_inv_def`):**
    -   **Language:** BigQuery SQL / Stored Procedures
    -   **Task:** Analyze the original SQL script, identify any Oracle-specific syntax or functions, and rewrite it for BigQuery. This will include creating the target table `ta_inv_def_result` if it does not exist.
2.  **Create BigQuery Tables for Job Management:**
    -   **Language:** BigQuery DDL (Data Definition Language)
    -   **Task:** Define schemas for `project.dataset.job_table`, `project.dataset.job_error_log`, and `project.dataset.job_run_log` based on the script's logic and business requirements.
3.  **Migrate `k_ausd_v_ta_inv_def.ksh` to BigQuery Stored Procedure (`project.dataset.r_ausd_vertrag_control`):**
    -   **Language:** BigQuery SQL / Stored Procedures
    -   **Task:** Implement the pseudocode provided in the design document, translating shell constructs to BigQuery SQL. This includes parameter validation, job table updates, and calling the `d_ausd_v_ta_inv_def` stored procedure.
4.  **Implement Helper Functionality:**
    -   **Language:** BigQuery SQL / Stored Procedures (or Python if advanced logic required)
    -   **Task:** Incorporate the error logging, date handling, and parameter validation logic from the sourced helper scripts directly into the `r_ausd_vertrag_control` stored procedure or as separate, smaller BigQuery functions/procedures.
5.  **Orchestration (Optional):**
    -   **Language:** Python (for Cloud Composer/Airflow DAG) or YAML (for Workflows) or SQL (for Dataform)
    -   **Task:** If external orchestration is chosen, create a DAG or workflow to trigger the `project.dataset.r_ausd_vertrag_control` BigQuery Stored Procedure, passing the necessary parameters. This would replace any existing scheduler that ran the original ksh script.