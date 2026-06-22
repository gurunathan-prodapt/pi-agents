# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh

## 1. Purpose & Scope
The KornShell script `k_ausd_v_ta_period.ksh` acts as a control and orchestration script. Its primary purpose is to manage the execution of an underlying SQL script, `d_ausd_v_ta_period.sql`, for data preparation related to `ta_period`. As per its comments, it performs the following functions:
- Initializes the environment and loads helper libraries.
- Parses and validates command-line parameters (JobKennung and EintragsNr).
- Calls the SQL script `d_ausd_v_ta_period.sql` to perform the actual data processing.
- Handles error conditions and reports status, including the number of processed records from a temporary file.
The script ensures that active jobs are ignored and old active jobs are deactivated, although this logic is likely delegated to the `starteSQLSkript` wrapper or the SQL script itself.

## 2. Source Inventory
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh`
  - **Technology:** KornShell
  - **Complexity Tier:** medium
  - **Automation Bucket:** semi_auto
  - **Purpose:** ETL (control/orchestration)

The script relies on several sourced KornShell utility scripts for environment setup, error handling, parameter parsing, and SQL execution (e.g., `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). The core data transformation logic is contained within the `d_ausd_v_ta_period.sql` file that this script invokes.

## 3. Target Architecture
The migration to BigQuery will involve:
- **BigQuery Stored Procedure (Orchestration):** The control flow and parameter validation logic of `k_ausd_v_ta_period.ksh` will be re-implemented as a BigQuery stored procedure (e.g., `project.dataset.r_ausd_vertrag_control`). This procedure will accept parameters and manage the execution of the data transformation logic.
- **BigQuery Stored Procedure (Transformation):** The actual data transformation logic from `d_ausd_v_ta_period.sql` will be migrated into a separate BigQuery stored procedure (e.g., `project.dataset.d_ausd_v_ta_period`) or a series of BigQuery DML statements within the orchestrating procedure.
- **Audit/Logging Table:** A BigQuery table (e.g., `project.dataset.job_audit`) will be used to track job status, parameters, processed records, and error messages, replacing the current file-based logging and job tracking mechanisms.
- **Python Orchestration (Optional):** If complex environment sourcing, external file interactions (beyond BigQuery's capabilities), or advanced parameter parsing are still required, a lightweight Python wrapper could be used to invoke the BigQuery stored procedures.

## 4. Data Flow & Lineage
The original data flow involves:
- `k_ausd_v_ta_period.ksh` (KornShell script)
  - Invokes `d_ausd_v_ta_period.sql` (SQL script) via `starteSQLSkript`.
- `d_ausd_v_ta_period.sql` (SQL script)
  - **Reads from:**
    - `TABLE:DWTK_MELDUNGEN`
    - `TABLE:CDS$TA_PERIOD`
  - **Writes to:**
    - `TABLE:SOF$TA_PERIOD`
    - `TABLE:VIA`
  - **Uses:**
    - `PACKAGE:DWPA_UTIL_SKRIPT`

In BigQuery, this will translate to:
- A BigQuery orchestration stored procedure will call a BigQuery transformation stored procedure.
- The transformation stored procedure will read data from the migrated `DWTK_MELDUNGEN` and `CDS$TA_PERIOD` tables (or their BigQuery equivalents).
- It will write processed data to the migrated `SOF$TA_PERIOD` and `VIA` tables (or their BigQuery equivalents).
- Any functionality from `PACKAGE:DWPA_UTIL_SKRIPT` will need to be re-implemented in BigQuery SQL as UDFs or part of the stored procedure logic.

## 5. Transformation Logic
The `k_ausd_v_ta_period.ksh` script itself does not contain business transformation logic. Its logic is limited to:
- Environment setup (`. $HOME/.dw_init`, etc.)
- Parameter parsing (`getopts`, `pruefeParameterGesetzt`)
- Error handling (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`)
- Invocation of the main SQL script (`starteSQLSkript`)

The core transformation logic resides within `d_ausd_v_ta_period.sql`. This SQL script, once extracted, will need to be converted to BigQuery SQL, ensuring all Oracle/legacy SQL constructs are translated to their BigQuery equivalents. This will likely involve mapping data types, functions, and query structures. The `starteSQLSkript` functionality (active job handling, deactivation of old jobs) should be incorporated into the BigQuery orchestration logic, possibly using conditional statements and updates to the `job_audit` table.

## 6. External Dependencies
- **Original Script Dependencies:**
    - **Oracle Database:** Implicitly used by the SQL script `d_ausd_v_ta_period.sql` for reading from `DWTK_MELDUNGEN`, `CDS$TA_PERIOD` and writing to `SOF$TA_PERIOD`, `VIA`, and potentially the `DWPA_UTIL_SKRIPT` package.
    - **Legacy Shell Environment:** Relies on environment variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) and sourced `.ksh` utility files.
    - **Temporary Files:** Uses a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_period_$$.tmp`) for inter-process communication (e.g., passing record counts).

- **BigQuery Replacements:**
    - **BigQuery Data Warehouse:** All Oracle tables (`DWTK_MELDUNGEN`, `CDS$TA_PERIOD`, `SOF$TA_PERIOD`, `VIA`) will be migrated to BigQuery tables.
    - **BigQuery Stored Procedures/Scripting:** The shell environment variables and helper scripts will be replaced by BigQuery stored procedure parameters, scripting variables, and built-in BigQuery functions.
    - **BigQuery Audit/Logging Table:** The temporary file communication will be replaced by direct variable assignments within BigQuery stored procedures and logging to a dedicated audit table.
    - **Service Accounts/Workload Identity:** Database authentication will switch from embedded SQL*Plus credentials to BigQuery service accounts with appropriate IAM roles for dataset and table access.

No other external systems were identified in the `external_systems` metadata for this job.

## 7. Unresolved / Risks
- **SQL Script Conversion Complexity:** The actual complexity of converting `d_ausd_v_ta_period.sql` to BigQuery SQL is unknown. This is the critical piece containing the business logic. Any Oracle-specific SQL features will require careful re-engineering.
- **`DWPA_UTIL_SKRIPT` Functionality:** The exact functionality of `PACKAGE:DWPA_UTIL_SKRIPT` is not detailed. This package will need to be analyzed and its logic reimplemented in BigQuery SQL as UDFs or integrated into stored procedures.
- **Job Activation/Deactivation Logic:** The comments mention "aktive Jobs werden ignoriert" and "alte aktive Jobs werden einfach dekativiert". The precise implementation of this logic (whether in `starteSQLSkript`, `d_ausd_v_ta_period.sql`, or other components) needs to be fully understood and replicated in the BigQuery orchestration layer, potentially using a job metadata table.
- **Parameter Validation (`pruefeParameterGesetzt`):** While basic presence checks are clear, any more complex validation rules within this helper script need to be identified and replicated in the BigQuery stored procedure.
- **Full Scope of Sourced Scripts:** The exact contents and side effects of all sourced `.ksh` files (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) are not fully known and may introduce hidden complexities if they perform actions beyond what is immediately obvious.
- **Empty `unresolved_targets`:** This indicates that the analysis tools did not find any explicitly unresolved references. However, the implicit dependencies within the shell environment and SQL package need thorough investigation during detailed design.

## 8. Build Plan
1.  **Extract and Analyze `d_ausd_v_ta_period.sql`:**
    - Obtain the source code for `d_ausd_v_ta_period.sql`.
    - Perform a detailed analysis of its SQL logic, identifying Oracle-specific constructs, table structures, and transformation rules.
2.  **Schema Migration:**
    - Migrate the schemas of `DWTK_MELDUNGEN`, `CDS$TA_PERIOD`, `SOF$TA_PERIOD`, and `VIA` from Oracle to BigQuery, adapting data types and partitioning strategies as needed.
3.  **Migrate `d_ausd_v_ta_period.sql` to BigQuery Stored Procedure:**
    - Convert `d_ausd_v_ta_period.sql` into a BigQuery SQL stored procedure (e.g., `d_ausd_v_ta_period_proc.sql`). This will be the core data transformation component.
4.  **Re-implement `DWPA_UTIL_SKRIPT`:**
    - Analyze the `DWPA_UTIL_SKRIPT` package and implement its functionality in BigQuery SQL, either as UDFs or integrated into the stored procedures.
5.  **Create BigQuery Audit Table:**
    - Design and create the `job_audit` table in BigQuery to log job execution status, parameters, and metrics.
6.  **Develop BigQuery Orchestration Stored Procedure (`k_ausd_v_ta_period_proc.sql`):**
    - Implement the logic from `k_ausd_v_ta_period.ksh` in a new BigQuery stored procedure (e.g., `r_ausd_vertrag_control.sql`).
    - This procedure will include:
        - Parameter validation for `p_JobKennung` and `p_EintragsNr`.
        - Logging job start, completion, and errors to the `job_audit` table.
        - Calling the `d_ausd_v_ta_period_proc` with the appropriate parameters.
        - Capturing and reporting `@@row_count` after the transformation.
        - Handling error conditions using `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks.
7.  **Optional Python Orchestration Layer:**
    - If needed, create a small Python script to trigger the BigQuery orchestration stored procedure, handle external parameter passing, or manage more complex scheduling if not handled by Airflow or similar.
8.  **Testing:**
    - Unit test each BigQuery stored procedure.
    - Integration test the entire BigQuery workflow, comparing results with the legacy system.

This build plan will result in BigQuery SQL code for the stored procedures and table definitions, and potentially Python code for an orchestration layer.