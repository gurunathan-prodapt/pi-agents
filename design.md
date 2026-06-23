# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier_zusgf.ksh

## 1. Purpose & Scope
This job is a wrapper KornShell script responsible for orchestrating the execution of a core script for data reconciliation related to the `ta_barrier_zusgf` table. Its primary purpose is to set up the execution environment, parse parameters, handle logging and error trapping, and then invoke the main processing logic encapsulated in another script. It acts as an operational wrapper rather than containing direct business transformation logic.

**Seed Name:** vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier_zusgf.ksh
**Summary from `file_analysis`:** This is a wrapper KornShell script that orchestrates the execution of a core script for data reconciliation of the 'ta_barrier_zusgf' table. It handles environment setup, parameter parsing, logging, and error trapping.
**Purpose Note from `lineage_assembled_jobs`:** Job assembled from 1 component(s); stage dist: medium=1

## 2. Source Inventory
The job consists of a single KornShell script.

| File Path                                                         | Technology  | Category | Tool       | Complexity Tier | Migration Bucket |
| :---------------------------------------------------------------- | :---------- | :------- | :--------- | :-------------- | :--------------- |
| vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier_zusgf.ksh | Shell Script | shell    | KornShell | medium          | semi_auto        |

## 3. Target Architecture
The target architecture in BigQuery will involve converting the KornShell script into a BigQuery Stored Procedure. This stored procedure will handle environmental setup, parameter parsing, logging, error handling, and the invocation of the core business logic.

*   **Main Component:** BigQuery Stored Procedure: `project.dataset.Vertragsdatenabgleich`
*   **Logging & Auditing:** Dedicated BigQuery tables will replace the file-based logging and status tracking:
    *   `project.dataset.job_registry`: To track job metadata and status.
    *   `project.dataset.job_log`: For detailed log messages.
    *   `project.dataset.job_error`: To record error details.
*   **Core Logic:** The invoked core script `k_ausd_v_ta_barrier_zusgf.ksh` will be replaced by a separate BigQuery Stored Procedure: `project.dataset.k_ausd_v_ta_barrier_zusgf`.
*   **Parameter Management:** Command-line parameters will be mapped to input parameters of the BigQuery Stored Procedure. Environment variables will be replaced by configuration tables or stored procedure parameters.

## 4. Data Flow & Lineage

The current script acts as an orchestration layer.

**Legacy Flow:**
1.  **Initialization:** The script sources `$HOME/.dw_init` and several utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) from `${BERT_DIR_ROOT}` to set up the environment and error handling framework.
2.  **Parameter Reading:** Uses `getopts` to parse command-line arguments.
3.  **Error Handling Setup:** Initializes a global error handling mechanism and sets up shell `trap`s for `INT` and `ERR` signals, directing error handling to `DWMSG_Fehlerbehandlung`.
4.  **Job Metadata:** Determines `DW_EintragsNr`, `JobKennung`, `v_sysdate` (current date), and `LogDatei` using framework functions.
5.  **Core Script Invocation:** Executes the core business logic script `k_ausd_v_ta_barrier_zusgf.ksh` with specific parameters (`-j $JobKennung -f ${DW_EintragsNr}`) and redirects its output to the generated log file.
6.  **Status Update:** On successful completion, logs a success message and updates the job status using `DWMSG_SetzeStatusOK`.

**Target BigQuery Flow:**
1.  **`project.dataset.Vertragsdatenabgleich` (Stored Procedure):**
    *   Takes input parameters `p_h`, `p_s`, `p_l` (corresponding to shell script arguments).
    *   Handles usage display if `p_h` is provided, logging to `project.dataset.job_log`.
    *   Performs parameter validation (error conditions will lead to `SIGNAL SQLSTATE`).
    *   Generates `DW_EintragsNr` (Job ID) and `LogDatei` name, leveraging `project.dataset.job_registry` for sequence.
    *   Inserts initial job status into `project.dataset.job_registry` and detailed log into `project.dataset.job_log`.
    *   **Core Logic Invocation:** Calls `project.dataset.k_ausd_v_ta_barrier_zusgf` (the migrated core business logic SP), passing `JobKennung` and `DW_EintragsNr`.
    *   **Error Handling:** Uses BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END;` block to catch errors during core logic execution, updating `project.dataset.job_error` and `project.dataset.job_registry` with FAILED status, and signaling an error.
    *   **Success Handling:** If successful, logs completion message to `project.dataset.job_log` and updates `project.dataset.job_registry` with OK status.

## 5. Transformation Logic

The KornShell script itself does not contain complex data transformations but rather acts as an orchestrator. The migration primarily focuses on translating shell control flow, parameter handling, and external script invocations into BigQuery procedural language and table-based logging.

**Key Transformation Areas:**

*   **Environment Variables:**
    *   `$HOME`, `${BERT_DIR_ROOT}`: These will be replaced by explicit BigQuery stored procedure parameters, or by values stored in BigQuery configuration tables.
    *   `JobKennung`, `v_sysdate`, `LogDatei`, `DW_EintragsNr`: These dynamic variables will be mapped to BigQuery `DECLARE`d variables within the stored procedure, with values derived using BigQuery functions (e.g., `CURRENT_DATE()`, `FORMAT_DATE()`).
*   **Parameter Parsing (`getopts`):**
    *   The shell script's `getopts` logic will be replaced by direct input parameters to the BigQuery Stored Procedure (`p_h`, `p_s`, `p_l`). Validation will happen explicitly.
*   **Error Handling (`trap`, `DWMSG_*` functions):**
    *   Shell `trap`s will be replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END;` blocks for error capture.
    *   The `DWMSG_*` error and logging functions will be replaced by `INSERT` statements into the `job_log`, `job_error`, and `job_registry` tables.
    *   Error exit codes will translate to `SIGNAL SQLSTATE` statements.
*   **External Script Invocation (`${Name_Kernskript}`):**
    *   The execution of `k_ausd_v_ta_barrier_zusgf.ksh` will be directly translated to a BigQuery Stored Procedure `CALL` to `project.dataset.k_ausd_v_ta_barrier_zusgf`.
*   **File I/O (`print`, `tee -a`, `>> $LogDatei`):**
    *   All console output and log file writes will be converted to `INSERT` statements into the `project.dataset.job_log` table.
*   **Date Formatting:**
    *   `date +%d%m%Y` will be replaced by `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.

**Pseudocode (BigQuery SQL):**
Refer to the `BigQuery SQL Pseudocode` section of the CM MCP tool output for detailed logic.

## 6. External Dependencies

The original script has dependencies on other shell scripts and internal framework functions for logging and date handling. It also invokes a core business script.

*   **Sourced Utility Scripts:**
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utilities.
    *   **Replacement:** These scripts' functionalities (environment setup, parameter defaults, logging utilities, date formatting) will need to be re-implemented directly within the BigQuery Stored Procedure, using BigQuery SQL features, or by populating configuration tables that the SP can read.
*   **Core Business Logic Script:**
    *   `Name_Kernskript="${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh"`: This is the critical dependency.
    *   **Replacement:** This script *must* be migrated to a separate BigQuery Stored Procedure, named `project.dataset.k_ausd_v_ta_barrier_zusgf`. The wrapper SP will `CALL` this core SP.
*   **OS Commands/Utilities:**
    *   `date`: Replaced by `CURRENT_DATE()` and `FORMAT_DATE()`.
    *   `getopts`: Replaced by SP parameters.
    *   `tee`: Replaced by `INSERT` into logging tables.
*   **External Systems:** No explicit external systems (like Oracle, SFTP, S3) were identified in the script. The primary interaction is with the filesystem (for logs and other scripts).

## 7. Unresolved / Risks

*   **Core Script (`k_ausd_v_ta_barrier_zusgf.ksh`) Content:** The most significant unresolved item is the actual content and complexity of `k_ausd_v_ta_barrier_zusgf.ksh`. Its migration strategy will dictate the overall complexity and success of this job's migration. Without its content, it's assumed to be migratable to a BigQuery Stored Procedure or equivalent. If it involves complex file manipulations, external system calls, or non-SQL logic, further analysis and potential redesign (B4) will be required for that component.
*   **Shell-specific Behaviors:**
    *   `source` / `.` command: Loading environment variables and functions from external files will require redesign. This is addressed by using SP parameters or configuration tables.
    *   `trap` semantics: Asynchronous shell error handling is replaced by synchronous BigQuery exception handling. This changes the behavior slightly but is generally functionally equivalent.
    *   Stdout/stderr redirection (`>>`): Replaced by structured logging to tables.
    *   `tee` command: Parallel output to console and file replaced by structured logging to table.
*   **Migration Bucket `semi_auto`:** This classification indicates that while automated conversion tools can provide a significant head start, manual intervention and potentially some redesign will be necessary to fully adapt the script to BigQuery's paradigm, especially concerning shell-specific constructs and the integration of the core script.

## 8. Build Plan

The build plan will involve creating the necessary BigQuery assets.

1.  **Define BigQuery Logging & Audit Tables:**
    *   Create `project.dataset.job_registry` table.
    *   Create `project.dataset.job_log` table.
    *   Create `project.dataset.job_error` table.
    *   *Language:* BigQuery DDL (SQL)

2.  **Migrate Core Business Logic Script:**
    *   Analyze `k_ausd_v_ta_barrier_zusgf.ksh` and design its migration to a BigQuery Stored Procedure.
    *   Create `project.dataset.k_ausd_v_ta_barrier_zusgf` Stored Procedure. This is a prerequisite for the wrapper.
    *   *Language:* BigQuery SQL

3.  **Create Wrapper BigQuery Stored Procedure:**
    *   Translate `r_ausd_v_ta_barrier_zusgf.ksh` into the `project.dataset.Vertragsdatenabgleich` Stored Procedure.
    *   This will include:
        *   Parameter definitions (`p_h`, `p_s`, `p_l`).
        *   Variable declarations (`ErrNr`, `JobKennung`, `v_sysdate`, etc.).
        *   Conditional logic for parameter validation and usage display.
        *   Logic for logging job status and messages to the audit tables.
        *   The `CALL` to `project.dataset.k_ausd_v_ta_barrier_zusgf`.
        *   Exception handling (`BEGIN...EXCEPTION WHEN ERROR THEN...END;`).
    *   *Language:* BigQuery SQL

4.  **Deployment & Testing:**
    *   Deploy the BigQuery tables and stored procedures.
    *   Thoroughly test the `project.dataset.Vertragsdatenabgleich` stored procedure, ensuring correct parameter handling, logging, error trapping, and successful invocation of the core logic.