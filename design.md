# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh

## 1. Purpose & Scope
This job, `r_ausd_bp_ta_bpr_optionen.ksh`, serves as an orchestration wrapper script responsible for the initial provisioning of selected base products for the BERT system. Its primary functions include parsing command-line parameters, handling execution date defaults, managing logging and job tracking, implementing error trapping, and invoking a core provisioning script. The overall purpose of the assembled job is described as "Job assembled from 1 component(s); stage dist: medium=1".

The scope of this migration design document is to detail the conversion of this KornShell wrapper script to a BigQuery-native solution, primarily using BigQuery Stored Procedures, while ensuring equivalent functionality regarding parameter handling, job control, logging, and orchestration of the core business logic.

## 2. Source Inventory
The job consists of a single KornShell script:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh`
    *   **Technology:** KornShell (shell script)
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto
    *   **Purpose:** Orchestration and job control. It acts as a launcher for a downstream core script that handles the actual data provisioning.

## 3. Target Architecture
The target architecture in BigQuery will primarily leverage BigQuery Stored Procedures to encapsulate the orchestration and control logic of the original KornShell script.

*   **Orchestration Layer:** A BigQuery Stored Procedure, tentatively named `project.dataset.ausd_bp_ta_bpr_optionen_wrapper`, will replace the KornShell script. This procedure will handle:
    *   Parameter parsing and validation.
    *   Defaulting of date values.
    *   Job control and logging.
    *   Invocation of the core business logic.
*   **Job Control and Logging:** Dedicated BigQuery tables will be used for:
    *   `project.dataset.job_control`: To store job metadata, status, and tracking information (e.g., `job_nr`, `job_kennung`, `script_name`, `log_file`, `stichtag_info`, `status`, `created_at`, `finished_at`).
    *   `project.dataset.job_log`: To store detailed log messages, errors, and informational output (e.g., `job_kennung`, `log_level`, `err_nr`, `err_arg`, `message`, `created_at`).
*   **Core Logic:** The business logic originally contained in the invoked `k_ausd_bp_ta_bpr_optionen.ksh` (which is not part of this job's components) will be migrated into a separate BigQuery Stored Procedure or set of SQL scripts. This design assumes this core logic will be available as another callable BigQuery component, e.g., `project.dataset.k_ausd_bp_ta_bpr_optionen`.
*   **Orchestration (External):** If the core logic `k_ausd_bp_ta_bpr_optionen.ksh` itself contains complex external interactions or is to be migrated as a separate, orchestrated job (e.g., a Dataflow job), Cloud Composer (Airflow DAG), Cloud Workflows, or Cloud Functions may be used for overall orchestration. For this wrapper script, the focus is on direct translation to BigQuery Stored Procedures.

## 4. Data Flow & Lineage
The original script performs the following logical flow, which will be replicated in the BigQuery environment:

1.  **Environment Setup (Source)**: The KornShell script first sources several utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). These provide environment variables, error handling, parameter parsing, and date utility functions.
2.  **Parameter Reading**: Command-line arguments (`-s` for Stichtag/cutoff date, `-l` for Wiederanlaufwert/restart value) are parsed using `getopts`.
3.  **Parameter Initialization/Defaulting**:
    *   `p_wiederanlaufWert` is defaulted to `0` if not provided.
    *   `p_stichtag` is defaulted to the current system date (`v_sysdate` derived from `DWDate_Gib_Zeitraum`) if not explicitly set.
4.  **Parameter Validation**: `pruefeParameterGesetzt` is called to validate required parameters. If validation fails, an error is logged, usage information is printed, and the script exits.
5.  **Job Metadata and Logging Setup**:
    *   A unique job entry number (`DW_EintragsNr`) is determined.
    *   A log file name (`LogDatei`) is constructed.
    *   A job entry is created in the internal job tracking system (implied by `DWMSG_ErzeugeEintrag`).
    *   Stichtag information is set.
6.  **Error Trapping**: Shell `trap` commands are set up to handle `INT`, `STOP`, `CONT`, and `ERR` signals, invoking `DWMSG_Fehlerbehandlung` on errors.
7.  **Job Execution**: The core script, `k_ausd_bp_ta_bpr_optionen.ksh`, is invoked with the gathered parameters (`-j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert}`).
8.  **Post-Execution Handling**:
    *   On successful completion, a success message is logged, and the job status is marked as OK (`DWMSG_SetzeStatusOK`).
    *   Error traps are cleared, and the script exits with `0` for success or an error code for failure.

This flow will be translated into a BigQuery Stored Procedure, where:
*   Shell sourcing will be replaced by direct declarations, external configuration tables, or the use of BigQuery built-in functions.
*   Parameter handling will use stored procedure input parameters and BigQuery's `IFNULL` or `COALESCE` for defaults.
*   Logging and job control will involve `INSERT` and `UPDATE` statements to the dedicated BigQuery job control and log tables.
*   Error handling will use `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks and `SIGNAL SQLSTATE` for explicit error reporting.
*   The invocation of `k_ausd_bp_ta_bpr_optionen.ksh` will become a `CALL` to its corresponding BigQuery Stored Procedure.

## 5. Transformation Logic
The transformation from KornShell to BigQuery SQL/Stored Procedures will involve the following mappings:

*   **Parameter Handling:**
    *   KornShell `getopts` arguments (`-s`, `-l`) will become `IN` parameters for the BigQuery Stored Procedure (e.g., `p_stichtag STRING`, `p_wiederanlaufWert INT64`).
    *   Defaulting logic (`if [[ -z "$p_stichtag" ]]`) will be replaced by `IFNULL(p_stichtag, v_sysdate)` in BigQuery.
    *   Parameter validation (`pruefeParameterGesetzt`) will be implemented using `IF ... THEN SIGNAL SQLSTATE` constructs or `ASSERT` statements for explicit error handling.
*   **Date Operations:**
    *   KornShell `DWDate_Gib_Zeitraum` and `v_sysdate` will be replaced by BigQuery's `CURRENT_DATE()`, `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` for current date and formatting.
*   **Environment Variables:**
    *   Variables like `$HOME`, `BERT_DIR_ROOT` will need to be replaced with hardcoded values (if static), fetched from a configuration table, or passed as procedure parameters. The sourcing of `.dw_init` implies environment setup which should be replicated via BigQuery project/dataset settings or configuration parameters for the stored procedure.
*   **Utility Scripts/Functions:**
    *   Sourced scripts like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh` indicate reusable functions. These functions (e.g., for logging, date manipulation, parameter parsing) will be reimplemented using BigQuery's built-in functions or encapsulated within smaller BigQuery Stored Procedures or UDFs if they represent complex, reusable logic.
*   **Job Control and Logging (`DWMSG_...` functions):**
    *   These shell functions will be replaced by `INSERT` and `UPDATE` statements against the `job_control` and `job_log` BigQuery tables. This includes generating new job entry numbers, logging start/end messages, and updating job status.
*   **Error Handling (`trap`, `exit`, `DWMSG_Fehlerbehandlung`):**
    *   BigQuery Stored Procedures will use `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks to catch errors.
    *   `SIGNAL SQLSTATE '45000'` will be used to raise custom errors, mimicking the `exit` behavior with error codes.
    *   Error messages and details will be inserted into the `job_log` table.
*   **Core Script Invocation:**
    *   The execution of `${Name_Kernskript}` (`k_ausd_bp_ta_bpr_optionen.ksh`) will be replaced by a `CALL` statement to the corresponding BigQuery Stored Procedure: `CALL project.dataset.k_ausd_bp_ta_bpr_optionen(JobKennung, v_stichtag, DW_EintragsNr, v_wiederanlaufWert);`.
*   **No Direct Data Extraction/Ingestion/Transformation in Wrapper:** The analysis confirms this wrapper script primarily handles orchestration and does not contain direct data extraction, ingestion, or complex transformations. These are delegated to the core script, which will need its own migration design.

## 6. External Dependencies
The `lineage_external_systems` analysis for this specific job returned no external systems. However, the source code analysis reveals several implicit dependencies on local file system scripts that serve as external utilities:

*   `$HOME/.dw_init`: An environment initialization script.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling utilities.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing utilities.
*   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utilities.
*   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_optionen.ksh`: The core provisioning script.

**Replacement Strategy:**

*   **`.dw_init` and environment variables:** Environment settings will be handled via:
    *   BigQuery Stored Procedure parameters for dynamic values.
    *   Configuration tables in BigQuery for static values.
    *   Explicit `DECLARE` statements within the stored procedure.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** The logic within these scripts will be re-implemented directly within the BigQuery Stored Procedure using native BigQuery SQL constructs, BigQuery UDFs (if the logic is complex and reusable), or smaller, focused BigQuery Stored Procedures for modularity. Specifically:
    *   Error handling will use `BEGIN...EXCEPTION` blocks and logging tables.
    *   Parameter parsing will be handled by the stored procedure's input parameters.
    *   Date manipulation will use BigQuery's rich set of date and time functions.
*   **`k_ausd_bp_ta_bpr_optionen.ksh` (Core Provisioning Script):** This is the most significant dependency. This script, which contains the actual business logic for data provisioning, will need to be migrated to its own BigQuery Stored Procedure or a BigQuery ETL job (e.g., using SQL or Dataflow/PySpark) and then invoked via a `CALL` statement from the migrated wrapper stored procedure. The current design assumes this will be a callable BigQuery component.

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_bp_ta_bpr_optionen.ksh`) Logic:** The content of the core provisioning script is unknown in this context. Its migration strategy (BigQuery SQL, Dataflow, etc.) and any external dependencies it may have need to be assessed separately. The current design only covers the wrapper script's orchestration.
*   **`DW_EintragsNr` Generation:** The `DWMSG_ErmittleNr` function determines `DW_EintragsNr`. In the BigQuery pseudocode, this is approximated by `SELECT IFNULL(MAX(job_nr), 0) + 1 FROM ...`. This implies a central job control table, and the concurrency of `job_nr` assignment needs careful consideration (e.g., using sequences or transactions for atomicity) in a production BigQuery environment.
*   **Detailed Log File Content:** The shell script writes extensive log output to a file. While a BigQuery `job_log` table will capture messages, ensuring an exact replication of the log format and all specific messages (including standard output from the invoked core script) might require detailed mapping.
*   **`BERT_DIR_ROOT` Definition:** The `BERT_DIR_ROOT` environment variable is crucial for locating other scripts. Its definition and how it will be mapped to a BigQuery context (e.g., via a configuration parameter or constant) needs to be finalized.
*   **`semi_auto` Migration Bucket:** The `semi_auto` designation suggests that while much of the conversion can be automated, manual intervention or refinement will be necessary. This likely stems from the environmental sourcing and custom shell functions which require careful re-implementation rather than direct 1:1 translation.

## 8. Build Plan
The migration build plan for `r_ausd_bp_ta_bpr_optionen.ksh` to BigQuery involves the following steps:

1.  **Define BigQuery Schemas:**
    *   Create the `job_control` table schema (`job_nr`, `job_kennung`, `script_name`, `log_file`, `stichtag_info`, `status`, `created_at`, `finished_at`).
    *   Create the `job_log` table schema (`job_nr`, `job_kennung`, `log_level`, `message`, `created_at`).
2.  **Migrate Utility Logic to BigQuery:**
    *   Reimplement the logic of `h_alis_date.ksh` (e.g., `DWDate_Gib_Zeitraum`) using BigQuery's native date/time functions within the main stored procedure or as separate UDFs.
    *   Reimplement parameter validation (`pruefeParameterGesetzt`) logic using `IF` statements and `SIGNAL SQLSTATE`.
    *   Reimplement error logging (`f_alis_msgerr.ksh`, `DWMSG_Fehlerbehandlung`) by writing to the `job_log` table.
3.  **Develop BigQuery Stored Procedure for Wrapper:**
    *   Write the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` based on the provided BigQuery SQL pseudocode.
    *   Implement parameter handling, defaulting, and validation.
    *   Integrate job control and logging logic using `INSERT` and `UPDATE` on the `job_control` and `job_log` tables.
    *   Include the `BEGIN...EXCEPTION WHEN ERROR THEN...END` block for robust error handling.
4.  **Migrate Core Script (`k_ausd_bp_ta_bpr_optionen.ksh`):** (This is a separate, dependent workstream)
    *   Analyze `k_ausd_bp_ta_bpr_optionen.ksh` to determine its migration path (e.g., BigQuery SQL Stored Procedure, Dataflow/PySpark, etc.).
    *   Develop the corresponding BigQuery component (e.g., `project.dataset.k_ausd_bp_ta_bpr_optionen`).
5.  **Integrate Core Script Call:**
    *   Once the core script is migrated, update the wrapper stored procedure to `CALL` the new BigQuery component.
6.  **Deployment and Orchestration:**
    *   Deploy the BigQuery tables and stored procedures.
    *   Configure a BigQuery scheduling mechanism (e.g., `bq scheduled_queries`) or an external orchestrator (e.g., Cloud Composer DAG, Cloud Workflows) to invoke `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` with the required parameters.
    *   Define any necessary deployment configuration for dataset/project identifiers, and IAM permissions.

**Language for generated files:**
*   BigQuery SQL for tables, stored procedures, and UDFs.
*   Python (for Cloud Composer DAGs) or YAML (for Cloud Workflows) if external orchestration is required.