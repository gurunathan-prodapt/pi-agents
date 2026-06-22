# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_bp_ta_bpr_optionen.ksh`. This script serves as an orchestrator for the initial provisioning of selected basic products for BERT. Its primary function is to handle parameter parsing, determine the execution date (`Stichtag`), set up error handling, manage logging, and then invoke a core KornShell script, `k_ausd_bp_ta_bpr_optionen.ksh`, which contains the actual data processing logic. The overall job is identified as having medium complexity.

The scope of this migration design specifically covers the wrapper/orchestration script (`r_ausd_bp_ta_bpr_optionen.ksh`). The migration of the core business logic within `k_ausd_bp_ta_bpr_optionen.ksh` will require a separate, detailed design.

## 2. Source Inventory
The job consists of a single primary source file:

| File Name                                                              | Technology | Tier   | Automation Bucket |
| :--------------------------------------------------------------------- | :--------- | :----- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh` | KornShell  | Medium | Semi-Automated    |

## 3. Target Architecture
The target platform for this migration is Google BigQuery. The orchestration logic currently implemented in the KornShell script will be converted into a BigQuery Stored Procedure.

The target architecture will include:
- **BigQuery Stored Procedure (`ausd_bp_ta_bpr_optionen_wrapper`):** This procedure will encapsulate the parameter handling, date defaulting, error management, and logging initiation of the original shell script. It will then `CALL` a separate BigQuery stored procedure for the core business logic.
- **BigQuery Logging Table (`project.dataset.job_log`):** A dedicated BigQuery table to store job execution logs, status updates, and error messages, replacing the file-based logging of the source script.
- **BigQuery Job Status Table (`project.dataset.job_status`):** A BigQuery table to track the overall status of job runs.
- **Core Business Logic Procedure:** A separate BigQuery Stored Procedure (e.g., `k_ausd_bp_ta_bpr_optionen`) will house the migrated logic from the `k_ausd_bp_ta_bpr_optionen.ksh` script.
- **External Orchestration:** An external orchestrator such as Cloud Composer (Airflow), Google Cloud Workflows, or BigQuery Scheduled Queries will be used to invoke the `ausd_bp_ta_bpr_optionen_wrapper` procedure, passing necessary parameters.

## 4. Data Flow & Lineage
The `r_ausd_bp_ta_bpr_optionen.ksh` script does not directly process data but manages the execution flow.

**Execution Flow:**
1. **Invocation:** The wrapper script/procedure is invoked with optional parameters (`-s Stichtag`, `-l Wiederanlaufwert`).
2. **Parameter Processing:** The script parses input parameters. It defaults the `Stichtag` to the current system date if not provided and `Wiederanlaufwert` to `0` if unset.
3. **Initialization:** Environment variables are sourced, and logging/error handling frameworks are prepared. A job entry is created in the logging system.
4. **Core Logic Invocation:** The script calls the core business logic script, `k_ausd_bp_ta_bpr_optionen.ksh`, passing the determined parameters.
5. **Status Update & Exit:** Upon successful completion of the core script, a success message is logged, and the job status is updated. If an error occurs, error handling routines are triggered, logging the failure and exiting with an error code.

**Inferred Dependencies (from source code):**
- **Environment Initialization:** `. $HOME/.dw_init`
- **Error/Logging Framework:** `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
- **Parameter Helper:** `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
- **Date Helper:** `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
- **Core Business Logic Script:** `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_optionen.ksh`

## 5. Transformation Logic
The transformation will convert KornShell constructs into BigQuery SQL scripting capabilities, primarily within a BigQuery Stored Procedure.

-   **Parameter Handling:**
    -   `getopts` and shell variable assignments (`p_stichtag=$OPTARG`, `p_wiederanlaufWert=$OPTARG`) will be replaced by `IN` parameters of the BigQuery Stored Procedure.
    -   Defaulting logic (`if [[ -z "$p_wiederanlaufWert" ]]`, `if [[ -z "$p_stichtag" ]]`) will use `IFNULL` and `NULLIF` with `DECLARE` and `SET` statements.
-   **Date Determination:**
    -   Shell functions like `DWDate_Gib_Zeitraum` and system date calls will be replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()` functions.
-   **Error Handling and Control Flow:**
    -   `set -e` will be implicitly handled by BigQuery's transaction model and explicit error handling.
    -   Shell `trap` statements will be translated into BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks to catch and manage errors.
    -   Error codes (`ErrNr`, `ErrArg`) and messages will be stored in the BigQuery logging table. `SIGNAL SQLSTATE` will be used for explicit error termination.
    -   Conditional checks (`if [ ! $ErrNr -eq 0 ]`) will be translated to `IF ... THEN ... END IF;` statements.
-   **Logging:**
    -   Shell `print` and `tee` commands (`print "..."`, `DWMSG_ErzeugeEintrag ... >> $LogDatei 2>&1`) will be replaced by `INSERT` statements into the `project.dataset.job_log` table.
    -   Job status updates (`DWMSG_SetzeStatusOK`) will correspond to `UPDATE` statements on the `project.dataset.job_status` or `job_log` table.
-   **Environment Sourcing & Helper Scripts:**
    -   `. $HOME/.dw_init`: This will need to be analyzed. Configuration values will be migrated to BigQuery procedure parameters, BigQuery lookup tables, or hardcoded constants within the procedure.
    -   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: The functionalities of these helper scripts (e.g., `pruefeParameterGesetzt`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_MeldeFehler`) will be reimplemented as BigQuery UDFs, helper procedures, or directly as BigQuery SQL logic within the main wrapper procedure.
-   **Core Script Invocation:**
    -   The line `${Name_Kernskript} -j $JobKennung -s $p_stichtag -f ${DW_EintragsNr} -l ${p_wiederanlaufWert} >> $LogDatei 2>&1` will be replaced by a `CALL` statement to the migrated BigQuery stored procedure for `k_ausd_bp_ta_bpr_optionen.ksh`.

## 6. External Dependencies
The original script directly sources other shell scripts and relies on a shell environment. The `lineage_assembled_jobs` record indicated no explicit external systems.

-   **Shell Environment Variables:** `$HOME`, `$BERT_DIR_ROOT`.
    -   **Replacement:** These will be replaced by explicit parameters to the BigQuery stored procedure, entries in a BigQuery configuration table, or constants within the procedure code.
-   **Sourced Shell Scripts:** `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `.dw_init`.
    -   **Replacement:** The functionalities provided by these scripts (e.g., parameter validation, date manipulation, error logging) will be reimplemented using BigQuery SQL scripting features, UDFs, or integrated directly into the main stored procedure.
-   **Invoked Core Script:** `k_ausd_bp_ta_bpr_optionen.ksh`.
    -   **Replacement:** This script's logic will be fully migrated to its own BigQuery stored procedure, which will then be called by the `ausd_bp_ta_bpr_optionen_wrapper` procedure.

## 7. Unresolved / Risks
-   **Core Business Logic Complexity:** The details of `k_ausd_bp_ta_bpr_optionen.ksh` (the "core script") are currently unknown. Its migration design and complexity are crucial and represent the largest unknown factor. This is considered a **critical dependency** for the overall job migration.
-   **Custom Shell Functions:** The exact implementation of `DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, and the `DWMSG_*` functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK`) needs to be thoroughly understood to ensure accurate translation to BigQuery.
-   **Environment Variables from `.dw_init`:** The full impact and contents of `$HOME/.dw_init` need to be analyzed to ensure all necessary environment configurations are correctly replicated or replaced in the BigQuery environment.
-   **Error Handling Fidelity:** Replicating the exact behavior of shell `trap` (e.g., `INT`, `STOP`, `CONT`) using BigQuery's `EXCEPTION` handling may require careful consideration to ensure equivalent robustness.
-   **Logging Format:** While the concept of logging to a table is clear, the exact schema and data types for the `job_log` table need to align with the information captured in the original shell script's log outputs.

## 8. Build Plan
1.  **Analyze and Design Core Script (`k_ausd_bp_ta_bpr_optionen.ksh`):**
    *   **Description:** Thoroughly analyze the core script to understand its data transformation logic, source/target tables, and business rules. Design its migration to a BigQuery Stored Procedure.
    *   **Language:** Design Document, BQSQL
2.  **Define BigQuery Schema for Logging and Status:**
    *   **Description:** Create the Data Definition Language (DDL) for `project.dataset.job_log` (to store job execution logs) and `project.dataset.job_status` (for overall job status tracking).
    *   **Language:** DDL
3.  **Implement Helper Logic in BigQuery:**
    *   **Description:** Translate the functionalities of the shell helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) and custom functions (`DWDate_Gib_Zeitraum`, `pruefeParameterGesetzt`, `DWMSG_*`) into BigQuery SQL UDFs or integrated procedural logic.
    *   **Language:** BQSQL
4.  **Develop `k_ausd_bp_ta_bpr_optionen` Stored Procedure:**
    *   **Description:** Implement the BigQuery Stored Procedure corresponding to the core business logic of `k_ausd_bp_ta_bpr_optionen.ksh` as per its dedicated design.
    *   **Language:** BQSQL
5.  **Develop `ausd_bp_ta_bpr_optionen_wrapper` Stored Procedure:**
    *   **Description:** Implement the main orchestration stored procedure in BigQuery. This will include parameter parsing, date defaulting, calling the `k_ausd_bp_ta_bpr_optionen` procedure, and managing logging and error handling.
    *   **Language:** BQSQL
6.  **Configure External Orchestration:**
    *   **Description:** Set up Cloud Composer (Airflow), Google Cloud Workflows, or BigQuery Scheduled Queries to invoke the `project.dataset.ausd_bp_ta_bpr_optionen_wrapper` BigQuery stored procedure with the required parameters.
    *   **Language:** Python (for Airflow DAGs), YAML (for Workflows), or BigQuery Scheduled Query Configuration.