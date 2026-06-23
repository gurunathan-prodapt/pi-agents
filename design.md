# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh

## 1. Purpose & Scope
This migration job, identified by `run_id: 5af228f1-3847-4cc6-9310-ed82ed19407c`, is responsible for the initial provisioning and snapshot extraction of the contract cache for the Forderungsscoring (FOS) system. The original script, `r_ausd_geschaeftspartner.ksh`, is a KornShell wrapper that orchestrates the execution of a core business logic script (`k_ausd_geschaeftspartner.ksh`). Its primary function is to:
*   Parse input parameters for a reference date (`Stichtag`) and a restart value (`Wiederanlaufwert`).
*   Handle logging and error management.
*   Invoke the core data processing script with the prepared parameters.
*   Ensure restartability by handling records based on a `DWH_VERTRAG_ID` threshold.

The scope of this migration is to re-implement this orchestration logic and the underlying data processing in Google Cloud's BigQuery, leveraging BigQuery Stored Procedures and tables for equivalent functionality.

## 2. Source Inventory
The job is comprised of a single KornShell script: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh`.

*   **File Path**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh`
*   **Technology**: KornShell
*   **Purpose**: ETL orchestration (wrapper script)
*   **Complexity Tier**: Medium
*   **Migration Flags**: None identified.
*   **Automation Bucket**: Semi-automated migration (B2)
*   **Summary**: A shell script that serves as a wrapper to initialize and run a data provisioning process, handling parameters, logging, and invoking a core script for the actual data extraction and loading.

## 3. Target Architecture
The target architecture in BigQuery will consist of:
*   **BigQuery Stored Procedures**:
    *   A main stored procedure, `sp_initial_befuellung_vertrags_cache_fos`, to replace the `r_ausd_geschaeftspartner.ksh` wrapper script. This procedure will manage parameter parsing, date defaulting, validation, and logging.
    *   A separate stored procedure, `sp_ausd_geschaeftspartner`, to encapsulate the core data extraction and loading logic originally found in `k_ausd_geschaeftspartner.ksh`.
*   **BigQuery Tables**:
    *   `project.dataset.job_control`: A control table to track job execution status, start/end times, and parameters.
    *   `project.dataset.job_log`: A logging table to store detailed execution logs, replacing filesystem-based logs.
    *   `project.dataset.fos_vertrags_cache`: The target table for the processed contract cache data.
    *   `project.dataset.dwh_vertrag_cache_source`: The source table from which contract cache data is extracted (conceptual, assuming this maps to the original DWH source for `k_ausd_geschaeftspartner.ksh`).

Orchestration of the main BigQuery stored procedure (`sp_initial_befuellung_vertrags_cache_fos`) will be handled by a Google Cloud orchestration service like Cloud Composer (Airflow), Workflows, or Dataform.

## 4. Data Flow & Lineage
The original data flow is:
1.  `r_ausd_geschaeftspartner.ksh` (wrapper script) is executed.
2.  It sources helper scripts for environment setup, parameter parsing, and date functions.
3.  It determines parameters like `Stichtag` (reference date) and `Wiederanlaufwert` (restart value).
4.  It initializes logging and error handling.
5.  It invokes `k_ausd_geschaeftspartner.ksh` (core script) with the processed parameters.
6.  `k_ausd_geschaeftspartner.ksh` performs the actual data extraction from DWH source tables and loads it into the contract cache for FOS.

In the BigQuery target architecture, this flow will be:
1.  An orchestrator (e.g., Cloud Composer DAG) triggers the `sp_initial_befuellung_vertrags_cache_fos` BigQuery stored procedure.
2.  `sp_initial_befuellung_vertrags_cache_fos` handles parameter initialization, validation, and logs its progress to `project.dataset.job_log` and `project.dataset.job_control`.
3.  `sp_initial_befuellung_vertrags_cache_fos` then calls `sp_ausd_geschaeftspartner`.
4.  `sp_ausd_geschaeftspartner` reads data from `project.dataset.dwh_vertrag_cache_source` (or equivalent source tables that `k_ausd_geschaeftspartner.ksh` would have used).
5.  It applies the restart logic (deleting records based on `p_wiederanlaufWert` if applicable) and inserts filtered records into `project.dataset.fos_vertrags_cache`.
6.  Both stored procedures log their status and messages to `project.dataset.job_log`.
7.  Upon completion, `sp_initial_befuellung_vertrags_cache_fos` updates the final job status in `project.dataset.job_control`.

## 5. Transformation Logic
The transformation logic from KornShell to BigQuery SQL Stored Procedures will involve:

**Wrapper Script (`r_ausd_geschaeftspartner.ksh` -> `sp_initial_befuellung_vertrags_cache_fos`):**
*   **Parameter Handling**: Shell `getopts` for `-s` (Stichtag) and `-l` (Wiederanlaufwert) will be replaced by `IN` parameters in the BigQuery stored procedure (`p_stichtag STRING`, `p_wiederanlaufWert INT64`).
*   **Defaulting Logic**:
    *   `p_wiederanlaufWert`: Defaults to `0` if not provided, using `COALESCE(p_wiederanlaufWert, 0)`.
    *   `p_stichtag`: Defaults to `CURRENT_DATE()` if not provided, otherwise `PARSE_DATE('%d%m%Y', p_stichtag)` is used.
*   **Validation**: The `pruefeParameterGesetzt` and `if [ ! $ErrNr -eq 0 ]` logic will be replaced by `IF v_stichtag IS NULL THEN RAISE USING MESSAGE = ... END IF;`.
*   **Logging**: `print`, `tee`, and shell log file redirection will be replaced by `INSERT` statements into the `project.dataset.job_log` table. Job control status will be maintained in `project.dataset.job_control`.
*   **Error Handling**: Shell `trap` mechanisms will be replaced by a `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;` block in the stored procedure, allowing for structured error logging and status updates in `job_log` and `job_control`.
*   **Core Script Invocation**: The shell command `${Name_Kernskript} -j ...` will be replaced by a `CALL project.dataset.sp_ausd_geschaeftspartner(...)` statement.

**Core Script (`k_ausd_geschaeftspartner.ksh` -> `sp_ausd_geschaeftspartner`):**
*   **Restart Logic**: The original script's implied mechanism for handling `Wiederanlaufwert` (deleting and re-inserting records `DWH_VERTRAG_ID > Wiederanlaufwert`) will be explicitly translated into a `DELETE` statement followed by an `INSERT` statement in BigQuery SQL.
    *   `DELETE FROM fos_vertrags_cache WHERE dwh_vertrag_id >= p_wiederanlaufWert AND p_wiederanlaufWert > 0;`
*   **Data Extraction Logic**: The selection criteria described (`Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`) will be directly translated into the `WHERE` clause of the `INSERT INTO ... SELECT` statement.
    *   `INSERT INTO fos_vertrags_cache SELECT src.* FROM dwh_vertrag_cache_source AS src WHERE src.gueltig_von <= v_stichtag AND v_stichtag < src.gueltig_bis AND src.ladedatum < v_stichtag AND (p_wiederanlaufWert = 0 OR src.dwh_vertrag_id > p_wiederanlaufWert);`

## 6. External Dependencies
The original script relies on several internal shell-based dependencies which, in the context of BigQuery, become "external" from the SQL perspective.

| Original Dependency (Type)                                       | Replacement in BigQuery                                         | Notes                                                                                                                                                                                                                                                                                                        |
| :--------------------------------------------------------------- | :-------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `$HOME/.dw_init` (Environment Initialization Script)             | BigQuery session parameters or configuration tables             | Environment variables and configurations typically set by `dw_init` will be managed either as BigQuery stored procedure variables, session variables, or entries in dedicated BigQuery configuration tables.                                                                                                        |
| `f_alis_msgerr.ksh` (Error Handling Framework)                   | `project.dataset.job_log` table and `EXCEPTION` blocks          | Shell-based error reporting and logging will be replaced by `INSERT` statements into a `job_log` table within BigQuery stored procedures. Structured `EXCEPTION` handling will manage control flow.                                                                                                                |
| `h_alis_parameter.ksh` (Parameter Parsing Helper)                | BigQuery stored procedure `IN` parameters and `IF` conditions   | The shell script's parameter parsing logic will be directly integrated into the BigQuery stored procedure's input arguments and conditional logic.                                                                                                                                                             |
| `h_alis_date.ksh` (Date Helper)                                  | BigQuery native date/timestamp functions (`CURRENT_DATE()`, `PARSE_DATE`, `FORMAT_DATE`) | Shell date calculations and formatting will be replaced by equivalent BigQuery SQL functions, ensuring consistent date manipulation.                                                                                                                                                                             |
| `k_ausd_geschaeftspartner.ksh` (Core Business Logic Script)     | `project.dataset.sp_ausd_geschaeftspartner` (Stored Procedure)  | The actual data processing logic contained within this invoked shell script will be migrated into a dedicated BigQuery stored procedure, called by the main wrapper procedure. The internal dependencies of this script (e.g. source tables) will map to BigQuery tables.                                            |
| `DWMSG_*`, `pruefeParameterGesetzt`, `DWDate_Gib_Zeitraum` (Shell Functions) | Custom BigQuery SQL logic / `job_log` / `job_control` table     | These custom shell functions will be re-implemented directly in BigQuery SQL using standard SQL constructs (e.g., `SELECT COALESCE(MAX(...), 0) + 1` for `DW_EintragsNr`, `INSERT` for `DWMSG_ErzeugeEintrag`).                                                                                                   |
| Filesystem logging (`>> $LogDatei 2>&1`)                         | `INSERT` statements into `project.dataset.job_log`              | All log messages previously written to a file will now be inserted as rows into a structured BigQuery logging table, enabling easier querying and monitoring.                                                                                                                                                    |

## 7. Unresolved / Risks
*   **Detailed Logic of `k_ausd_geschaeftspartner.ksh`**: The migration tool provided a placeholder for `sp_ausd_geschaeftspartner`. The exact SQL for data extraction and transformation within this core script is not present in the provided analysis of `r_ausd_geschaeftspartner.ksh`. This will require separate analysis of `k_ausd_geschaeftspartner.ksh` to fully define the `INSERT` and `DELETE` logic for the target `fos_vertrags_cache` table. This is a crucial step to ensure all business rules are correctly translated.
*   **Environment Initialization (`. $HOME/.dw_init`)**: The `.dw_init` script might set complex environment variables or execute commands with side effects beyond simple parameter assignment. These deeper implications need to be thoroughly investigated and replicated using BigQuery parameters, configuration tables, or potentially external Python scripts if complex OS-level interactions are present.
*   **KornShell Specifics**: While the provided design covers most aspects, subtle nuances of KornShell behavior (e.g., specific string manipulations, advanced `trap` scenarios, or external command execution not fully captured) might require careful review during implementation and testing.
*   **Performance**: The performance characteristics of the shell script and its called components might differ significantly from the BigQuery implementation. Query optimization for `sp_ausd_geschaeftspartner` will be essential.

## 8. Build Plan
The migration build plan involves creating the necessary BigQuery assets in the following order:

1.  **Define BigQuery Dataset**: Ensure `project.dataset` exists in BigQuery.
2.  **Create Logging and Control Tables**:
    *   Create `project.dataset.job_log` table (e.g., `job_nr INT64, job_kennung STRING, log_level STRING, message STRING, created_at TIMESTAMP`).
    *   Create `project.dataset.job_control` table (e.g., `job_nr INT64, job_kennung STRING, stichtag DATE, resume_value INT64, status STRING, created_at TIMESTAMP, finished_at TIMESTAMP`).
3.  **Create Target Data Table**:
    *   Create `project.dataset.fos_vertrags_cache` table with the appropriate schema (requires schema definition from the original target of `k_ausd_geschaeftspartner.ksh`).
4.  **Create Source Data Table (if not already existing)**:
    *   Ensure `project.dataset.dwh_vertrag_cache_source` (or the actual source tables) exists and contains the necessary data.
5.  **Develop Core Logic Stored Procedure**:
    *   Translate the logic of `k_ausd_geschaeftspartner.ksh` into `CREATE OR REPLACE PROCEDURE project.dataset.sp_ausd_geschaeftspartner(...)`. This will include `DELETE` and `INSERT` statements with the data transformation rules.
6.  **Develop Wrapper Stored Procedure**:
    *   Create `CREATE OR REPLACE PROCEDURE project.dataset.sp_initial_befuellung_vertrags_cache_fos(...)` based on the provided pseudocode, incorporating parameter handling, logging, error handling, and the `CALL` to `sp_ausd_geschaeftspartner`.
7.  **Develop Orchestration**:
    *   Create an orchestration script (e.g., an Airflow DAG in Cloud Composer) to schedule and execute `CALL project.dataset.sp_initial_befuellung_vertrags_cache_fos()`, passing the required parameters.
8.  **Testing**: Comprehensive unit, integration, and user acceptance testing will be required to validate the migrated logic against the legacy system's output.