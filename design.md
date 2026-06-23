# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh

## 1. Purpose & Scope
This job, `r_ausd_v_ta_notice.ksh`, is an ETL workflow designed for the reconciliation of contract data specifically for the `ta_notice` table. It acts as a wrapper script, orchestrating the execution of a core data processing logic. Its primary responsibilities include handling command-line parameters, initializing the execution environment, managing error logging, and updating job status. The core data manipulation involves truncating and inserting data into the `ta_notice` table based on a source table, applying specific date and production flag filters.

## 2. Source Inventory

| File Path                                                                     | Technology   | Tier (Assumed) | Automation Bucket (Assumed) | Summary                                                                                                                                                                                                                                                        |
|:------------------------------------------------------------------------------|:-------------|:---------------|:----------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh` | KornShell    | Medium         | Semi-Auto                   | KornShell script acting as a wrapper/orchestrator for a core data processing script, handling parameter parsing, error logging, and job status management for the ta_notice table.                                                                            |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh` | KornShell    | Medium         | Semi-Auto                   | This ksh script acts as a control script for `r_ausd_vertrag.ksh`, handling job activation/deactivation, parsing parameters, and orchestrating the execution of an SQL script `d_ausd_v_ta_notice.sql`.                                                         |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_notice.sql` | Oracle SQL*Plus | Complex        | Semi-Auto                   | This SQL*Plus script truncates a target table and then inserts data into it from a source table, applying filters based on dates and a production flag. It utilizes a database link and SQL*Plus substitution variables for dynamic elements.                 |

*Note: Complexity tiers and automation buckets are assumed due to missing data from `file_complexity` and `automation_rate` tables.*

## 3. Target Architecture
The migration to BigQuery will involve transforming the shell scripts into BigQuery Stored Procedures for orchestration and parameter handling, and the Oracle SQL*Plus script into BigQuery SQL.

*   **Orchestration Layer**: The `r_ausd_v_ta_notice.ksh` and `k_ausd_v_ta_notice.ksh` scripts will be converted into BigQuery Stored Procedures (e.g., `project.dataset.sp_r_ausd_v_ta_notice` and `project.dataset.sp_k_ausd_v_ta_notice_core`). These procedures will manage job parameters, control flow, and error handling using BigQuery scripting features.
*   **Data Processing Layer**: The `d_ausd_v_ta_notice.sql` script will be translated into a BigQuery SQL statement or another BigQuery Stored Procedure that performs the `TRUNCATE` and `INSERT` operations.
*   **Logging and Control**: Dedicated BigQuery tables (e.g., `project.dataset.job_log`, `project.dataset.job_control`, `project.dataset.error_log`) will replace the shell script's file-based logging and job status management. Helper stored procedures (`sp_dwmsg_ermittle_nr`, `sp_dwmsg_logdateiname`, etc.) will abstract these logging and control functions.
*   **Data Sources**: Oracle tables like `cds$ta_notice` and `isbert_schema.dwtk_meldungen` will be ingested into BigQuery native tables, or accessed via BigQuery external tables or federated queries if real-time access to the source Oracle system is required. The target `sof$ta_notice` will become a native BigQuery table.

## 4. Data Flow & Lineage
The original ETL flow is a hierarchical invocation of scripts:
1.  **`r_ausd_v_ta_notice.ksh`**: The top-level wrapper script, responsible for overall job setup and teardown.
2.  **`k_ausd_v_ta_notice.ksh`**: Invoked by `r_ausd_v_ta_notice.ksh`, this acts as a control script, handling specific job parameters (`-j`, `-f`) and then calling the core SQL logic.
3.  **`d_ausd_v_ta_notice.sql`**: Invoked by `k_ausd_v_ta_notice.ksh` (via a helper function `starteSQLSkript`), this is the data processing component.
    *   **Reads From**:
        *   `isbert_schema.dwtk_meldungen`: To determine the `v_datum` (reference date).
        *   `cds$ta_notice@pcrs1`: The primary source table for contract notice data, accessed via an Oracle DB link.
    *   **Writes To**:
        *   `sof$ta_notice`: The target table where the processed contract notice data is stored.

In BigQuery, this flow will be implemented as a chain of stored procedure calls. The main orchestration procedure (`sp_r_ausd_v_ta_notice`) will call the control procedure (`sp_k_ausd_v_ta_notice_core`), which in turn will execute the core data manipulation logic (either directly as SQL or via another dedicated stored procedure).

## 5. Transformation Logic

### 5.1. `r_ausd_v_ta_notice.ksh` to `sp_r_ausd_v_ta_notice`
*   **Parameter Handling**: Shell `getopts` will be replaced by BigQuery Stored Procedure input parameters (e.g., `param_h`, `param_s`, `param_l`).
*   **Environment Sourcing**: `. $HOME/.dw_init` and other sourced utility scripts will be replaced by BigQuery configuration parameters, variables, or calls to dedicated BigQuery helper procedures.
*   **Error Handling**: Shell `set -eu` and `trap` commands will be replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks for robust error management within the stored procedure. Custom `DWMSG_*` functions will map to inserts into a BigQuery logging table and calls to helper logging procedures.
*   **Logging**: `print` and `tee -a $LogDatei` commands will be replaced by `INSERT` statements into BigQuery logging tables (`project.dataset.job_log`).
*   **Invocation**: The shell execution `"${Name_Kernskript}" -j $JobKennung -f ${DW_EintragsNr}` will translate into a BigQuery `CALL` statement to `project.dataset.sp_k_ausd_v_ta_notice_core` with appropriate parameters.

### 5.2. `k_ausd_v_ta_notice.ksh` to `sp_k_ausd_v_ta_notice_core`
*   **Parameter Handling**: Shell `getopts` for `-j p_JobKennung` and `-f p_EintragsNr` will be translated to BigQuery Stored Procedure input parameters.
*   **Validation**: Shell `pruefeParameterGesetzt` calls will become `IF` conditions checking for `NULL` or empty string values of the input parameters, followed by appropriate error logging and `ASSERT FALSE` or `SIGNAL SQLSTATE` for termination.
*   **SQL Script Execution**: The `starteSQLSkript` helper function will be replaced by direct execution of the BigQuery SQL for `d_ausd_v_ta_notice.sql` within this procedure, or by calling another BigQuery Stored Procedure that encapsulates the `d_ausd_v_ta_notice.sql` logic.
*   **Temporary Files**: The `tmpFile` used to store record counts will be replaced by BigQuery variables or temporary tables, with the final count stored in a job result table.

### 5.3. `d_ausd_v_ta_notice.sql` to BigQuery SQL
*   **Variable Definition**: Oracle `DEFINE v_carmen = "@pcrs1"` and `COLUMN s_datum new_value v_datum noprint` will be replaced by BigQuery `DECLARE` statements. The `v_datum` calculation will be an inline `SELECT` statement within the `DECLARE` block.
*   **DB Link**: The `@pcrs1` DB link will require careful migration. Options include:
    *   **Data Ingestion**: Periodically ingest `cds$ta_notice` into a BigQuery native table.
    *   **Federated Queries**: Use BigQuery federated queries if direct, real-time access to the Oracle source is needed.
    *   **External Tables**: Create an external table in BigQuery pointing to the Oracle table via a connector if available.
*   **`NVL` and `TO_CHAR/TO_DATE`**: Oracle functions like `NVL` and `TO_CHAR/TO_DATE` will be converted to their BigQuery equivalents, `COALESCE` and `FORMAT_DATE`/`PARSE_DATE` respectively, with appropriate format strings.
*   **TRUNCATE**: The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_notice')` will be replaced by a direct BigQuery `TRUNCATE TABLE \`project.dataset.sof_ta_notice\`` statement.
*   **INSERT Logic**: The `INSERT INTO ... SELECT FROM ... WHERE` statement will be directly translatable to BigQuery SQL, with appropriate table names and schema prefixes. Filters involving `insert_at`, `modified_at`, `valid_to`, and `is_production` will remain logically equivalent.
*   **`COMMIT` / `SPOOL`**: Explicit `COMMIT` and `SPOOL` commands from SQL*Plus are not directly applicable in BigQuery's transactional model and logging, and will be removed or replaced by logging statements.

## 6. External Dependencies

| Original External Dependency                     | Type      | BigQuery Migration Strategy                                                                                                                                                                                                                                                                                                                               |
|:-------------------------------------------------|:----------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cds$ta_notice@pcrs1`                            | Database  | **Data Ingestion / Federated Query / External Table**: `cds$ta_notice` will need to be made available in BigQuery. This can be achieved by ingesting the data into a native BigQuery table, setting up a BigQuery federated query to directly access Oracle, or using an external table if a suitable connector exists. The `@pcrs1` DB link will be removed. |
| `isbert_schema.dwtk_meldungen`                   | Database  | **BigQuery Native Table**: This metadata table should be migrated to a BigQuery native table (e.g., `project.dataset.dwtk_meldungen`) and referenced directly in BigQuery SQL queries.                                                                                                                                                               |
| `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`    | Procedure | **BigQuery Stored Procedure**: The functionality of `runstatement` (specifically `TRUNCATE TABLE`) will be replaced by direct BigQuery DDL or encapsulated in a dedicated BigQuery stored procedure.                                                                                                                                                  |
| `$HOME/.dw_init`                                 | File      | **Configuration Parameters / Variables**: Environmental configurations will be replaced by BigQuery procedure parameters, BigQuery variables, or values retrieved from a dedicated BigQuery configuration table.                                                                                                                                      |
| `${BERT_DIR_ROOT}/allgemein/is/util/bin/*`       | Files     | **BigQuery Stored Procedures / Helper Functions**: Helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) will be reimplemented as BigQuery stored procedures or integrated into the main orchestration procedures.                                                                               |
| Log files (e.g., `./tmp/trace_d_ausd_v_ta_notice`) | File      | **BigQuery Logging Tables**: All file-based logging will be redirected to BigQuery native tables (e.g., `project.dataset.job_log`, `project.dataset.error_log`) for centralized and searchable logging.                                                                                                                                         |
| `FUNCTION:DWMSG_ERMITTLENR`                      | Function  | **BigQuery Stored Procedure**: The `DWMSG_ERMITTLENR` function, identified as a send_mail type function from lineage, indicates a notification mechanism. This will be replaced by a BigQuery stored procedure that either sends emails via Cloud Functions/Pub/Sub or logs notifications to a dedicated table.                                 |

## 7. Unresolved / Risks
*   **Missing Complexity/Automation Data**: The absence of `file_complexity` and `automation_rate` data required assumptions about the migration effort. A more precise assessment would benefit from this information.
*   **Oracle-Specific Features**: The `d_ausd_v_ta_notice.sql` script uses Oracle SQL*Plus specific commands (`WHENEVER SQLERROR`, `START ../trace.sql.cfg`, `SPOOL`, `COLUMN ... NEW_VALUE`) which have no direct BigQuery equivalents and require careful re-architecting into BigQuery scripting or external services.
*   **DB Link Performance**: The performance implications of migrating the `cds$ta_notice@pcrs1` DB link must be thoroughly evaluated. Federated queries might introduce latency, while data ingestion requires a robust data pipeline.
*   **Dynamic Script Paths**: The use of `BERT_DIR_ROOT` for dynamic script paths needs to be resolved into static BigQuery procedure names or configurable parameters.
*   **`starteSQLSkript` Implementation**: The exact logic within `h_alis_sqlplus.ksh` and `starteSQLSkript` regarding how the SQL is executed (e.g., error handling during SQL execution, output parsing) needs to be fully understood to accurately replicate in BigQuery.
*   **Job Control Semantics**: The "active jobs ignored" and "old active jobs deactivated" logic mentioned in `k_ausd_v_ta_notice.ksh` summary must be precisely replicated in the BigQuery control procedures, likely involving BigQuery metadata tables for job status.

## 8. Build Plan
The migration will follow a component-by-component build strategy:

1.  **Define BigQuery Schemas**:
    *   `project.dataset.job_log`: For general job logging.
    *   `project.dataset.job_control`: For job status and metadata.
    *   `project.dataset.error_log`: For error messages.
    *   `project.dataset.dwtk_meldungen`: BigQuery native table for `isbert_schema.dwtk_meldungen`.
    *   `project.dataset.cds_ta_notice`: BigQuery native table or external table definition for `cds$ta_notice`.
    *   `project.dataset.sof_ta_notice`: BigQuery native table for the target `sof$ta_notice`.

2.  **Develop BigQuery Helper Stored Procedures (Python/SQL)**:
    *   `project.dataset.sp_dwmsg_ermittle_nr`: Replicate `DWMSG_ErmittleNr` logic for generating job entry numbers.
    *   `project.dataset.sp_dwmsg_logdateiname`: Replicate `DWMSG_Logdateiname` logic for constructing log file names (will insert into `job_log`).
    *   `project.dataset.sp_dwmsg_erzeuge_eintrag`: Replicate `DWMSG_ErzeugeEintrag` for creating job entries.
    *   `project.dataset.sp_dwmsg_setze_stichtag_info`: Replicate `DWMSG_SetzeStichtagInfo` for setting reference dates.
    *   `project.dataset.sp_dwmsg_fehlerbehandlung`: Replicate `DWMSG_Fehlerbehandlung` for error status updates.
    *   `project.dataset.sp_dwmsg_setze_status_ok`: Replicate `DWMSG_SetzeStatusOK` for successful job status updates.
    *   Procedures for handling notification (`DWMSG_ERMITTLENR`).

3.  **Migrate Core SQL Logic (BigQuery SQL)**:
    *   Create a BigQuery SQL script or stored procedure (e.g., `project.dataset.sp_d_ausd_v_ta_notice_sql`) corresponding to `d_ausd_v_ta_notice.sql`. This will include:
        *   `DECLARE` statements for `v_datum`.
        *   `TRUNCATE TABLE \`project.dataset.sof_ta_notice\``
        *   `INSERT INTO \`project.dataset.sof_ta_notice\` ... SELECT ... FROM \`project.dataset.cds_ta_notice\` ... WHERE ...` statement.

4.  **Migrate Control Script Logic (BigQuery Stored Procedure)**:
    *   Create `project.dataset.sp_k_ausd_v_ta_notice_core` (BigQuery Stored Procedure) corresponding to `k_ausd_v_ta_notice.ksh`. This will handle:
        *   Input parameters `p_JobKennung`, `p_EintragsNr`.
        *   Parameter validation.
        *   Calls to `project.dataset.sp_d_ausd_v_ta_notice_sql`.
        *   Updates to logging/job control tables for record counts.

5.  **Migrate Wrapper Script Logic (BigQuery Stored Procedure)**:
    *   Create `project.dataset.sp_r_ausd_v_ta_notice` (BigQuery Stored Procedure) corresponding to `r_ausd_v_ta_notice.ksh`. This will act as the main entry point:
        *   Input parameters `param_h`, `param_s`, `param_l`.
        *   Parameter validation and usage display logic.
        *   Calls to logging/job control helper procedures.
        *   Call to `project.dataset.sp_k_ausd_v_ta_notice_core`.
        *   Error handling with `BEGIN...EXCEPTION`.

6.  **Orchestration (Cloud Composer / Workflows)**:
    *   Develop a Cloud Composer DAG or Cloud Workflows definition to schedule and execute `project.dataset.sp_r_ausd_v_ta_notice`. This will be the new job scheduler.

7.  **Data Pipeline for `cds$ta_notice`**:
    *   Implement a data pipeline (e.g., Dataflow, Dataproc, Fivetran, or a custom Cloud Function) to continuously or periodically ingest data from the Oracle `cds$ta_notice` source into the BigQuery `project.dataset.cds_ta_notice` table, if a native BigQuery table is chosen.