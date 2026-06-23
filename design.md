# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh

## 1. Purpose & Scope
This job, `r_ausd_v_ta_vvl_upgrade.ksh`, is a KornShell wrapper script designed to orchestrate the reconciliation of contract data for the `ta_vvl_upgrade` table. Its primary purpose is to set up the execution environment, parse and validate parameters, manage logging and error handling, and invoke a core processing script (`k_ausd_v_ta_vvl_upgrade.ksh`). The core processing script, in turn, executes an Oracle SQL script (`d_ausd_v_ta_vvl_upgrade.sql`) which performs the actual data transformation and update on the `ta_vvl_upgrade` table. The overall job's business purpose is to prepare data for VVL (Vertragsverlängerung - contract extension) upgrades.

## 2. Source Inventory
The assembled job consists of three main components:

| File Name (relative_path)                                              | Technology       | Category | Tool            | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                  |
| :--------------------------------------------------------------------- | :--------------- | :------- | :-------------- | :----- | :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh` | KornShell Script | shell    | KornShell       | medium | semi_auto         | This is a KornShell wrapper script that orchestrates the execution of a core data reconciliation script for the ta_vvl_upgrade table, handling environment setup, parameter parsing, error trapping, and logging.                                                                                             |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh` | KornShell Script | shell    | KornShell       | medium | semi_auto         | KornShell control script for r_ausd_vertrag.ksh, handling job execution, parameter parsing, error handling, and calling a SQL script to update the ta_vvl_upgrade table.                                                                                                                                    |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_vvl_upgrade.sql` | Oracle SQL       | sql      | Oracle SQLPlus  | medium | semi_auto         | This SQL script prepares data for VVL upgrade by truncating a target table and then inserting transformed data from several source tables, including a lookup for upgrade reasons and a subquery for the latest upgrade date.                                                                                   |

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services, primarily BigQuery for data storage and transformation, and Cloud Composer (Apache Airflow) or Cloud Workflows for orchestration.

*   **BigQuery**: All source Oracle tables will be migrated to BigQuery tables. The `sof$ta_vvl_upgrade`, `sof$ta_vvl_dwh`, `dwh$ta_l_bindefr_aendgr_carm`, and `isbert_schema.dwtk_meldungen` tables will have their schemas replicated in BigQuery. The data transformation logic from `d_ausd_v_ta_vvl_upgrade.sql` will be implemented as a BigQuery SQL stored procedure.
*   **BigQuery Stored Procedures**: The logic of `r_ausd_v_ta_vvl_upgrade.ksh` and `k_ausd_v_ta_vvl_upgrade.ksh` will be combined and migrated into a single or multiple BigQuery stored procedures. These procedures will handle parameter validation, job logging, and call the core data transformation BigQuery SQL procedure.
*   **Cloud Composer (Apache Airflow) / Cloud Workflows**: The overall job orchestration (e.g., scheduling, triggering the main BigQuery stored procedure) will be managed by Cloud Composer or Cloud Workflows. This will replace the shell script's role as the primary orchestrator.
*   **Logging**: The custom shell-based logging framework (`DWMSG_*` functions) will be replaced by inserting records into a dedicated BigQuery logging table. This table will capture job execution details, status, and any errors.

## 4. Data Flow & Lineage
The original data flow is:
`r_ausd_v_ta_vvl_upgrade.ksh` (wrapper) -> `k_ausd_v_ta_vvl_upgrade.ksh` (control) -> `d_ausd_v_ta_vvl_upgrade.sql` (data transformation on Oracle).

In the BigQuery target architecture, this will be transformed into:

1.  **Orchestration Layer (Cloud Composer/Workflows)**: A DAG/workflow will be created to trigger the main BigQuery stored procedure.
2.  **Main BigQuery Stored Procedure (replacing `r_ausd_v_ta_vvl_upgrade.ksh` and `k_ausd_v_ta_vvl_upgrade.ksh`)**:
    *   Accepts parameters (e.g., `job_kennung`, `eintrags_nr`).
    *   Performs parameter validation.
    *   Logs job start and metadata into a BigQuery logging table.
    *   Calls the `d_ausd_v_ta_vvl_upgrade` BigQuery SQL stored procedure.
    *   Logs job completion or error status into the BigQuery logging table.
3.  **Data Transformation BigQuery Stored Procedure (replacing `d_ausd_v_ta_vvl_upgrade.sql`)**:
    *   Retrieves the `v_datum` (date) from the `dwtk_meldungen` BigQuery table.
    *   Truncates the target table `sof$ta_vvl_upgrade`.
    *   Inserts transformed data into `sof$ta_vvl_upgrade` by joining `sof$ta_vvl_dwh`, `dwh$ta_l_bindefr_aendgr_carm`, and a subquery to determine the latest `upgr_datum`.

**Data Lineage (conceptual):**
`isbert_schema.dwtk_meldungen` (BigQuery Table)
`sof$ta_vvl_dwh` (BigQuery Table)
`dwh$ta_l_bindefr_aendgr_carm` (BigQuery Table)
   -> BigQuery Stored Procedure (`d_ausd_v_ta_vvl_upgrade`)
   -> `sof$ta_vvl_upgrade` (BigQuery Table)

## 5. Transformation Logic

**a. `r_ausd_v_ta_vvl_upgrade.ksh` (Wrapper Script)**
*   **Original Logic**: Handles usage display, environment sourcing (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`), `getopts` for parameter parsing (`-h`, `-s`, `-l`), error trapping (`INT`, `ERR`), and invocation of `k_ausd_v_ta_vvl_upgrade.ksh`. It also uses a custom logging framework (`DWMSG_*` functions).
*   **Target Transformation**: This script's orchestration logic will be replaced by a BigQuery stored procedure and potentially Cloud Composer.
    *   Parameters (`-s`, `-l`) will be BigQuery stored procedure input parameters.
    *   `getopts` and shell-specific error handling (`trap`) will be replaced by BigQuery's procedural `IF` statements and `EXCEPTION WHEN ERROR` blocks.
    *   The `DWMSG_*` logging functions will be replaced by `INSERT` statements into a BigQuery logging table.
    *   The invocation of `k_ausd_v_ta_vvl_upgrade.ksh` will become a `CALL` statement to the corresponding BigQuery stored procedure.

**b. `k_ausd_v_ta_vvl_upgrade.ksh` (Control Script)**
*   **Original Logic**: Parses parameters (`-j` for `JobKennung`, `-f` for `EintragsNr`), validates them using `pruefeParameterGesetzt`, sets `v_TabName`, sources `h_alis_sqlplus.ksh`, defines `Name_SQLskript` (`d_ausd_v_ta_vvl_upgrade.sql`), and executes it via `starteSQLSkript`. It also uses a temporary file (`tmpFile`) to capture record counts.
*   **Target Transformation**: This script's logic will be merged into the main BigQuery stored procedure.
    *   Parameters (`p_JobKennung`, `p_EintragsNr`) will be BigQuery stored procedure input parameters.
    *   Parameter validation will use `IF` statements.
    *   The `starteSQLSkript` call will be replaced by directly executing the BigQuery SQL `d_ausd_v_ta_vvl_upgrade` stored procedure.
    *   The temporary file `tmpFile` will be replaced by a BigQuery variable (`DECLARE v_records INT64;`) which will store the count of affected rows.

**c. `d_ausd_v_ta_vvl_upgrade.sql` (Core SQL Logic)**
*   **Original Logic**: Oracle SQLPlus script.
    *   Defines `v_carmen` (DB-Link variable).
    *   Determines `v_datum` from `isbert_schema.dwtk_meldungen` using `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')`.
    *   Includes SQLPlus settings (`WHENEVER SQLERROR`, `SET TIMING ON`, `SET SERVEROUTPUT ON`).
    *   Truncates `sof$ta_vvl_upgrade` using `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
    *   Inserts into `sof$ta_vvl_upgrade` from `sof$ta_vvl_dwh`, `dwh$ta_l_bindefr_aendgr_carm`, and a subquery (`SELECT MAX(aenderung_am) GROUP BY vertrags_id`) using parallel hints and `CASE` statement for `upgradegrund`.
    *   `commit;` and `spool off;`.
*   **Target Transformation**: This will be converted directly into a BigQuery SQL stored procedure.
    *   Oracle `NVL` will become BigQuery `IFNULL`.
    *   Oracle `TO_CHAR(date, 'YYYYMMDD')` will become BigQuery `FORMAT_DATE('%Y%m%d', DATE(...))`.
    *   Oracle comma-separated joins will be converted to explicit `JOIN` clauses.
    *   The `TRUNCATE TABLE` statement is compatible with BigQuery.
    *   Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` will be replaced by a direct `TRUNCATE TABLE` statement.
    *   Parallel hints will be removed as BigQuery handles parallelism automatically.
    *   The `commit;` and `spool off;` commands are not needed in BigQuery stored procedures.

## 6. External Dependencies
The original system has the following external dependencies:

*   **Oracle Database**: This is the primary data source and target for the SQL script.
    *   **Replacement**: All Oracle tables (`sof$ta_vvl_upgrade`, `sof$ta_vvl_dwh`, `dwh$ta_l_bindefr_aendgr_carm`, `isbert_schema.dwtk_meldungen`) will be migrated to BigQuery tables. The Oracle SQL logic will be converted to BigQuery SQL.
*   **Shell Environment (KornShell)**: The scripts rely on shell-specific features like `getopts`, `trap`, `source` (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`), and environment variables (`BERT_DIR_ROOT`, `DW_DIR_UTL`).
    *   **Replacement**:
        *   Shell parameter parsing will be replaced by BigQuery stored procedure parameters.
        *   Shell error trapping will be replaced by BigQuery's `EXCEPTION WHEN ERROR` blocks.
        *   Sourced utility scripts will be replaced by BigQuery stored procedures (for reusable logic like logging/date handling) or removed if their functionality is natively handled by BigQuery or the orchestration layer.
        *   Environment variables will be replaced by BigQuery stored procedure variables, constants, or parameters passed from the orchestration layer.
*   **Custom Logging Framework (`DWMSG_*` functions)**: These functions are sourced from other shell scripts.
    *   **Replacement**: Will be replaced by `INSERT` statements into a dedicated BigQuery logging table.

## 7. Unresolved / Risks
*   **Specifics of Sourced Shell Scripts**: The content of `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, and `h_alis_sqlplus.ksh` were not provided. Their exact functionality needs to be understood to ensure all implicit dependencies and logic are correctly migrated or accounted for. Assumptions have been made based on their names.
*   **Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`**: While assumed to be a generic SQL execution utility, any complex logic within this Oracle package call would need specific migration to BigQuery stored procedures or UDFs if it's more than a simple DDL/DML wrapper.
*   **Performance of Max Date Subquery**: The subquery for `MAX(aenderung_am) GROUP BY vertrags_id` in `d_ausd_v_ta_vvl_upgrade.sql` is parallelized in Oracle. BigQuery's automatic parallelism should handle this efficiently, but performance should be monitored.
*   **Character Encoding**: Potential issues with non-ASCII characters in `CASE` statements (e.g., `Endgerteupgrade` vs `Endgeräteupgrade`) need to be verified during migration to ensure correct character sets are maintained.

## 8. Build Plan
The migration will result in BigQuery SQL stored procedures and a Cloud Composer DAG.

1.  **Schema Migration**:
    *   Migrate all Oracle source tables (`sof$ta_vvl_dwh`, `dwh$ta_l_bindefr_aendgr_carm`, `isbert_schema.dwtk_meldungen`) to BigQuery datasets and tables.
    *   Create the target BigQuery table `sof$ta_vvl_upgrade`.
    *   Create the BigQuery logging table for job status and messages.

2.  **`d_ausd_v_ta_vvl_upgrade.sql` to BigQuery Stored Procedure**:
    *   **Language**: BigQuery SQL
    *   Create a BigQuery stored procedure (e.g., `project.dataset.d_ausd_v_ta_vvl_upgrade_sp`) containing the transformed SQL logic as detailed in Section 5.c.

3.  **`r_ausd_v_ta_vvl_upgrade.ksh` and `k_ausd_v_ta_vvl_upgrade.ksh` to BigQuery Stored Procedure**:
    *   **Language**: BigQuery SQL
    *   Create a BigQuery stored procedure (e.g., `project.dataset.vertragsdatenabgleich_wrapper_sp`) that encapsulates the combined orchestration and control logic, parameter handling, and logging. This procedure will `CALL` `project.dataset.d_ausd_v_ta_vvl_upgrade_sp`.

4.  **Orchestration Layer**:
    *   **Language**: Python (for Airflow DAG) or YAML (for Workflows)
    *   Develop a Cloud Composer DAG or Cloud Workflow definition to schedule and trigger `project.dataset.vertragsdatenabgleich_wrapper_sp` with the necessary parameters.

5.  **Refactor Utility Functions (if necessary)**:
    *   If the sourced shell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) contain complex reusable logic not covered by BigQuery native functions or the logging table, these should be re-implemented as BigQuery SQL UDFs or separate helper stored procedures.