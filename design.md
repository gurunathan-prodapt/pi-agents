# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

## 1. Purpose & Scope
This job, `r_ausd_v_ta_inv_assign.ksh`, is an assembled ETL workflow focused on the reconciliation of contract data for the `ta_inv_assign` table. It acts as a wrapper script, orchestrating the execution of a core processing script (`k_ausd_v_ta_inv_assign.ksh`), which in turn executes a data manipulation SQL script (`d_ausd_v_ta_inv_assign.sql`). The primary business purpose is to synchronize or update contract assignment data within the `sof$ta_inv_assign` table based on a source `cds$ta_inv_assignment` table, filtering records by date and production status. The workflow includes environment initialization, parameter validation, comprehensive logging, and error handling.

## 2. Source Inventory
This job comprises three primary files and several sourced utility scripts. Complexity tier information is missing from the database for all files.

*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh`**
    *   **Technology:** KornShell
    *   **Tier:** (missing)
    *   **Automation Bucket:** semi_auto
    *   **Summary:** Framework script for contract data reconciliation (`ta_inv_assign`), acting as a wrapper for a core processing script.
*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh`**
    *   **Technology:** KornShell
    *   **Tier:** (missing)
    *   **Automation Bucket:** semi_auto
    *   **Summary:** Control script that parses parameters, sources utility scripts (error handling, date checks, parameter parsing, SQL*Plus interaction), executes `d_ausd_v_ta_inv_assign.sql`, and manages job status.
*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_inv_assign.sql`**
    *   **Technology:** Oracle PL/SQL
    *   **Tier:** (missing)
    *   **Automation Bucket:** manual
    *   **Summary:** SQL script that truncates and populates `sof$ta_inv_assign` from `cds$ta_inv_assignment`, filtering by date and `is_production` flag, and uses a database link.

## 3. Target Architecture
The target architecture in BigQuery will consist of:

*   **BigQuery Stored Procedures:**
    *   A main stored procedure (`Vertragsdatenabgleich`) will replace `r_ausd_v_ta_inv_assign.ksh`, acting as the wrapper. It will handle parameter parsing, environment setup logic, logging initialization, and call the core processing stored procedure.
    *   A core stored procedure (`k_ausd_v_ta_inv_assign`) will replace `k_ausd_v_ta_inv_assign.ksh`. It will accept parameters, perform parameter validation, and execute the data transformation logic.
*   **BigQuery SQL Script:** The data transformation logic from `d_ausd_v_ta_inv_assign.sql` will be directly translated into a BigQuery SQL script, which can be executed within the core stored procedure using `EXECUTE IMMEDIATE` or inlined as a DML statement.
*   **Logging and Status Tables:** Custom logging and status tracking logic (e.g., `DWMSG_MeldeFehler`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`) will be replaced by dedicated BigQuery logging tables (e.g., `job_log`, `job_error_log`, `job_table`) and helper stored procedures for managing these tables.
*   **Configuration:** Environment variables (`$HOME/.dw_init`, `${BERT_DIR_ROOT}`) and parameters will be managed via BigQuery stored procedure parameters, session variables, or dedicated configuration tables.

## 4. Data Flow & Lineage
The lineage describes a three-stage process:

1.  **Wrapper Execution (`r_ausd_v_ta_inv_assign.ksh` -> BigQuery Stored Procedure):**
    *   Initializes environment variables (migrated to parameters/config).
    *   Sets up logging and error trapping (migrated to BigQuery logging tables and `BEGIN...EXCEPTION` blocks).
    *   Calls the core processing script (`k_ausd_v_ta_inv_assign.ksh`).
    *   Logs job start and end status.

2.  **Core Processing (`k_ausd_v_ta_inv_assign.ksh` -> BigQuery Stored Procedure):**
    *   Receives job identifier (`-j`) and entry number (`-f`) as parameters.
    *   Sources additional utility scripts (migrated to helper stored procedures or inlined logic).
    *   Performs parameter validation.
    *   Identifies and possibly deactivates older active jobs (migrated to updates on BigQuery job status tables).
    *   Executes the main SQL script (`d_ausd_v_ta_inv_assign.sql`).
    *   Captures record counts (migrated to SQL `COUNT(*)` and variable assignment).

3.  **Data Transformation (`d_ausd_v_ta_inv_assign.sql` -> BigQuery SQL):**
    *   **Source:**
        *   `isbert_schema.dwtk_meldungen`: Used to determine a `v_datum` (snapshot date) based on `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        *   `cds$ta_inv_assignment` (accessed via `&v_carmen` database link): Provides the source contract assignment data.
    *   **Process:**
        *   Truncates the target table `sof$ta_inv_assign`.
        *   Inserts data into `sof$ta_inv_assign` by selecting from `cds$ta_inv_assignment`.
        *   **Filtering Logic:** Records are filtered based on `insert_at`, `modified_at`, `valid_from`, `valid_to` fields against the derived `v_datum`, and where `is_production = 1`.
    *   **Target:** `sof$ta_inv_assign`

## 5. Transformation Logic
The core data transformation logic resides within `d_ausd_v_ta_inv_assign.sql`, which will be directly translated to BigQuery SQL.

**Original Oracle SQL Logic (`d_ausd_v_ta_inv_assign.sql`):**

1.  **Date Variable Determination:**
    ```sql
    DEFINE v_carmen       = "@pcrs1" -- Database link placeholder
    COLUMN s_datum new_value v_datum noprint
    SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
      FROM isbert_schema.dwtk_meldungen m
     WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    ```
    This defines `v_datum` as the maximum `timecreated` from `dwtk_meldungen` for a specific `job_kennung`, formatted as 'YYYYMMDD', defaulting to '19000101'.

2.  **Truncate Target Table:**
    ```sql
    begin
    isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_inv_assign');
    end;
    /
    ```
    This truncates the target table `sof$ta_inv_assign` before inserting new data. The `DWPA_UTIL_SKRIPT.runstatement` suggests a procedural wrapper for DDL.

3.  **Insert Data into Target Table:**
    ```sql
    INSERT  INTO sof$ta_inv_assign(
            cntrct_id,
            inv_definition_id)
    SELECT
            ia.cntrct_id,
            ia.inv_definition_id
    FROM
            cds$ta_inv_assignment     &v_carmen     ia
    WHERE
            ia.insert_at <= TO_DATE('&v_datum','YYYYMMDD')
    AND     (   ia.modified_at IS NULL
             OR ia.modified_at > TO_DATE('&v_datum','YYYYMMDD')     )
    AND     ia.valid_from <= TO_DATE('&v_datum','YYYYMMDD')
    AND     (   ia.valid_to IS NULL
             OR ia.valid_to > TO_DATE('&v_datum','YYYYMMDD')       )
    AND ia.is_production = 1;
    commit;
    ```
    This populates `sof$ta_inv_assign` with `cntrct_id` and `inv_definition_id` from `cds$ta_inv_assignment`.
    The filtering conditions are:
    *   `ia.insert_at` must be less than or equal to `v_datum`.
    *   `ia.modified_at` must be `NULL` or greater than `v_datum`.
    *   `ia.valid_from` must be less than or equal to `v_datum`.
    *   `ia.valid_to` must be `NULL` or greater than `v_datum`.
    *   `ia.is_production` must be `1`.

**Migrated BigQuery SQL Logic:**

1.  **Date Variable Determination:**
    ```sql
    DECLARE v_datum STRING DEFAULT (
      SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
      FROM `isbert_schema.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    ```
    *   `NVL` becomes `COALESCE`.
    *   `TO_CHAR(MAX(m.timecreated),'YYYYMMDD')` becomes `FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated)))`.

2.  **Truncate Target Table:**
    ```sql
    TRUNCATE TABLE `project.dataset.sof$ta_inv_assign`;
    ```
    *   `TRUNCATE TABLE` is a valid BigQuery DDL statement. The procedural `runstatement` call will be removed, and `TRUNCATE` will be executed directly.

3.  **Insert Data into Target Table:**
    ```sql
    INSERT INTO `project.dataset.sof$ta_inv_assign` (
      cntrct_id,
      inv_definition_id
    )
    SELECT
      ia.cntrct_id,
      ia.inv_definition_id
    FROM `project.dataset.cds$ta_inv_assignment` ia -- External system `&v_carmen` resolved to BigQuery table
    WHERE
      ia.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
      AND (ia.modified_at IS NULL OR ia.modified_at > PARSE_DATE('%Y%m%d', v_datum))
      AND ia.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
      AND (ia.valid_to IS NULL OR ia.valid_to > PARSE_DATE('%Y%m%d', v_datum))
      AND ia.is_production = 1;
    ```
    *   `TO_DATE('&v_datum','YYYYMMDD')` becomes `PARSE_DATE('%Y%m%d', v_datum)`.
    *   The Oracle database link `&v_carmen` needs to be resolved to a BigQuery dataset and table (`project.dataset.cds$ta_inv_assignment`).
    *   `commit` is implicitly handled in BigQuery.

**Orchestration Logic (from `r_ausd_v_ta_inv_assign.ksh` and `k_ausd_v_ta_inv_assign.ksh`):**
*   **Parameter Parsing:** Replaced by BigQuery stored procedure input parameters.
*   **Environment Initialization (`. $HOME/.dw_init`):** Replaced by BigQuery project/dataset configuration or initial DECLARE statements in stored procedures.
*   **Utility Script Calls (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`):** Replaced by BigQuery helper stored procedures, UDFs, or inlined scripting logic.
*   **Error Handling (`set -eu`, `trap`):** Replaced by BigQuery `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks and conditional logic (`IF ErrNr != 0 THEN SIGNAL SQLSTATE...END IF`). Custom error logging functions will insert into BigQuery logging tables.
*   **Logging:** Output to log files (`>> $LogDatei`) and `tee -a` will be replaced by inserts into BigQuery logging tables.
*   **External Script Execution (`${Name_Kernskript}` and `starteSQLSkript`):** The call to `k_ausd_v_ta_inv_assign.ksh` will become a call to a BigQuery stored procedure. The `starteSQLSkript` function within `k_ausd_v_ta_inv_assign.ksh` will be replaced by `EXECUTE IMMEDIATE` for dynamic SQL or direct DML within the BigQuery stored procedure.

## 6. External Dependencies
The job has the following external dependencies:

*   **Oracle Database:**
    *   **Source Tables:**
        *   `isbert_schema.dwtk_meldungen`: Used to determine the processing date.
        *   `cds$ta_inv_assignment` (accessed via `&v_carmen` database link): The primary source of contract assignment data.
    *   **Target Table:**
        *   `sof$ta_inv_assign`: The target table to be populated.
    *   **Utility/Procedural Schema:** `isbert_schema.DWPA_UTIL_SKRIPT` for `runstatement` (used for `TRUNCATE TABLE`).
    *   **Replacement Strategy:** These Oracle tables will need to be migrated or replicated into BigQuery. `cds$ta_inv_assignment` will become `project.dataset.cds$ta_inv_assignment`, `isbert_schema.dwtk_meldungen` will become `project.dataset.dwtk_meldungen`, and `sof$ta_inv_assign` will become `project.dataset.sof$ta_inv_assign`. The `&v_carmen` database link implies a connection to a remote Oracle instance, which must be replaced by direct access to the migrated BigQuery tables. The `DWPA_UTIL_SKRIPT` call will be replaced by native BigQuery DDL.

*   **KornShell Utility Scripts:**
    *   `$HOME/.dw_init`: Environment initialization.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging framework.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utilities.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus execution utilities.
    *   **Replacement Strategy:** These utilities will need to be re-implemented as BigQuery helper stored procedures, UDFs, or absorbed into the main stored procedures where their functionality is simple. Environment variables will be handled by BigQuery parameters or configuration.

## 7. Unresolved / Risks
*   **Missing Complexity Data:** The database queries for `file_complexity` consistently returned "No rows" for all three analyzed files. This means there is no pre-calculated complexity tier or migration flags, which could indicate unforeseen challenges or a higher effort than currently estimated.
*   **`manual` Migration Bucket for SQL Script:** The `d_ausd_v_ta_inv_assign.sql` is flagged for 'manual' migration. While the MCP tool provided a direct BigQuery translation, this flag suggests potential complexities not captured in the basic analysis, such as highly specific Oracle features, performance concerns, or data volume issues that might require a redesign. The usage of `WHENEVER SQLERROR CONTINUE` and `WHENEVER SQLERROR EXIT FAILURE` in the Oracle script implies specific error handling that needs careful re-evaluation for BigQuery's `BEGIN...EXCEPTION` blocks.
*   **Orchestration Beyond SQL:** While BigQuery Stored Procedures can replicate much of the shell script logic, complex scheduling, cross-system orchestration, or interactions with external services might still require an external orchestrator like Cloud Composer (Airflow) or Cloud Workflows.
*   **Error Handling Framework:** The existing custom error handling and logging framework within the KornShell scripts needs to be thoroughly understood and accurately translated into BigQuery's error handling mechanisms and logging table structures.
*   **Database Link (`&v_carmen`):** The Oracle database link needs to be replaced. This usually means the linked source system (implied "Carmen DB") must either be migrated to BigQuery or a robust data ingestion pipeline (e.g., Fivetran, Dataflow) must be established to bring data from Carmen DB into BigQuery.

## 8. Build Plan
The migration will involve creating the following BigQuery components:

1.  **BigQuery Logging Tables:**
    *   `project.dataset.job_log`
    *   `project.dataset.job_error_log`
    *   `project.dataset.job_table` (to manage job status, similar to the original system's job table for active/inactive jobs).
    *   **Language:** BigQuery DDL

2.  **Helper Stored Procedures / UDFs:**
    *   `project.dataset.DWMSG_MeldeFehler`: To replicate error reporting.
    *   `project.dataset.DWMSG_ErmittleNr`: To generate job entry numbers.
    *   `project.dataset.DWMSG_Logdateiname`: To generate log file names (or logical log identifiers).
    *   `project.dataset.DWMSG_ErzeugeEintrag`: To create log entries.
    *   `project.dataset.DWMSG_SetzeStichtagInfo`: To set date information.
    *   `project.dataset.DWMSG_Fehlerbehandlung`: Generic error handling.
    *   `project.dataset.DWMSG_SetzeStatusOK`: To mark job status as OK.
    *   Additional procedures to encapsulate functionality from `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh` as needed.
    *   **Language:** BigQuery SQL (Stored Procedures/UDFs)

3.  **BigQuery Core Transformation Stored Procedure:**
    *   `project.dataset.k_ausd_v_ta_inv_assign`
    *   **Functionality:** Will take `JobKennung` and `EintragsNr` as parameters. It will contain the translated logic from `k_ausd_v_ta_inv_assign.ksh` and embed or call the BigQuery SQL transformation from `d_ausd_v_ta_inv_assign.sql`.
    *   **Language:** BigQuery SQL (Stored Procedure)

4.  **BigQuery Wrapper Stored Procedure:**
    *   `project.dataset.Vertragsdatenabgleich`
    *   **Functionality:** Will take parameters (e.g., `-h`, `-s`, `-l` if they are to be maintained, or just job execution parameters). It will replicate the environment setup, parameter validation, initial logging, and then call `project.dataset.k_ausd_v_ta_inv_assign`.
    *   **Language:** BigQuery SQL (Stored Procedure)

**Execution Order:**
1.  Deploy all BigQuery DDL for logging and status tables.
2.  Deploy all BigQuery helper stored procedures and UDFs.
3.  Deploy `project.dataset.k_ausd_v_ta_inv_assign` stored procedure.
4.  Deploy `project.dataset.Vertragsdatenabgleich` wrapper stored procedure.
5.  Schedule `project.dataset.Vertragsdatenabgleich` for execution, e.g., via Cloud Scheduler, Dataform, or Cloud Composer.