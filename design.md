# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

## 1. Purpose & Scope
This job, `k_ausd_geschaeftspartner.ksh`, serves as a control script for a data processing workflow, specifically related to `r_ausd_vertrag.ksh`. Its primary purpose is to orchestrate the execution of an external SQL script (`d_ausd_geschaeftspartner.sql`) for data processing. It handles parameter parsing, validation of input dates, manages job status (though some parts are commented out), and captures the record count from the SQL script's output. The job is designed to ignore active jobs and ensures proper execution flow by checking for necessary parameters. The overall purpose is to prepare data related to business partners.

## 2. Source Inventory
The job is comprised of a single KornShell script that acts as an orchestrator, invoking other utility scripts and a core SQL script.

| File Path                                                            | Technology    | Role         | Complexity Tier | Automation Bucket |
| :------------------------------------------------------------------- | :------------ | :----------- | :-------------- | :---------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh` | KornShell     | Orchestration | Unspecified     | `semi_auto`       |

**Implicitly Referenced Files (not part of component_files but identified from code):**
- `d_ausd_geschaeftspartner.sql`: Core SQL logic for data transformation.
- `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling utility.
- `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date utility script (e.g., date format checks).
- `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing utility.
- `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus execution utility.
- `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: Script to determine yesterday's and today's dates.
- `$HOME/.dw_init`: Environment initialization file.

## 3. Target Architecture
The target architecture will leverage Google Cloud's BigQuery for data processing and storage, and potentially Cloud Composer (Airflow) or Cloud Functions/Cloud Run for orchestration of the migrated shell script logic.

*   **BigQuery:**
    *   **Stored Procedure:** The core logic of `k_ausd_geschaeftspartner.ksh`, including parameter parsing, validation, and orchestration of the SQL logic, will be migrated into a BigQuery Stored Procedure (e.g., `project.dataset.r_ausd_vertrag_control`). This stored procedure will encapsulate the control flow, error handling, and calls to the transformed SQL logic.
    *   **Data Tables:** All source and target tables currently processed by `d_ausd_geschaeftspartner.sql` will reside in BigQuery. Control tables (e.g., `project.dataset.job_log`) will be introduced to manage job metadata, logging, and status, replacing the file-based temporary outputs and intended job-table updates.
    *   **UDFs/Functions:** Simple utility functions like date formatting or parameter validation might be converted to BigQuery User-Defined Functions (UDFs) if reusable across multiple processes, or directly incorporated into the stored procedure.
*   **Cloud Composer (Airflow):** If complex scheduling, dependency management, or integration with other Google Cloud services is required, an Airflow DAG will be developed to invoke the BigQuery Stored Procedure, pass parameters, and handle higher-level orchestration aspects.
*   **Cloud Functions/Cloud Run:** For any minimal shell-specific behavior that cannot be directly translated to BigQuery SQL, or if a lightweight external execution layer is preferred for parameter processing before calling BigQuery, Cloud Functions or Cloud Run could be used.

## 4. Data Flow & Lineage
The original data flow involves the `k_ausd_geschaeftspartner.ksh` script orchestrating an external SQL script (`d_ausd_geschaeftspartner.sql`). This SQL script performs the actual data reads and writes.

**Original Flow:**
1.  `k_ausd_geschaeftspartner.ksh` is invoked with parameters (JobKennung, EintragsNr, Stichtag, wiederanlaufWert).
2.  Environment variables are sourced (`. $HOME/.dw_init`).
3.  Utility scripts are sourced (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
4.  Parameters are parsed and validated.
5.  Date format of `p_Stichtag` is checked.
6.  `gestern.ksh` is executed to get `p_datum_heute` and `p_datum_gestern`.
7.  The SQL script `d_ausd_geschaeftspartner.sql` is executed via `starteSQLSkript` function, passing all collected parameters.
8.  The record count from the SQL execution is captured from a temporary file (`$DW_DIR_UTL/bert_k_ausd_geschaeftspartner_$$.tmp`).
9.  (Commented out) Job table entry is created/updated.

**Target BigQuery Flow:**
1.  **Orchestration Layer (e.g., Cloud Composer DAG / Cloud Functions):**
    *   Receives input parameters.
    *   Invokes the BigQuery Stored Procedure `project.dataset.r_ausd_vertrag_control`.
2.  **BigQuery Stored Procedure (`project.dataset.r_ausd_vertrag_control`):**
    *   Declares variables for job parameters, dates, and control values.
    *   Performs parameter validation using `IF` conditions and `ASSERT` statements.
    *   Parses `p_Stichtag` into a `DATE` type.
    *   Logs job start to `project.dataset.job_log` table.
    *   Executes the migrated SQL logic (from `d_ausd_geschaeftspartner.sql`), likely embedded or as another stored procedure call. This core SQL logic will read from source tables (e.g., `project.dataset.source_table`) and write to target tables (e.g., `project.dataset.target_table`).
    *   Captures the `COUNT(*)` from the SQL execution directly into a variable.
    *   Logs job completion and record count to `project.dataset.job_log` table.
    *   Handles errors using `BEGIN...EXCEPTION` blocks and logs failures to `project.dataset.job_log`.

## 5. Transformation Logic

**File: `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh`**

**Legacy Logic (KornShell):**
*   **Environment Sourcing:** `. $HOME/.dw_init`
*   **Utility Sourcing:** `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/*` scripts for error handling, date, parameter parsing, and SQL*Plus execution.
*   **Parameter Parsing:** Uses `getopts` for `j`, `f`, `s`, `l` (JobKennung, EintragsNr, Stichtag, wiederanlaufWert).
*   **Parameter Validation:** `pruefeParameterGesetzt` function checks if required parameters are set.
*   **Date Validation:** `DWDate_Datum_Check` validates `p_Stichtag` in `DDMMYYYY` format.
*   **Date Derivation:** Executes `gestern.ksh` to get yesterday's and today's dates.
*   **SQL Execution:** Calls `starteSQLSkript` which wraps `d_ausd_geschaeftspartner.sql` execution with parameters.
*   **Record Count:** Reads from a temporary file (`tmpFile`) generated by the SQL execution.
*   **Job Logging:** (Commented out) `FOSJobErzeugeEintrag`, `FOSJobDeaktivate` were intended for job status management in a `PoolVertrag` table.

**Target Logic (BigQuery Stored Procedure `project.dataset.r_ausd_vertrag_control`):**
*   **Parameters:** Directly takes `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert` as `IN` arguments. `p_Stichtag` will be `STRING` then `PARSE_DATE`'d, `p_wiederanlaufWert` as `INT64`.
*   **Environment/Utilities:** Replaced by BigQuery's native functions, SQL constructs, and control flow.
    *   `DWMSG_MeldeFehler` → `RAISE USING MESSAGE` and logging to `job_log` table.
    *   `pruefeParameterGesetzt` → `IF` conditions and `ASSERT` statements for mandatory parameters.
    *   `DWDate_Datum_Check` → `PARSE_DATE('%d%m%Y', p_Stichtag)` with error handling via `SAFE_CAST` or `IF/ASSERT`.
    *   `gestern.ksh` → `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`.
*   **SQL Execution Orchestration:** The logic of `starteSQLSkript` will be replaced by direct invocation of the migrated `d_ausd_geschaeftspartner.sql` logic, likely as another internal stored procedure or a series of SQL statements within `r_ausd_vertrag_control`.
*   **Record Count:** Use `SELECT COUNT(*)` directly within the BigQuery SQL transformation and assign it to a `DECLARE`d variable. Temporary files are eliminated.
*   **Job Logging:** Implement `INSERT` and `UPDATE` statements against a `project.dataset.job_log` table (e.g., `job_log(job_kennung, eintrags_nr, tab_name, stichtag, status, record_count, message, created_at)`). The `v_TabName` variable will be directly used.

## 6. External Dependencies

| Original System / Component             | External Class | Notes                                                                                                                                                                                                                                                                                                                                | Target Replacement / Strategy                                                                                                                                                                                                                                    |
| :-------------------------------------- | :------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) | FILE           | These are shell scripts providing common functionalities like error handling, date manipulation, parameter parsing, and SQL*Plus invocation. `gestern.ksh` specifically provides today's and yesterday's dates.                                                                                                                                                                              | Functionality will be absorbed into the BigQuery Stored Procedure: <br>- Error handling: `RAISE USING MESSAGE` and logging to a BigQuery control table. <br>- Date handling: BigQuery's `CURRENT_DATE()`, `DATE_SUB()`, `PARSE_DATE()` functions. <br>- Parameter parsing/validation: Handled by SP `IN` parameters, `IF` conditions, and `ASSERT` statements. <br>- SQL*Plus invocation logic is no longer needed as the core SQL will run natively in BigQuery. |
| `$HOME/.dw_init`                        | FILE           | Environment initialization file, likely setting up paths and environment variables.                                                                                                                                                                                                                                                    | Environment variables will be replaced by BigQuery Stored Procedure parameters, BigQuery datasets/project IDs, or configuration values passed by an orchestration layer (e.g., Airflow variables).                                                         |
| `d_ausd_geschaeftspartner.sql`          | FILE           | This is the core SQL script containing the business logic for data processing. Its content is executed by the shell script.                                                                                                                                                                                                            | This SQL script must be migrated to BigQuery SQL. It will either become the body of a separate BigQuery Stored Procedure invoked by `r_ausd_vertrag_control`, or its logic will be embedded directly within `r_ausd_vertrag_control` for simplicity if it's not reused elsewhere. All tables referenced within this SQL will be BigQuery tables. |
| Temporary file (`$DW_DIR_UTL/bert_k_ausd_geschaeftspartner_$$.tmp`) | FILE           | Used to store the record count resulting from the SQL execution.                                                                                                                                                                                                                                                       | Replaced by capturing the `COUNT(*)` directly into a `DECLARE`d variable within the BigQuery Stored Procedure.                                                                                                                                         |
| `PoolVertrag` (implicitly referenced for FOSJob calls) | DB TABLE       | This appears to be a job control or logging table, based on the commented-out `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` calls in the shell script. It likely stores job status, metadata, and possibly record counts.                                                                                              | A BigQuery control table (e.g., `project.dataset.job_log`) will be created to manage job status, metadata, and logs. `INSERT` and `UPDATE` statements will be used to record job start, completion, errors, and record counts.                           |

## 7. Unresolved / Risks
*   **Missing `file_complexity` data:** The `file_complexity` table returned no data for `k_ausd_geschaeftspartner.ksh`. This means there's no official "tier" or `migration_flags` information. While the `semi_auto` bucket suggests it's migratable, the lack of detailed complexity signals could hide unforeseen challenges.
*   **Unclear `r_ausd_vertrag.ksh` context:** The comments mention `Kontrollscript zu r_ausd_vertrag.ksh`. The exact relationship and potential interdependencies with `r_ausd_vertrag.ksh` are not fully known without further lineage data. If `r_ausd_vertrag.ksh` is another orchestrator, the migration of `k_ausd_geschaeftspartner.ksh` might need to be coordinated.
*   **`BERT_DIR_ROOT` and `DW_DIR_UTL` resolution:** The script relies on these environment variables for paths. Their values in the legacy environment need to be mapped to appropriate BigQuery dataset/project references or GCP storage paths in the target environment.
*   **SQL*Plus specifics:** While `h_alis_sqlplus.ksh` is a wrapper, `d_ausd_geschaeftspartner.sql` might contain SQL*Plus specific commands or syntax that needs to be adapted for BigQuery SQL. This requires a separate, detailed migration of the SQL script itself.
*   **`eval` command:** The `eval "v_records=\`cat $tmpFile\`"` command is a shell-specific idiom for variable assignment. While its functionality is clear (reading record count from a file), the use of `eval` in general can sometimes indicate more complex dynamic execution which should be carefully reviewed. In this case, it's straightforwardly replaced by direct SQL counting.
*   **Commented-out code:** The presence of commented-out `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` calls suggests these functionalities were either previously active or are intended for future use. The design assumes these are not critical for the current migration, but if they need to be re-activated, this would require additional implementation in the BigQuery control plane.

## 8. Build Plan
The migration will primarily involve translating the shell orchestration logic into a BigQuery Stored Procedure and migrating the core SQL script to BigQuery SQL.

1.  **Migrate `d_ausd_geschaeftspartner.sql` to BigQuery SQL (if not already done).**
    *   **Description:** Convert the SQL script's logic, including table references, data types, and functions, to be compatible with BigQuery SQL.
    *   **Language:** BigQuery SQL
    *   **Output:** `project.dataset.d_ausd_geschaeftspartner_sp` (BigQuery Stored Procedure) or directly embedded SQL within the control SP.
2.  **Create BigQuery `job_log` Control Table.**
    *   **Description:** Define a BigQuery table to store job execution metadata, status, parameters, and record counts.
    *   **Language:** BigQuery DDL
    *   **Output:** `project.dataset.job_log` (Table)
3.  **Develop BigQuery Stored Procedure for `k_ausd_geschaeftspartner.ksh` logic.**
    *   **Description:** Translate the parameter parsing, validation, date logic, and orchestration of `d_ausd_geschaeftspartner.sql` into a BigQuery Stored Procedure. This will include implementing error handling and updating the `job_log` table.
    *   **Language:** BigQuery SQL (Stored Procedure)
    *   **Output:** `project.dataset.r_ausd_vertrag_control` (BigQuery Stored Procedure)
4.  **Develop Orchestration Layer (if needed).**
    *   **Description:** If external scheduling or more complex parameter passing beyond direct BigQuery SP invocation is required, create an Airflow DAG or Cloud Function to call `project.dataset.r_ausd_vertrag_control`.
    *   **Language:** Python (for Airflow DAG or Cloud Function)
    *   **Output:** Airflow DAG file (`k_ausd_geschaeftspartner_dag.py`) or Cloud Function code.