# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

## 1. Purpose & Scope
This document outlines the migration design for `f_alis_msgerr.ksh`, a KornShell utility script. The script's primary purpose is to provide standardized error handling, logging, and status management functionalities for Information Services. It acts as a wrapper for Oracle PL/SQL procedures, interacting with an Oracle database via `sqlplus`. The script's functions include creating and updating message/log entries, marking entries as successful or aborted, recording errors with various details, generating unique entry numbers, and constructing log filenames. The scope of this migration is to re-implement these functionalities on the Google Cloud Platform, specifically leveraging BigQuery for database operations and potentially other GCP services for orchestration.

## 2. Source Inventory
The migration involves a single source file:
- **File Name:** `f_alis_msgerr.ksh`
- **Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`
- **Technology:** KornShell (KSH)
- **Complexity Tier:** Medium
- **Automation Bucket:** Semi-Auto
- **Summary:** KornShell utility script providing functions for error handling, logging, and status management in Information Services, interacting with an Oracle database via SQL*Plus. It defines functions like `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`, `DWMSG_SetzeStatusAbbruch`, `DWMSG_ErmittleNr`, `DWMSG_ErzeugeEintrag`, `DWMSG_MeldeFehler`, `DWMSG_Logdateiname`, `DWMSG_SetzeStichtagInfo`, and `DWMSG_AppendTimingInfos`.

## 3. Target Architecture
The target architecture will predominantly utilize Google BigQuery for data storage and procedural logic.
- **BigQuery Stored Procedures:** Each KornShell function that interacts with the database will be migrated to a BigQuery Stored Procedure. These procedures will encapsulate the data manipulation logic currently residing in Oracle PL/SQL.
- **BigQuery Tables:** The Oracle `BERT_MELDUNG` table (and potentially `BERT_MELDUNG_FEHLER`) will be migrated to BigQuery tables, maintaining equivalent schemas.
- **Orchestration Layer:** For shell-specific behaviors not directly supported by BigQuery (e.g., environment variable management, temporary file handling, external calls), a lightweight orchestration layer using Cloud Workflows, Cloud Composer, or Cloud Run will be considered. This layer will be responsible for calling BigQuery Stored Procedures and managing job-level parameters.
- **Error Handling:** BigQuery's `BEGIN...EXCEPTION...END` blocks and explicit status updates within stored procedures will replace the shell's `trap ERR` mechanism.

## 4. Data Flow & Lineage
The original script does not explicitly define a data flow in terms of source-to-target tables in the traditional ETL sense, but rather provides utility functions for managing metadata and error logs within a centralized Oracle system.

**Original Data Flow (Conceptual):**
- **Input (via parameters/environment variables):** `DW_ORAUSER`, `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DateFormat`, `EintragsNr`, `JobKennung`, `Programmname`, `LogDatei`, `Typ`, `FehlerNr`, `Zusatz1`, `Zusatz2`, `VarName`, `DWMSG_Stichtag`, `DWMSG_StichtagFmt`, `DWMSG_InfoText`.
- **Processing (KornShell functions calling Oracle PL/SQL via `sqlplus`):**
    - `DWMSG_Fehlerbehandlung`: Reads shell exit code (`$?`), logs fatal error, sets status to aborted.
    - `DWMSG_SetzeStatusOK`: Updates `BERT_MELDUNG` status to 'OK'.
    - `DWMSG_SetzeStatusAbbruch`: Updates `BERT_MELDUNG` status to 'ABORTED'.
    - `DWMSG_ErmittleNr`: Generates a unique number (via Oracle PL/SQL), stores it in a temporary file, reads it back, and assigns it to a shell variable.
    - `DWMSG_ErzeugeEintrag`: Inserts a new entry into `BERT_MELDUNG` with job/program/log metadata.
    - `DWMSG_MeldeFehler`: Inserts/updates error details into `BERT_MELDUNG` (or a related error table).
    - `DWMSG_Logdateiname`: Constructs a log filename string.
    - `DWMSG_SetzeStichtagInfo`: Updates additional info (`ZUSATZINFOS`) with date/time.
    - `DWMSG_AppendTimingInfos`: Appends timing info to `ZUSATZINFOS`.
- **Output (Oracle Database / Shell variables):** Updates to `BERT_MELDUNG` table, return values to shell variables (`eval`), constructed log filenames.

**Migrated Data Flow (Conceptual):**
- **Input (via Orchestration / BigQuery Stored Procedure parameters):** Equivalent parameters will be passed to BigQuery Stored Procedures. Environment variables will be replaced by BigQuery script variables or parameters.
- **Processing (BigQuery Stored Procedures):**
    - Each `DWMSG_` function will be a corresponding BigQuery Stored Procedure.
    - `DWMSG_ErmittleNr`: Will generate a unique identifier directly within BigQuery (e.g., using `GENERATE_UUID()` or `FARM_FINGERPRINT`) and return it as an `OUT` parameter.
    - `DWMSG_Logdateiname`: Will be a BigQuery Stored Procedure that constructs and returns the log filename string.
    - All other `DWMSG_` procedures will perform `INSERT` or `UPDATE` operations on the BigQuery `BERT_MELDUNG` (and `BERT_MELDUNG_FEHLER`) tables.
    - Error handling will use BigQuery's `BEGIN...EXCEPTION...END` blocks and `SIGNAL SQLSTATE` for explicit error messaging.
- **Output (BigQuery Tables / Orchestration Layer):** Updates to `BERT_MELDUNG` and `BERT_MELDUNG_FEHLER` tables. Return values from BigQuery Stored Procedures will be handled by the orchestration layer.

## 5. Transformation Logic
The core transformation logic involves converting KornShell script functions and their Oracle PL/SQL interactions into BigQuery Stored Procedures.

- **`DWMSG_Fehlerbehandlung`:**
    - The shell's `$?` (exit code) capture will be handled by explicit error signaling/passing in the orchestration layer or within BigQuery scripting if triggered by a BigQuery error.
    - Will call BigQuery Stored Procedures for error logging (`DWMSG_MeldeFehler`) and status update (`DWMSG_SetzeStatusAbbruch`).
- **`DWMSG_SetzeStatusOK` / `DWMSG_SetzeStatusAbbruch`:**
    - Direct conversion to BigQuery Stored Procedures performing `UPDATE` statements on the `BERT_MELDUNG` table to set the `Status` field.
    - Parameter validation (`[ -z "$DWMSG_EintragsNr" ]`) will be translated to `IF ... THEN SIGNAL SQLSTATE ... END IF` within the BigQuery Stored Procedure.
- **`DWMSG_ErmittleNr`:**
    - The logic of generating a unique number will be moved entirely to a BigQuery Stored Procedure. Instead of writing to a temporary file, the procedure will return the generated number as an `OUT` parameter or via a `SELECT` statement.
    - Example BigQuery approach: `SELECT CAST(ABS(FARM_FINGERPRINT(GENERATE_UUID())) AS STRING)` for generating a unique string.
- **`DWMSG_ErzeugeEintrag`:**
    - Migrated to a BigQuery Stored Procedure that performs an `INSERT` statement into the `BERT_MELDUNG` table.
- **`DWMSG_MeldeFehler`:**
    - Migrated to a BigQuery Stored Procedure. The parameter count logic for choosing SQL wrapper (`d_alis_spaufruf_p${NumParm}.sql`) will be replaced by a single BigQuery Stored Procedure accepting all parameters, with optional parameters handled by `NULL` values. The procedure will `INSERT` into an error logging table (e.g., `BERT_MELDUNG_FEHLER`).
- **`DWMSG_Logdateiname`:**
    - Migrated to a BigQuery Stored Procedure that constructs the log filename string using BigQuery string functions (e.g., `CONCAT`, `FORMAT_TIMESTAMP`, `CURRENT_TIMESTAMP`) and returns it.
- **`DWMSG_SetzeStichtagInfo` / `DWMSG_AppendTimingInfos`:**
    - Migrated to BigQuery Stored Procedures performing `UPDATE` statements on the `BERT_MELDUNG` table's `ZusatzInfos` column.
    - Oracle's `to_date` and `to_char` functions will be replaced by BigQuery's `PARSE_TIMESTAMP` and `FORMAT_TIMESTAMP`. String concatenation (`||`) will be replaced by `CONCAT`.

## 6. External Dependencies
The primary external dependency is the Oracle Database and its PL/SQL procedures.

- **Oracle Database (`sqlplus` calls to `BERT_MELDUNG` package and `d_alis_spaufruf_pX.sql` scripts):**
    - **Replacement:** The Oracle `BERT_MELDUNG` package will be entirely re-implemented as a set of BigQuery Stored Procedures and corresponding BigQuery tables. The `d_alis_spaufruf_pX.sql` wrapper scripts become obsolete as the BigQuery Stored Procedures will be called directly.
    - **Data Migration:** The `BERT_MELDUNG` (and any related error/log) tables in Oracle will need to be migrated to BigQuery. This can be done via various methods, including:
        - **Batch export/import:** Using `gsutil cp` for CSVs or Dataflow for more complex types.
        - **Database Migration Service (DMS):** For ongoing replication if required during a transition period.
    - **Credentials:** The `DW_ORAUSER` environment variable will be replaced by appropriate Google Cloud authentication mechanisms (e.g., service accounts) for BigQuery access.
- **Temporary Files (`/tmp/ErmittleNr_$$.lst`):**
    - **Replacement:** The functionality of passing data via temporary files (specifically for `DWMSG_ErmittleNr`) will be replaced by direct return values from BigQuery Stored Procedures (e.g., using `OUT` parameters or a `SELECT` statement). No direct filesystem operations for temporary data will be needed within BigQuery.
- **`date` command:**
    - **Replacement:** BigQuery's `CURRENT_TIMESTAMP()` and `FORMAT_TIMESTAMP()` functions will replace the shell's `date` command for timestamp generation and formatting.
- **`cat`, `tr`, `rm` commands:**
    - **Replacement:** These shell utilities, used for reading and processing the temporary file, will become obsolete as the `DWMSG_ErmittleNr` logic is re-implemented directly in BigQuery.

There are no `external_systems` identified in the `lineage_assembled_jobs` for this particular job.

## 7. Unresolved / Risks
- **Oracle PL/SQL Complexity:** The current design assumes a straightforward conversion of Oracle PL/SQL procedures to BigQuery Stored Procedures. If the underlying Oracle PL/SQL in `BERT_MELDUNG` involves highly complex business logic, cursors, or advanced features not easily translatable to BigQuery SQL, a more detailed analysis of each PL/SQL procedure will be required. This could potentially lead to parts of the logic being re-implemented in a more programmatic language (e.g., Python using Dataflow or Cloud Functions) if BigQuery Stored Procedures prove insufficient.
- **Environment Variable Management:** The shell script relies on environment variables (`DW_ORAUSER`, `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DateFormat`). In the BigQuery environment, these will need to be managed as:
    - Parameters to BigQuery Stored Procedures.
    - BigQuery script variables.
    - Configuration managed by the orchestration layer (e.g., environment variables in Cloud Run, Airflow variables in Cloud Composer).
    - The `DW_DIR_PROT` for log file paths is a conceptual path in BigQuery as there won't be a physical filesystem to write to; it will just be a string.
- **Indirect Variable Assignment (`eval`):** The use of `eval` for dynamic variable assignment (`eval "$VarName=$DWMSG_EintragsNr"`) is a shell-specific construct. In BigQuery, this will be replaced by explicit `SET` statements for script variables or `OUT` parameters for stored procedures. This requires careful mapping to ensure the correct variable scope and assignment.
- **Error Context:** The original `DWMSG_Fehlerbehandlung` uses `$?` to get the last command's exit status. In BigQuery, error handling is typically done with `BEGIN...EXCEPTION...END` blocks or by explicitly passing error codes/messages. The precise mapping of how external command failure information is captured and passed to `DWMSG_Fehlerbehandlung` will need careful design.
- **Existing Consumers:** This script is a utility library. A critical risk is identifying all calling scripts/jobs that invoke `f_alis_msgerr.ksh` and ensuring their migration or integration with the new BigQuery-based error handling system. The absence of `lineage_edges` for this file in the initial analysis suggests it's a called component, not a top-level job, reinforcing this risk. This "reverse dependency" needs to be thoroughly mapped out.

## 8. Build Plan

The build plan focuses on incremental migration of components:

1.  **Define BigQuery Schema for `BERT_MELDUNG` (and `BERT_MELDUNG_FEHLER` if separate):**
    *   Create `schema.sql` defining the `BERT_MELDUNG` table (and `BERT_MELDUNG_FEHLER`) in BigQuery, mirroring the Oracle schema.
    *   **Language:** BigQuery DDL
2.  **Migrate Data from Oracle to BigQuery:**
    *   Establish a data migration process to move existing data from Oracle `BERT_MELDUNG` (and `BERT_MELDUNG_FEHLER`) tables to the newly created BigQuery tables.
    *   **Language:** Dataflow (Python/Java) or `bq load` with exported CSVs.
3.  **Develop BigQuery Stored Procedures for each `DWMSG_` function:**
    *   `DWMSG_ErmittleNr` (BigQuery SQL SP: Generates unique ID)
    *   `DWMSG_ErzeugeEintrag` (BigQuery SQL SP: `INSERT` into `BERT_MELDUNG`)
    *   `DWMSG_SetzeStatusOK` (BigQuery SQL SP: `UPDATE` `BERT_MELDUNG` status)
    *   `DWMSG_SetzeStatusAbbruch` (BigQuery SQL SP: `UPDATE` `BERT_MELDUNG` status)
    *   `DWMSG_MeldeFehler` (BigQuery SQL SP: `INSERT` into `BERT_MELDUNG_FEHLER` or `BERT_MELDUNG` with error details)
    *   `DWMSG_Logdateiname` (BigQuery SQL SP: Returns constructed log filename string)
    *   `DWMSG_SetzeStichtagInfo` (BigQuery SQL SP: `UPDATE` `BERT_MELDUNG` with date info)
    *   `DWMSG_AppendTimingInfos` (BigQuery SQL SP: `UPDATE` `BERT_MELDUNG` appending timing info)
    *   `DWMSG_Fehlerbehandlung` (BigQuery SQL SP: Orchestrates calls to `DWMSG_MeldeFehler` and `DWMSG_SetzeStatusAbbruch`, handles error context)
    *   **Language:** BigQuery SQL (for each stored procedure)
4.  **Develop Orchestration Layer (if needed):**
    *   If shell-specific behaviors (e.g., passing dynamic parameters, managing job context outside of BigQuery) are required, develop a lightweight orchestration script. This script will call the BigQuery Stored Procedures and handle any non-database logic.
    *   **Language:** Python (Cloud Functions/Cloud Run) or Airflow DAG (Cloud Composer)
5.  **Integration Testing:**
    *   Test each BigQuery Stored Procedure independently.
    *   Test the orchestration layer (if any) calling the BigQuery Stored Procedures.
    *   Test end-to-end scenarios involving the migrated error handling system.
    *   **Language:** Pytest (for Python orchestration) or BigQuery scripting for direct procedure calls.
6.  **Update Calling Jobs:**
    *   Identify all legacy jobs/scripts that call `f_alis_msgerr.ksh`.
    *   Modify these calling jobs to invoke the new BigQuery Stored Procedures or the orchestration layer instead of the original KornShell script.
    *   **Language:** Dependent on the calling job's language (e.g., Airflow DAGs, Python scripts, other shell scripts modified to call GCP services).