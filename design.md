# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

## 1. Purpose & Scope
This KornShell (KSH) utility script (`f_alis_msgerr.ksh`) serves as a standardized library for error handling, logging, and status management within Information Services. It provides a set of functions that interact with an Oracle database via SQL*Plus to manage a message table (`BERT_MELDUNG`). The primary purpose is to centralize and simplify error management processes, including generating unique entry numbers, creating message entries, logging various error types (Fatal, Error, Warning), setting job statuses (OK or Aborted), building log filenames, and appending timing information to message entries. The scope of this migration is to re-platform this utility functionality from a KSH script interacting with Oracle to a BigQuery-native solution, primarily using BigQuery Stored Procedures and BigQuery SQL.

## 2. Source Inventory
The job `5af228f1` consists of a single source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   **Technology:** KornShell (KSH)
    *   **Category:** shell (utility script)
    *   **Summary:** KornShell utility script providing functions for error handling, logging, and status management in Information Services, interacting with an Oracle database via SQL*Plus.
    *   **Complexity Tier:** medium
    *   **Migration Bucket:** semi_auto (B2)

The script defines the following key functions:
*   `DWMSG_Fehlerbehandlung`: Handles errors detected by the shell.
*   `DWMSG_SetzeStatusOK`: Sets a message entry's status to 'OK'.
*   `DWMSG_SetzeStatusAbbruch`: Sets a message entry's status to 'Abbruch' (aborted).
*   `DWMSG_ErmittleNr`: Retrieves a unique entry number.
*   `DWMSG_ErzeugeEintrag`: Creates a new entry in the message table.
*   `DWMSG_MeldeFehler`: Logs an error message with additional details.
*   `DWMSG_Logdateiname`: Constructs a log filename.
*   `DWMSG_SetzeStichtagInfo`: Sets specific date information in a message entry.
*   `DWMSG_AppendTimingInfos`: Appends timing information to a message entry.

## 3. Target Architecture
The target architecture in Google Cloud will primarily leverage BigQuery Stored Procedures and a BigQuery table for message tracking.

*   **BigQuery Dataset:** A dedicated dataset (e.g., `data_operations_logs`) will host the message tracking table and related stored procedures.
*   **BigQuery Table:** A central message table (e.g., `data_operations_logs.message_table`) will store all message entries, statuses, error details, and additional information, replacing the Oracle `BERT_MELDUNG` table.
    *   **Schema (example):**
        ```sql
        dataset.message_table(
          eintrags_nr STRING,       -- Unique entry number
          job_kennung STRING,       -- Job identifier
          programmname STRING,      -- Program name
          logdatei STRING,          -- Log file path
          status STRING,            -- Status (e.g., 'OPEN', 'OK', 'ABBRUCH')
          fehler_typ STRING,        -- Error type (e.g., 'F', 'E', 'W')
          fehler_nr INT64,          -- Error code
          zusatz1 STRING,           -- Additional info field 1
          zusatz2 STRING,           -- Additional info field 2
          zusatzinfos STRING,       -- Generic additional info
          created_ts TIMESTAMP,     -- Record creation timestamp
          updated_ts TIMESTAMP      -- Record last updated timestamp
        )
        ```
*   **BigQuery Stored Procedures:** Each KornShell function (`DWMSG_*`) that interacts with the Oracle database will be migrated to a corresponding BigQuery Stored Procedure. These procedures will encapsulate the logic for creating, updating, and querying the `message_table`.
*   **Cloud Composer (Airflow) / Cloud Workflows:** For orchestration of calling these BigQuery Stored Procedures, especially if this utility is part of larger ETL workflows, Cloud Composer or Cloud Workflows can be used to manage the execution flow and exception handling.

## 4. Data Flow & Lineage
The original script's data flow involves:
1.  **Inputs:** Environment variables (`DW_ORAUSER`, `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DateFormat`) and function parameters.
2.  **Processing:** KornShell functions execute `sqlplus` commands to run SQL scripts (`d_alis_spaufruf_p1.sql`, `d_al_is_ermittlenr.sql`, `d_alis_spaufruf_p4.sql`) or directly execute Oracle PL/SQL procedures (`BERT_MELDUNG.SetzeStatusOk`, `BERT_MELDUNG.Erzeuge_Eintrag`, etc.). Communication for `DWMSG_ErmittleNr` involves writing to and reading from a temporary file (`/tmp/ErmittleNr_$$.lst`).
3.  **Outputs:** Updates to the Oracle `BERT_MELDUNG` table. Derived log filenames are returned to the calling shell environment via variable assignment. Console messages are output for validation failures.

In the BigQuery target architecture, this data flow will be re-engineered:
*   **Inputs:** Function parameters will directly map to BigQuery Stored Procedure input parameters. Environment variables can be managed as BigQuery constants, configuration tables, or passed as parameters from the orchestrator.
*   **Processing:** The logic within each KSH function will be translated into BigQuery SQL within BigQuery Stored Procedures. Direct Oracle PL/SQL calls will be replaced by equivalent BigQuery SQL `INSERT` and `UPDATE` statements against the `message_table`. Temporary file-based communication (`DWMSG_ErmittleNr`) will be replaced by directly returning values from the stored procedure. Shell `eval` for variable assignment will be replaced by `OUT` parameters for stored procedures.
*   **Outputs:** All database updates will be directed to the `data_operations_logs.message_table`. Return values from BigQuery Stored Procedures (e.g., the unique ID from `DWMSG_ErmittleNr` or the generated filename from `DWMSG_Logdateiname`) will be passed back to the calling orchestrator.

## 5. Transformation Logic

Each KornShell function will be transformed into a BigQuery Stored Procedure, implementing its logic directly in BigQuery SQL.

*   **`DWMSG_Fehlerbehandlung` (Migrated to `dataset.DWMSG_Fehlerbehandlung`):**
    *   The BigQuery Stored Procedure will receive the `EintragsNr` as input.
    *   It will call other BigQuery Stored Procedures (`DWMSG_MeldeFehler` and `DWMSG_SetzeStatusAbbruch`) to log the error and update the status.
    *   The original shell's `$?` error code capture will need to be handled by the calling orchestration layer's exception handling, which then invokes this BigQuery procedure.
*   **`DWMSG_SetzeStatusOK` (Migrated to `dataset.DWMSG_SetzeStatusOK`):**
    *   Validates `EintragsNr`.
    *   `UPDATE` `dataset.message_table` to set `status = 'OK'` for the given `eintrags_nr`.
*   **`DWMSG_SetzeStatusAbbruch` (Migrated to `dataset.DWMSG_SetzeStatusAbbruch`):**
    *   Validates `EintragsNr`.
    *   `UPDATE` `dataset.message_table` to set `status = 'ABBRUCH'` for the given `eintrags_nr`.
*   **`DWMSG_ErmittleNr` (Migrated to `dataset.DWMSG_ErmittleNr`):**
    *   Instead of writing to a temporary file, this procedure will generate a unique identifier (e.g., using `GENERATE_UUID()` or a sequence-like mechanism) and return it directly as an `OUT` parameter.
*   **`DWMSG_ErzeugeEintrag` (Migrated to `dataset.DWMSG_ErzeugeEintrag`):**
    *   Validates input parameters (`EintragsNr`, `JobKennung`, `Programmname`, `LogDatei`).
    *   `INSERT` a new row into `dataset.message_table` with the provided details, initial `status = 'OPEN'`, and `created_ts`/`updated_ts` set to `CURRENT_TIMESTAMP()`.
*   **`DWMSG_MeldeFehler` (Migrated to `dataset.DWMSG_MeldeFehler`):**
    *   Accepts `EintragsNr`, `Typ`, `FehlerNr`, `Zusatz1`, `Zusatz2` as parameters.
    *   Determines the number of parameters similar to the original script to handle optional fields.
    *   `UPDATE` `dataset.message_table` to set `fehler_typ`, `fehler_nr`, `zusatz1`, and `zusatz2` for the specified `eintrags_nr`.
*   **`DWMSG_Logdateiname` (Migrated to `dataset.DWMSG_Logdateiname`):**
    *   Accepts `JobKennung` and `EintragsNr` as input parameters.
    *   Constructs the log filename string using BigQuery string functions (`CONCAT`, `FORMAT_TIMESTAMP`, `CURRENT_TIMESTAMP()`) and returns it as an `OUT` parameter. The `DW_DIR_PROT` equivalent will be a configurable BigQuery constant or parameter.
*   **`DWMSG_SetzeStichtagInfo` (Migrated to `dataset.DWMSG_SetzeStichtagInfo`):**
    *   Validates `EintragsNr`, `DWMSG_Stichtag`, and `DWMSG_StichtagFmt`.
    *   Uses `PARSE_DATE` to convert the `DWMSG_Stichtag` string to a date based on `DWMSG_StichtagFmt`.
    *   `UPDATE` `dataset.message_table` to set `zusatzinfos` with the formatted stichtag.
*   **`DWMSG_AppendTimingInfos` (Migrated to `dataset.DWMSG_AppendTimingInfos`):**
    *   Validates `EintragsNr` and `DWMSG_DateFormat`.
    *   Constructs the timing text using `CONCAT`, `FORMAT_TIMESTAMP`, and `CURRENT_TIMESTAMP()`.
    *   `UPDATE` `dataset.message_table` to append the `timing_text` to the existing `zusatzinfos` for the specified `eintrags_nr`.

## 6. External Dependencies
The original script has several external dependencies:

*   **Oracle Database / `BERT_MELDUNG` table:** This is the primary data store for message tracking.
    *   **Replacement:** Will be replaced by the `data_operations_logs.message_table` in BigQuery.
*   **Oracle PL/SQL Procedures (`BERT_MELDUNG.SetzeStatusOk`, etc.):** These are invoked via `sqlplus` from the shell script.
    *   **Replacement:** Each will be translated into equivalent BigQuery Stored Procedures, operating on the `data_operations_logs.message_table`.
*   **SQL Wrapper Scripts (`d_alis_spaufruf_p1.sql`, `d_al_is_ermittlenr.sql`, etc.):** These are SQL files executed by `sqlplus`.
    *   **Replacement:** The SQL logic within these scripts will be integrated directly into the body of the corresponding BigQuery Stored Procedures.
*   **`sqlplus` command-line client:** Used to execute SQL against Oracle.
    *   **Replacement:** Removed entirely, as BigQuery Stored Procedures are executed directly within BigQuery.
*   **Temporary filesystem (`/tmp/ErmittleNr_$$.lst`):** Used for inter-process communication for `DWMSG_ErmittleNr`.
    *   **Replacement:** Direct return values (OUT parameters) from BigQuery Stored Procedures will eliminate the need for temporary files.
*   **KornShell (ksh) specific features (`trap ERR`, `eval`, `typeset`):**
    *   **Replacement:** `trap ERR` will be handled by the orchestration layer's error handling. `eval` and `typeset` are language-specific constructs that will not have direct BigQuery SQL equivalents; instead, BigQuery Stored Procedure input/output parameters will manage data flow.
*   **OS utilities (`cat`, `tr`, `rm`, `date`):** Used for file manipulation and date formatting.
    *   **Replacement:** BigQuery SQL functions (`FORMAT_TIMESTAMP`, string manipulation functions) will replace these. File operations are eliminated.

## 7. Unresolved / Risks
*   **Comprehensive Error Handling:** The original `DWMSG_Fehlerbehandlung` function relies on `trap ERR` in KornShell. Replicating this exact behavior in a BigQuery-native context requires the calling orchestration layer (e.g., Cloud Composer) to implement robust error catching and then invoke the BigQuery error logging procedures. This needs careful design in the orchestrator.
*   **Dynamic SQL (`d_alis_spaufruf_p${NumParm}.sql`):** The `DWMSG_MeldeFehler` function dynamically selects a SQL script based on the number of parameters. This dynamic behavior will need to be translated into conditional logic (e.g., `IF/ELSEIF` statements) within the BigQuery Stored Procedure, or separate procedures called based on logic. The provided pseudocode already accounts for this.
*   **Implicit Oracle Behavior:** Any subtle Oracle-specific behavior within the `BERT_MELDUNG` package that is not explicitly captured in the provided KSH script will need to be identified and replicated or replaced with equivalent BigQuery functionality.
*   **`Zusatzinfos` Data Type and Structure:** The original `zusatzinfos` in Oracle might have a specific structure or be free-form. The migration assumes a `STRING` type in BigQuery. If it contains structured data, further parsing/structuring might be needed in BigQuery.
*   **Notification Mechanisms:** The original script mentions mail and Robomon notifications in the comments for `DWMSG_Fehlerbehandlung`. If these are active, they are external to the script's core logic and will need to be re-implemented using Google Cloud services (e.g., Cloud Functions, Pub/Sub, SendGrid) and integrated with the orchestration layer.

## 8. Build Plan
The build plan will focus on creating the necessary BigQuery components.

1.  **BigQuery Dataset Creation:**
    *   Create a BigQuery dataset, e.g., `data_operations_logs`, to house the message table and stored procedures.
    *   **Language:** DDL (BigQuery SQL)

2.  **`message_table` DDL Creation:**
    *   Define the schema for `data_operations_logs.message_table` as outlined in Section 3.
    *   **Language:** DDL (BigQuery SQL)

3.  **BigQuery Stored Procedure Implementations:**
    *   Translate each KSH function into a BigQuery Stored Procedure, adhering to the logic and pseudocode provided by the `shellscript_to_bqsql_design` tool.
    *   **Procedures to create:**
        *   `data_operations_logs.DWMSG_ErmittleNr`
        *   `data_operations_logs.DWMSG_Logdateiname`
        *   `data_operations_logs.DWMSG_ErzeugeEintrag`
        *   `data_operations_logs.DWMSG_MeldeFehler`
        *   `data_operations_logs.DWMSG_SetzeStatusOK`
        *   `data_operations_logs.DWMSG_SetzeStatusAbbruch`
        *   `data_operations_logs.DWMSG_SetzeStichtagInfo`
        *   `data_operations_logs.DWMSG_AppendTimingInfos`
        *   `data_operations_logs.DWMSG_Fehlerbehandlung` (as a wrapper/coordinator)
    *   **Language:** BigQuery SQL (Stored Procedure syntax)

4.  **Orchestration Layer Integration (if applicable):**
    *   Develop Cloud Composer DAGs or Cloud Workflows to call these BigQuery Stored Procedures as needed within the migrated ETL jobs.
    *   Implement error handling in the orchestrator to correctly invoke `DWMSG_Fehlerbehandlung` on job failures.
    *   **Language:** Python (for Cloud Composer) or YAML (for Cloud Workflows).

5.  **Configuration Management:**
    *   Establish a method to manage configuration parameters (e.g., root directories, protocol directories, date formats) that were originally environment variables. This could be a BigQuery configuration table or parameters passed from the orchestration layer.
    *   **Language:** BigQuery SQL (for config table DDL/DML) or Python/YAML (for orchestrator parameters).