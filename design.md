# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

## 1. Purpose & Scope
The KornShell script `f_alis_msgerr.ksh` (originally `dwmsg.ksh`) is a utility library providing helper routines for error management and logging within the Information Services platform. It standardizes and simplifies error handling by interacting with an Oracle database via `sqlplus` to manage a message table. The script defines functions for:
*   Error handling (`DWMSG_Fehlerbehandlung`): Called on script errors to log details and set status to 'Abbruch'.
*   Status updates (`DWMSG_SetzeStatusOK`, `DWMSG_SetzeStatusAbbruch`): Marks job entries in the message table as successful or aborted.
*   Entry management (`DWMSG_ErmittleNr`, `DWMSG_ErzeugeEintrag`): Generates unique entry numbers and creates new entries in the message table with job metadata.
*   Error reporting (`DWMSG_MeldeFehler`): Logs specific error details (type, number, additional info) to the message table.
*   Log filename generation (`DWMSG_Logdateiname`): Constructs a log filename based on job ID, timestamp, and entry number.
*   Additional info logging (`DWMSG_SetzeStichtagInfo`, `DWMSG_AppendTimingInfos`): Records date-specific or general timing information in the message table.

The scope of this migration is to re-implement the functionality of this KornShell script and its Oracle interactions within the Google Cloud Platform, specifically leveraging BigQuery for data persistence and BigQuery Scripting/Stored Procedures for procedural logic.

## 2. Source Inventory
The job consists of a single source file:
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh`
    *   **Technology:** KornShell
    *   **Tool:** KornShell
    *   **Summary:** KornShell utility script providing functions for error handling, logging, and status management in Information Services, interacting with an Oracle database via SQL*Plus.
    *   **Complexity Tier:** (Metadata missing - implied medium due to `sqlplus` interactions and procedural logic)
    *   **Automation Bucket:** (Metadata missing - estimated Semi-Auto (B2) given the need to translate shell logic and Oracle PL/SQL calls to BigQuery, with some patterns being automatable but requiring review and manual intervention for the PL/SQL conversion and orchestration aspects).
    *   **File Purpose:** (Metadata missing - identified as `Utility / Library`).

## 3. Target Architecture
The migrated solution will primarily reside within BigQuery, utilizing:
*   **BigQuery Datasets:** To host the migrated tables. A `dataset` will be created (e.g., `dw_is_error_management`).
*   **BigQuery Tables:**
    *   `dw_is_error_management.message_table`: To replace the Oracle message table (`BERT_MELDUNG`). This table will store job status, error details, and additional information.
    *   `dw_is_error_management.message_log`: To store detailed error log entries.
    *   `dw_is_error_management.message_sequence`: A table or sequence generator to provide unique `EintragsNr` values, replicating the Oracle sequence functionality.
*   **BigQuery Stored Procedures:** Each KornShell function (`DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`, etc.) will be reimplemented as a BigQuery Stored Procedure, wrapping the DML operations on the `message_table` and `message_log`.
*   **Orchestration Layer:** An external orchestration tool (e.g., Cloud Composer/Apache Airflow, Cloud Workflows) will be used to invoke these BigQuery Stored Procedures, replacing the shell script's role as the primary executor. This layer will also handle parameter passing and error handling not directly managed within BigQuery.

## 4. Data Flow & Lineage
The original script did not have explicit lineage edges detected within the provided `run_id`, indicating it functions as a utility library invoked by other processes rather than a standalone data pipeline.
The data flow in the target architecture will be:
1.  **Invoking Process (e.g., another Airflow DAG, Cloud Workflow):** Calls the BigQuery Stored Procedures.
2.  **BigQuery Stored Procedures:**
    *   **Reads:**
        *   `dw_is_error_management.message_sequence` (for `DWMSG_ErmittleNr`)
        *   `dw_is_error_management.message_table` (for status updates, appending info)
    *   **Writes:**
        *   `dw_is_error_management.message_table` (inserts new entries, updates status, updates additional info, updates error details)
        *   `dw_is_error_management.message_log` (inserts error log entries)
3.  **BigQuery Functions:** Internal functions like `FORMAT_TIMESTAMP` and `PARSE_DATE` will be used for data manipulation.

**Execution Order:** The BigQuery Stored Procedures will be called sequentially or conditionally by the orchestration layer, mirroring the logic of how the original KornShell functions were invoked. For example, a wrapper script or DAG would:
*   Call `DWMSG_ErmittleNr` to get a new entry ID.
*   Call `DWMSG_Logdateiname` to construct a log filename.
*   Call `DWMSG_ErzeugeEintrag` to create the initial message table entry.
*   Execute main job logic.
*   Call `DWMSG_SetzeStatusOK` on success or `DWMSG_Fehlerbehandlung` on error.

## 5. Transformation Logic
Each KornShell function will be transformed into an equivalent BigQuery Stored Procedure.

*   **`DWMSG_Fehlerbehandlung(p_EintragsNr INT64)`:**
    *   Captures the error code using `@@error.code` within a BigQuery Script.
    *   Calls `dataset.DWMSG_MeldeFehler` to log the error.
    *   Calls `dataset.DWMSG_SetzeStatusAbbruch`.
    *   Replaces shell `echo` with `SELECT 'message' AS msg;`.
*   **`DWMSG_SetzeStatusOK(p_EintragsNr INT64)`:**
    *   Validates `p_EintragsNr` using `IF p_EintragsNr IS NULL THEN SIGNAL SQLSTATE '45000' ... END IF;`.
    *   `UPDATE dataset.message_table SET status = 'OK', updated_at = CURRENT_TIMESTAMP() WHERE eintragsnr = p_EintragsNr;`.
*   **`DWMSG_SetzeStatusAbbruch(p_EintragsNr INT64)`:**
    *   Similar validation as `DWMSG_SetzeStatusOK`.
    *   `UPDATE dataset.message_table SET status = 'ABBRUCH', updated_at = CURRENT_TIMESTAMP() WHERE eintragsnr = p_EintragsNr;`.
*   **`DWMSG_ErmittleNr(OUT p_VarName INT64)`:**
    *   Replaces temporary file logic with a direct `SELECT nextval INTO p_VarName FROM dataset.message_sequence LIMIT 1;` or similar mechanism for sequence generation in BigQuery.
*   **`DWMSG_ErzeugeEintrag(p_EintragsNr INT64, p_JobKennung STRING, p_Programmname STRING, p_LogDatei STRING)`:**
    *   Validates `p_EintragsNr`.
    *   `INSERT INTO dataset.message_table (...) VALUES (...);`.
*   **`DWMSG_MeldeFehler(p_EintragsNr INT64, p_Typ STRING, p_FehlerNr INT64, p_Zusatz1 STRING, p_Zusatz2 STRING)`:**
    *   Validates `p_EintragsNr`.
    *   Conditional logic (`IF...THEN...ELSEIF...END IF`) to handle optional parameters.
    *   `INSERT INTO dataset.message_log (...) VALUES (...);`.
    *   `UPDATE dataset.message_table SET last_error_type = p_Typ, ... WHERE eintragsnr = p_EintragsNr;`.
*   **`DWMSG_Logdateiname(OUT p_VarName STRING, p_JobKennung STRING, p_EintragsNr INT64)`:**
    *   `SET p_VarName = CONCAT('/path/to/prot/', p_JobKennung, '_', FORMAT_TIMESTAMP('%Y%m%d_%H%M', CURRENT_TIMESTAMP()), '_', CAST(p_EintragsNr AS STRING), '.log');`.
    *   The `/path/to/prot/` prefix should be configurable.
*   **`DWMSG_SetzeStichtagInfo(p_EintragsNr INT64, p_Stichtag STRING, p_StichtagFmt STRING)`:**
    *   Validates all parameters.
    *   `DECLARE parsed_date DATE; SET parsed_date = PARSE_DATE(p_StichtagFmt, p_Stichtag);`.
    *   `UPDATE dataset.message_table SET zusatzinfos_date = parsed_date, updated_at = CURRENT_TIMESTAMP() WHERE eintragsnr = p_EintragsNr;`.
*   **`DWMSG_AppendTimingInfos(p_EintragsNr INT64, p_InfoText STRING, p_DateFormat STRING)`:**
    *   Validates `p_EintragsNr` and `p_DateFormat`.
    *   `UPDATE dataset.message_table SET zusatzinfos_text = CONCAT(COALESCE(zusatzinfos_text, ''), p_InfoText, ' ', FORMAT_TIMESTAMP(p_DateFormat, CURRENT_TIMESTAMP()), ' '), updated_at = CURRENT_TIMESTAMP() WHERE eintragsnr = p_EintragsNr;`.

## 6. External Dependencies
The original script has the following external dependencies:

*   **Oracle Database:** Interacted via `sqlplus` and specific PL/SQL procedures (`BERT_MELDUNG` package: `SetzeStatusOk`, `SetzeStatusAbbruch`, `Erzeuge_Eintrag`, `Fehler`, `SetzeZusatzInfos`).
    *   **Replacement:** The Oracle database will be replaced by BigQuery tables (`message_table`, `message_log`, `message_sequence`) and BigQuery Stored Procedures that replicate the logic of the `BERT_MELDUNG` package. The schema of `BERT_MELDUNG` and its procedures need to be analyzed separately to ensure accurate translation to BigQuery.
*   **Filesystem (temporary files and log directory):** Used for temporary file communication in `DWMSG_ErmittleNr` and for constructing log file paths using `DW_DIR_PROT`.
    *   **Replacement:**
        *   Temporary file usage will be replaced by direct variable assignment in BigQuery SQL Scripting or by using BigQuery's native capabilities for temporary data handling.
        *   Log directory paths like `DW_DIR_PROT` will be replaced by configurable string parameters or environment variables in the orchestration layer, pointing to Cloud Storage buckets for logs if external logging is required. The constructed log filename itself (`DWMSG_Logdateiname`) will output a string value that can be used for reference.
*   **Environment Variables:** `DW_ORAUSER`, `DW_DIR_ROOT`, `DW_DIR_PROT`.
    *   **Replacement:** These will be translated into configuration parameters for the BigQuery Stored Procedures or set as environment variables within the orchestration environment (e.g., Airflow Variables, Cloud Composer Environment Variables).

## 7. Unresolved / Risks
*   **Missing Metadata:** `file_complexity` and `automation_rate` metadata was unavailable for this file. This implies a potential gap in automated assessment and the need for manual validation of complexity and migration effort.
*   **Oracle PL/SQL Logic Translation:** The actual logic within the Oracle PL/SQL package `BERT_MELDUNG` is not available. The BigQuery Stored Procedures created are based on the *assumed* functionality and parameters derived from the KornShell script's calls. A detailed analysis of the `BERT_MELDUNG` package's source code is crucial for accurate migration and to ensure all business rules are correctly replicated in BigQuery.
*   **Error Handling Granularity:** The KornShell `trap ERR` mechanism for error handling is a system-level feature. While BigQuery Scripting offers `EXCEPTION WHEN ERROR` blocks, the exact behavior and any custom error codes from the original Oracle PL/SQL need careful mapping to BigQuery's error handling.
*   **Dynamic SQL/Parameter Quoting:** The `DWMSG_MeldeFehler` function dynamically constructs the `sqlplus` command, including quoting for `Zusatz1` and `Zusatz2`. This needs to be carefully replicated in BigQuery to ensure proper handling of string parameters, especially if they contain special characters.
*   **External Notifications (Robomon):** The comments mention "Robomonbenachrichtigung" (Robomon notification) as a potential action in `DWMSG_Fehlerbehandlung`. If this is an active notification system, it needs to be identified and integrated with a GCP equivalent (e.g., Cloud Monitoring alerts, Pub/Sub triggered Cloud Functions/Workflows).

## 8. Build Plan
The migration build plan will focus on creating the BigQuery assets and setting up the orchestration.

1.  **Schema Definition (DDL):**
    *   Create `dw_is_error_management` BigQuery Dataset.
    *   Define and create the schema for `dw_is_error_management.message_table`.
    *   Define and create the schema for `dw_is_error_management.message_log`.
    *   Define and create a mechanism for `dw_is_error_management.message_sequence` (e.g., a table with a single row and an auto-incrementing ID, or a BigQuery sequence if available and suitable).
    *   **Language:** BigQuery DDL
2.  **BigQuery Stored Procedure Implementation:**
    *   Create BigQuery Stored Procedures for each of the nine `DWMSG_` functions, translating the shell logic and Oracle PL/SQL calls to BigQuery SQL and scripting constructs.
    *   **Language:** BigQuery SQL (Scripting and Stored Procedures)
3.  **Oracle PL/SQL `BERT_MELDUNG` Analysis and Translation:**
    *   Obtain the source code for the `BERT_MELDUNG` Oracle package.
    *   Thoroughly analyze its internal logic, table structures, and any specific error codes or business rules.
    *   Refine the BigQuery Stored Procedures based on this detailed analysis to ensure functional parity.
    *   **Language:** Manual analysis, BigQuery SQL.
4.  **Orchestration Layer Development:**
    *   Create an example orchestration script/DAG that demonstrates how to call the BigQuery Stored Procedures in a typical workflow, including error handling (`BEGIN...EXCEPTION...END`).
    *   Configure environment variables/parameters for BigQuery dataset, project, and log path replacements.
    *   **Language:** Python (for Airflow DAGs) or YAML (for Cloud Workflows).
5.  **Testing:**
    *   Develop unit tests for each BigQuery Stored Procedure.
    *   Develop integration tests to ensure the overall workflow (orchestration calling procedures) functions correctly.
    *   **Language:** BigQuery SQL (for stored procedure testing), Python (for orchestration testing).