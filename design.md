# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh

## 1. Purpose & Scope
This KornShell script, `r_ausd_v_ta_p_discount_rr.ksh`, serves as an orchestration wrapper for a contract data reconciliation process, specifically for the table `ta_p_discount_rr`. Its primary purpose is to handle environment setup, parse command-line parameters, initialize logging and error handling, and invoke a core processing script, `k_ausd_v_ta_p_discount_rr.ksh`. The script ensures proper execution flow, error trapping, and logging for the overall job. It does not perform any direct data transformations itself but manages the execution of the main data processing logic.

## 2. Source Inventory
| File Name                                                                     | Technology | Tool      | Tier   | Automation Bucket | Summary                                                                                                                                                                                                                                                                                          |
|:------------------------------------------------------------------------------|:-----------|:----------|:-------|:------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh` | shell      | KornShell | medium | semi_auto         | This KornShell script acts as a wrapper for the 'k_ausd_v_ta_p_discount_rr.ksh' core script, handling environment setup, parameter parsing, error trapping, and logging for the 'ta_p_discount_rr' data reconciliation process. It is primarily an orchestration script without direct data manipulation. |

## 3. Target Architecture
The target architecture in BigQuery will involve:
*   **BigQuery Stored Procedure**: A main stored procedure, `project.dataset.vertragsdatenabgleich_wrapper`, will replace the KornShell wrapper script. This procedure will encapsulate the parameter parsing, logging initialization, and the invocation of the core logic.
*   **BigQuery Audit/Log Tables**: Dedicated BigQuery tables (e.g., `project.dataset.job_error_log`, `project.dataset.job_log`, `project.dataset.job_status`, `project.dataset.job_stichtag`, `project.dataset.job_entry_sequence`) will replace the file-based logging mechanism. These tables will store job execution details, status updates, and error messages.
*   **Core Logic Stored Procedure**: The functionality of the invoked core script (`k_ausd_v_ta_p_discount_rr.ksh`) will be migrated into a separate BigQuery stored procedure (e.g., `project.dataset.k_ausd_v_ta_p_discount_rr`) that is called by the wrapper procedure.
*   **Configuration**: Environment variables and sourced utility script functionalities will be replaced by BigQuery procedure parameters, declared variables, or potentially a configuration table.

## 4. Data Flow & Lineage
The original script (`r_ausd_v_ta_p_discount_rr.ksh`) is an orchestration layer. Its primary data flow involves:
1.  **Environment Initialization**: Sourcing of several KornShell utility scripts (`. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`). These provide common functions and environment variables.
2.  **Parameter Parsing**: Reads command-line arguments, primarily looking for a help flag (`-h`).
3.  **Logging Setup**: Initializes a logging framework by calling functions like `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, and `DWMSG_SetzeStichtagInfo` which write job metadata to a log file.
4.  **Core Script Invocation**: Executes the main data processing script `k_ausd_v_ta_p_discount_rr.ksh`, passing job identification parameters. This is the crucial invocation point for the actual data reconciliation.
5.  **Status Reporting**: Upon successful completion or error, it logs the final status using `DWMSG_SetzeStatusOK` or `DWMSG_MeldeFehler`.

In the BigQuery target, this flow will be:
*   The `project.dataset.vertragsdatenabgleich_wrapper` stored procedure will be the entry point.
*   It will declare and set variables for job identification, dates, and configuration (replacing environment sourcing).
*   It will interact with the BigQuery audit/log tables to record job start, parameters, and status.
*   It will then call the `project.dataset.k_ausd_v_ta_p_discount_rr` stored procedure (the migrated core logic).
*   Error handling will be managed via BigQuery's `EXCEPTION WHEN ERROR` blocks, writing to error log tables.

## 5. Transformation Logic
The KornShell wrapper script itself performs minimal "transformations," primarily related to system metadata:
*   **Job Identifier Uppercasing**: `typeset -u JobKennung="BERT_V_TA_P_DISCOUNT_RR"`
*   **System Date Formatting**: `v_sysdate=$(date +%d%m%Y)`
*   **Log Entry Number Retrieval**: `DWMSG_ErmittleNr DW_EintragsNr` (presumably an incrementing sequence).
*   **Log File Name Generation**: `DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr`.
*   **Parameter Validation**: Checks for missing or unknown arguments from `getopts`.

The BigQuery SQL pseudocode outlines the transformation of these logic elements:
*   **Procedure Parameters**: Command-line parameters like `-s`, `-l`, `-h` will become `IN` parameters to the stored procedure.
*   **Variable Declarations**: Shell variables will be replaced by `DECLARE` statements in BigQuery.
*   **Date Functions**: `$(date +%d%m%Y)` translates to `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
*   **Conditional Logic**: Shell `if [ ! $ErrNr -eq 0 ]` becomes `IF ErrNr != 0 THEN ... END IF;`.
*   **Logging**: `print` and `tee -a $LogDatei` are replaced by `INSERT INTO` statements into audit tables.
*   **Error Handling**: Shell `trap` mechanisms are replaced by BigQuery `BEGIN...EXCEPTION WHEN ERROR THEN...END;` blocks, writing to `job_error_log`.
*   **Core Script Call**: The `${Name_Kernskript}` invocation becomes `CALL project.dataset.k_ausd_v_ta_p_discount_rr(JobKennung, DW_EintragsNr);`.

## 6. External Dependencies
The `lineage_assembled_jobs` query did not identify any direct external systems. However, based on the script's content, there are implicit dependencies:
*   **Sourced KornShell Utilities**: `. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`. These scripts provide common functions and environment setup.
    *   **Replacement Strategy**: The critical functionalities from these scripts (e.g., parameter handling, date formatting, error messaging) will need to be re-implemented directly within the BigQuery stored procedures or defined as BigQuery user-defined functions (UDFs) if reusable logic is identified. Environment variables will be replaced by BigQuery variables or configuration tables.
*   **Core Processing Script**: `k_ausd_v_ta_p_discount_rr.ksh`.
    *   **Replacement Strategy**: This script contains the actual business logic for data reconciliation. It must be fully migrated to a BigQuery stored procedure (`project.dataset.k_ausd_v_ta_p_discount_rr`) or a set of BigQuery SQL queries/procedures.

## 7. Unresolved / Risks
*   **Core Script Migration**: The most significant unresolved item is the migration of the core script, `k_ausd_v_ta_p_discount_rr.ksh`. The current design only provides a wrapper; the details of the core logic will need its own dedicated migration design. This script is expected to contain the actual SQL or data processing logic for `ta_p_discount_rr`.
*   **Sourced Utility Logic**: The exact functions and environment variables provided by the sourced utility scripts (`. $HOME/.dw_init`, etc.) need further analysis to ensure all necessary functionalities are replicated or accounted for in BigQuery.
*   **DWMSG Framework**: The `DWMSG` logging and error handling framework is custom. While the BigQuery pseudocode provides a mapping to audit tables, a complete understanding and re-implementation of all `DWMSG` functions (`DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, etc.) will be crucial. This might involve creating several BigQuery stored procedures or UDFs to mimic the behavior.
*   **Sequential Job Numbers**: The `DWMSG_ErmittleNr` function suggests a mechanism for generating sequential job entry numbers. In BigQuery, this would require either a sequence table with atomic updates or using a robust ID generation strategy (e.g., `GENERATE_UUID()`, `ROW_NUMBER()` over an ordered set in an audit table, or a custom sequence generator). The current pseudocode uses `COALESCE(MAX(entry_nr), 0) + 1` which is susceptible to race conditions and may need refinement for production.
*   **Error Trapping Semantics**: While BigQuery's `EXCEPTION WHEN ERROR` provides error handling, the exact behavior of KornShell `trap` (e.g., `INT` signal handling) may have nuances that require careful consideration during re-implementation.

## 8. Build Plan
The build plan will proceed in the following order:

1.  **Define BigQuery Audit Schema**:
    *   Create tables: `project.dataset.job_error_log`, `project.dataset.job_log`, `project.dataset.job_status`, `project.dataset.job_stichtag`, `project.dataset.job_entry_sequence`. (Language: BigQuery DDL)
2.  **Migrate Utility Functions**:
    *   Analyze the content of `$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`.
    *   Implement necessary functionalities (e.g., date formatting, parameter parsing helpers) as BigQuery UDFs or small helper stored procedures if they are generic and reusable. (Language: BigQuery SQL)
3.  **Migrate DWMSG Logging Framework**:
    *   Implement BigQuery stored procedures for `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK` that interact with the newly created audit tables. This will centralize logging logic. (Language: BigQuery SQL)
4.  **Design and Migrate Core Script `k_ausd_v_ta_p_discount_rr.ksh`**:
    *   This is a separate, significant migration effort. The core script's logic must be translated into a BigQuery stored procedure: `project.dataset.k_ausd_v_ta_p_discount_rr`. This will likely involve data extraction, transformation, and loading (ETL) logic. (Language: BigQuery SQL)
5.  **Implement Wrapper Stored Procedure**:
    *   Create the main wrapper stored procedure: `project.dataset.vertragsdatenabgleich_wrapper`.
    *   Integrate parameter handling, calls to the migrated `DWMSG` procedures, and the call to the core `k_ausd_v_ta_p_discount_rr` stored procedure, using the provided BigQuery SQL pseudocode as a guide.
    *   Refine the job entry number generation (`DW_EintragsNr`) to be robust in a multi-user/concurrent environment if necessary. (Language: BigQuery SQL)
6.  **Orchestration Integration**:
    *   If part of a larger workflow, integrate `project.dataset.vertragsdatenabgleich_wrapper` into an orchestration tool like Cloud Composer (Airflow) or Dataform. This will replace the UC4 scheduling context if applicable. (Language: Python for Airflow DAGs or SQLX for Dataform)