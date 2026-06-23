# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh

## 1. Purpose & Scope
This job, primarily orchestrated by the KornShell script `r_aurd_rechstan.ksh`, is responsible for generating a snapshot of invoice-related contract data from the Data Warehouse (DWH) to support demand scoring. It ensures that the data provided to the "Forderungsscoring" system is current and consistent based on a specified cutoff date (`Stichtag`). The process involves parsing input parameters, initializing the runtime environment and logging, determining or validating the cutoff date, and then invoking a core processing script (`k_aurd_rechstan.ksh`) with the derived parameters. The job handles restart/resume functionality, allowing processing to continue from a specific `DWH_VERTRAG_ID`.

## 2. Source Inventory
The assembled job consists of one primary component file:
*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh`**
    *   **Technology:** KornShell
    *   **Complexity Tier:** Not available (no data from `file_complexity`) - *Assumed Medium due to orchestration and external script invocation.*
    *   **Automation Bucket:** semi_auto
    *   **Summary:** This script acts as a wrapper, orchestrating the data extraction process. It handles parameter parsing, environment setup, logging, and error handling, before delegating the main data processing to another script.

The script `r_aurd_rechstan.ksh` explicitly invokes:
*   **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh`** (KornShell)
    *   This script, in turn, executes an SQL script:
        *   **`SCRIPT:D_AURD_RECHSTAN.SQL`**

## 3. Target Architecture
The migration target is Google BigQuery. The existing KornShell-based orchestration and SQL execution pattern will be re-architected into BigQuery stored procedures, complemented by BigQuery tables for logging and configuration, and potentially an external orchestrator like Cloud Composer for scheduling.

*   **BigQuery Stored Procedures:**
    *   **`project.dataset.erzeugung_abzug_rechnungsdaten`**: This will be the main orchestrating stored procedure, replacing `r_aurd_rechstan.ksh`. It will handle parameter parsing, date logic, and invocation of the core processing logic.
    *   **`project.dataset.k_aurd_rechstan`**: This stored procedure will encapsulate the core data extraction and transformation logic, replacing `k_aurd_rechstan.ksh` and its execution of `D_AURD_RECHSTAN.SQL`.
*   **BigQuery Tables:**
    *   **`project.dataset.job_log`**: An audit table to store job execution logs, status updates, and error messages, replacing file-based logging.
    *   **`project.dataset.job_status`**: A control table to track the overall status of the job, similar to what the `DWMSG_SetzeStatusOK` framework call implies.
    *   **`project.dataset.target_table`**: The target table for the extracted invoice data (e.g., `FOS-Tabelle` mentioned in the source script).
    *   **`project.dataset.source_table`**: The source table(s) in BigQuery that correspond to the `DWH` contract cache tables.
*   **Orchestration (Optional but Recommended):** Cloud Composer (Apache Airflow) could be used to schedule and monitor the execution of the BigQuery stored procedures, providing robust scheduling, dependency management, and monitoring capabilities that replace the original ksh scheduling.

## 4. Data Flow & Lineage
**Current Data Flow (Legacy):**
1.  **`r_aurd_rechstan.ksh`** (Orchestration):
    *   Reads environment variables (`$HOME/.dw_init`, `$BERT_DIR_ROOT`).
    *   Loads helper scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
    *   Parses command-line arguments: `-s Stichtag` (cutoff date) and `-l Wiederanlaufwert` (restart value).
    *   Determines `v_sysdate` and `p_stichtag`.
    *   Initializes logging via `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`.
    *   Invokes (`INVOKES`) the core script:
2.  **`k_aurd_rechstan.ksh`** (Core Logic Execution):
    *   Executes SQL code from **`D_AURD_RECHSTAN.SQL`**.
    *   This SQL code is assumed to read from DWH contract cache tables (`READS`) and write to the FOS target table (`WRITES`).

**Proposed Data Flow (BigQuery):**
1.  **(Optional) Cloud Composer DAG:** Triggers the main BigQuery stored procedure.
2.  **`project.dataset.erzeugung_abzug_rechnungsdaten`** (Orchestration SP):
    *   Accepts `p_stichtag` and `p_wiederanlaufWert` as parameters.
    *   Derives `v_sysdate` using `CURRENT_DATE()` and `FORMAT_DATE()`.
    *   Implements `Stichtag` fallback logic (if required).
    *   Logs job start and parameters into `project.dataset.job_log`.
    *   Calls (`CALLS`) the core processing stored procedure:
3.  **`project.dataset.k_aurd_rechstan`** (Core Logic SP):
    *   Accepts `p_job_kennung`, `p_stichtag`, `p_fehler_nr`, `p_wiederanlaufWert` as parameters.
    *   If `p_wiederanlaufWert > 0`, it `DELETE`s records from `project.dataset.target_table` where `dwh_vertrag_id >= p_wiederanlaufWert`.
    *   `INSERT`s data into `project.dataset.target_table` by `SELECT`ing from `project.dataset.source_table`. The selection criteria will include date filters (`gueltig_von`, `gueltig_bis`, `ladedatum`) and the `dwh_vertrag_id > p_wiederanlaufWert` filter.
    *   Logs core extraction completion into `project.dataset.job_log`.
4.  **`project.dataset.erzeugung_abzug_rechnungsdaten`** (Orchestration SP continues):
    *   Logs successful completion into `project.dataset.job_log`.
    *   Updates job status in `project.dataset.job_status` to 'OK'.

## 5. Transformation Logic
The transformation involves translating the shell script's control flow and parameter handling into BigQuery SQL procedural logic, and the embedded SQL into native BigQuery SQL.

**`r_aurd_rechstan.ksh` (Wrapper Logic) -> `project.dataset.erzeugung_abzug_rechnungsdaten` (BigQuery Stored Procedure):**
*   **Parameter Handling:** `getopts` logic will be replaced by direct stored procedure input parameters (`p_stichtag STRING`, `p_wiederanlaufWert INT64`).
*   **Environment Initialization:** Sourcing `.dw_init` and other helper scripts will be replaced by:
    *   Defining constants or reading from configuration tables for paths (`BERT_DIR_ROOT`) or job metadata (`JobKennung`).
    *   Integrating helper logic (e.g., date formatting) directly into the stored procedure or using BigQuery's built-in functions.
*   **Date Determination:** The `DWDate_Gib_Zeitraum` call for `v_sysdate` will be translated to `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`. The conditional logic for `p_stichtag` (if not provided, derive from `MIN(sysdate, max_ladedatum)`) will be implemented using `IF` statements and `LEAST(CURRENT_DATE(), MAX(DATE(ladedatum)))` on the source table.
*   **Error Handling & Logging:** Shell traps (`trap`) will be replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END;` blocks for error trapping within the procedure. Logging calls (`DWMSG_*`) will be converted to `INSERT` statements into the `project.dataset.job_log` table. Parameter validation failures will `RAISE` an error.
*   **Job Status:** `DWMSG_SetzeStatusOK` will become an `UPDATE` statement on the `project.dataset.job_status` table.

**`k_aurd_rechstan.ksh` + `D_AURD_RECHSTAN.SQL` -> `project.dataset.k_aurd_rechstan` (BigQuery Stored Procedure):**
*   **Restart Logic:** The `Wiederanlaufwert` logic, where records with `DWH_VERTRAG_ID >= restart_value` are deleted and then re-inserted, will be translated into a `DELETE` statement followed by an `INSERT` statement within the stored procedure.
*   **Data Selection Filters:** The SQL predicates `Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag` will be directly translated into BigQuery `WHERE` clauses using `DATE` conversions and comparisons. The `dwh_vertrag_id > restart_value` will also be part of the `WHERE` clause during the `INSERT`.
*   **Data Flow:** The SQL will involve a direct `INSERT INTO ... SELECT FROM ...` pattern, reflecting the extraction and loading of data.

## 6. External Dependencies
The original script has several dependencies:
*   **Environment Initialization:** `. $HOME/.dw_init` - This will be replaced by BigQuery stored procedure parameters, configuration variables, or by defining constants within the stored procedure.
*   **Helper Scripts:**
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error/Message Framework)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Parameter Helper)
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date Helper)
    These helper script functionalities will be incorporated directly into the BigQuery stored procedures logic using BigQuery SQL functions, control flow statements (`IF`, `CASE`), and insertions into logging/audit tables. Specific framework calls like `DWMSG_ErmittleNr`, `pruefeParameterGesetzt`, etc., will be reimplemented as BigQuery SQL logic manipulating the audit tables.
*   **Kernel Script Invocation:** `${Name_Kernskript}` (`k_aurd_rechstan.ksh`) - This will be replaced by a `CALL` statement to the `project.dataset.k_aurd_rechstan` BigQuery stored procedure.

No external systems (like Oracle, SFTP, S3) were explicitly identified in `lineage_assembled_jobs`, meaning the data sources are considered internal to the DWH environment.

## 7. Unresolved / Risks
*   **Missing Complexity Data:** The `file_complexity` analysis for `r_aurd_rechstan.ksh` returned no rows. This means specific migration flags or detailed complexity dimensions are unknown, potentially leading to underestimation of effort if unexpected complexities arise within the script's logic.
*   **Commented-out `p_stichtag` Fallback:** The source code contains commented-out logic (`FOSHoleLadedatum "DWH\\$TA_C_VERTRAG" v_ladedatum`) for deriving `p_stichtag` if not provided. It's crucial to confirm if this fallback is still a business requirement. If so, it needs to be explicitly implemented in the BigQuery stored procedure, performing `MIN(CURRENT_DATE(), MAX(ladedatum))` from the relevant BigQuery source table.
*   **Shell Traps:** The `trap` commands in the KornShell script (for `INT`, `STOP`, `CONT`, `ERR`) handle process signals. BigQuery stored procedures do not have a direct equivalent. Error handling will rely on `EXCEPTION` blocks within the procedure and potentially on external orchestrator (e.g., Airflow) retry mechanisms.
*   **Legacy Framework Functions:** The various `DWMSG_` and `DWDate_` functions need careful translation to BigQuery. While a general approach of logging to tables is proposed, specific functionalities (e.g., `DWMSG_ErmittleNr` for entry numbers) might need custom BigQuery functions or careful mapping to auto-incrementing IDs or sequences if exact behavior is required.
*   **SQL in `D_AURD_RECHSTAN.SQL`:** The actual SQL logic within `D_AURD_RECHSTAN.SQL` needs to be fully analyzed and converted to BigQuery SQL, considering syntax, data types, and any proprietary functions. This analysis was not part of the current scope but is critical for the `k_aurd_rechstan` stored procedure.

## 8. Build Plan
1.  **Define BigQuery Schema for Logging and Status Tables:**
    *   Create `project.dataset.job_log` table DDL.
    *   Create `project.dataset.job_status` table DDL.
    *   Language: BigQuery DDL
2.  **Migrate Core Logic to BigQuery Stored Procedure:**
    *   Analyze `k_aurd_rechstan.ksh` and `D_AURD_RECHSTAN.SQL` to extract the exact SQL logic and any conditional processing.
    *   Develop `project.dataset.k_aurd_rechstan` stored procedure. This procedure will encapsulate the `DELETE` and `INSERT...SELECT` logic, including parameters for the job identifier, cutoff date, error number, and restart value.
    *   Language: BigQuery SQL
3.  **Migrate Orchestration Logic to BigQuery Stored Procedure:**
    *   Develop `project.dataset.erzeugung_abzug_rechnungsdaten` stored procedure.
    *   Implement parameter parsing, date derivation, and parameter validation.
    *   Integrate logging calls (`INSERT` into `job_log`) and status updates (`UPDATE` `job_status`).
    *   Include the `CALL` statement to `project.dataset.k_aurd_rechstan`.
    *   Language: BigQuery SQL
4.  **Develop Cloud Composer DAG (Optional):**
    *   Create an Airflow DAG that schedules and executes `project.dataset.erzeugung_abzug_rechnungsdaten`, passing necessary runtime parameters.
    *   Language: Python
5.  **Data Validation and Testing:**
    *   Develop test cases to compare the output of the migrated BigQuery solution with the legacy system, especially concerning data content, restart behavior, and error handling.