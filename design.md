# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh

## 1. Purpose & Scope
This job, `r_ausd_bp_ta_rn_vertrag.ksh`, is an initial provisioning process for selected base products (Basisprodukte) for the BERT system. Its primary purpose is to generate a daily snapshot of contract data (Vertrags-Cache) from the Data Warehouse (DWH) and make it available for credit scoring (Forderungsscoring). The process involves selecting records based on a cutoff date (`Stichtag`) and handling a restart value (`Wiederanlaufwert`). The data is aggregated at the contract level and written to a target table.

The main script orchestrates the execution of an internal control script (`k_ausd_bp_ta_rn_vertrag.ksh`), which in turn executes a SQL script (`d_ausd_bp_ta_rn_vertrag.sql`) to perform the core data transformation.

## 2. Source Inventory
| File Name | Technology | Tier | Automation Bucket | Purpose |
| :------------------------------------------------------------------ | :---------- | :----- | :----------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh` | KornShell | medium | semi_auto | Main wrapper script for parameter handling, logging initialization, and invoking the core processing script. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh` | KornShell | medium | semi_auto | Internal control script, responsible for further parameter validation, sourcing utility scripts, determining dates, and executing the SQL transformation script. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_rn_vertrag.sql` | Oracle SQL | medium | semi_auto | Core SQL script that performs data truncation, aggregation, and insertion into the target table. |

## 3. Target Architecture
The migration will target Google BigQuery. The existing KornShell scripts will be re-engineered into BigQuery Stored Procedures for orchestration and SQL for data transformation.

-   **BigQuery Stored Procedures:** The `r_ausd_bp_ta_rn_vertrag.ksh` and `k_ausd_bp_ta_rn_vertrag.ksh` scripts will be converted into separate BigQuery Stored Procedures (`project.dataset.ausd_bp_ta_rn_vertrag` and `project.dataset.k_ausd_bp_ta_rn_vertrag`) to handle parameter parsing, date calculations, and overall job flow control.
-   **BigQuery SQL:** The `d_ausd_bp_ta_rn_vertrag.sql` script will be translated into a BigQuery Stored Procedure (`project.dataset.d_ausd_bp_ta_rn_vertrag`) containing the core DML operations (TRUNCATE and INSERT with SELECT).
-   **Data Storage:** Source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_rn_einzeln`) and the target table (`sof$ta_rn_vertrag`) will be migrated to BigQuery tables within a specified `project.dataset`.
-   **Logging/Auditing:** A dedicated BigQuery `job_audit` table will be used to log job status, errors, and messages, replacing the shell-based logging mechanisms.

## 4. Data Flow & Lineage
The data flow of the original system is:
1.  `r_ausd_bp_ta_rn_vertrag.ksh` (main orchestrator)
2.  `r_ausd_bp_ta_rn_vertrag.ksh` invokes `k_ausd_bp_ta_rn_vertrag.ksh`
3.  `k_ausd_bp_ta_rn_vertrag.ksh` invokes `d_ausd_bp_ta_rn_vertrag.sql`
4.  `d_ausd_bp_ta_rn_vertrag.sql` reads from `isbert_schema.dwtk_meldungen` and `sof$ta_rn_einzeln`.
5.  `d_ausd_bp_ta_rn_vertrag.sql` writes to `sof$ta_rn_vertrag`.

In the BigQuery target architecture, this flow will be:
1.  `project.dataset.ausd_bp_ta_rn_vertrag` (orchestration stored procedure).
2.  `project.dataset.ausd_bp_ta_rn_vertrag` calls `project.dataset.k_ausd_bp_ta_rn_vertrag`.
3.  `project.dataset.k_ausd_bp_ta_rn_vertrag` calls `project.dataset.d_ausd_bp_ta_rn_vertrag`.
4.  `project.dataset.d_ausd_bp_ta_rn_vertrag` queries `project.dataset.dwtk_meldungen` and `project.dataset.sof_ta_rn_einzeln`.
5.  `project.dataset.d_ausd_bp_ta_rn_vertrag` truncates and inserts into `project.dataset.sof_ta_rn_vertrag`.
6.  Logging and error handling will update `project.dataset.job_audit`.

## 5. Transformation Logic
The core transformation logic resides in `d_ausd_bp_ta_rn_vertrag.sql`. It performs the following steps:
-   **Variable Definition:** Defines a `v_carmen` variable and determines a `v_datum` from `isbert_schema.dwtk_meldungen` based on `job_kennung = 'BERT_DROP_TEMP_TABLE'`. This `v_datum` needs to be reviewed as it appears unused in the subsequent SQL body.
-   **Truncation:** Empties the target table `sof$ta_rn_vertrag` using a `TRUNCATE TABLE` statement within a PL/SQL block (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`). This will be a direct `TRUNCATE TABLE` DML statement in BigQuery.
-   **Aggregation and Insertion:** Inserts data into `sof$ta_rn_vertrag` by selecting from `sof$ta_rn_einzeln`. The aggregation uses `MAX()` functions on multiple columns, grouped by `cntrct_id`, effectively collapsing multiple detail rows into a single contract-level summary row for various telephone/fax/data MSISDNs and statuses. This will be directly translated to BigQuery SQL `SELECT MAX(...) FROM ... GROUP BY ...`.
-   **Commit:** The original script includes a `COMMIT;` statement. In BigQuery, DML operations are atomic, so an explicit commit is not required.

## 6. External Dependencies
-   **Oracle Database:** The source system heavily relies on an Oracle database for table storage (`isbert_schema.dwtk_meldungen`, `sof$ta_rn_einzeln`, `sof$ta_rn_vertrag`) and PL/SQL procedures (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`).
    -   **Replacement:** All Oracle tables will be migrated to BigQuery tables. The PL/SQL procedure call will be replaced by a direct BigQuery DML `TRUNCATE TABLE`.
-   **Shell Utilities:**
    -   `$HOME/.dw_init`: Environment initialization.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling.
    -   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`: SQL*Plus execution helper.
    -   `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`: Date calculation for yesterday/today.
    -   **Replacement:** These utilities will be replaced by BigQuery Stored Procedure logic using built-in functions (`CURRENT_DATE()`, `DATE_SUB`, `FORMAT_DATE`, `SAFE.PARSE_DATE`), and for job metadata/logging, a dedicated `job_audit` table. Complex parameter parsing or advanced shell features may require a minimal Python orchestration layer (e.g., in Cloud Composer).
-   **SQL*Plus Spooling:** The `d_ausd_bp_ta_rn_vertrag.sql` script uses `spool` for tracing.
    -   **Replacement:** This will be replaced by structured logging to the `job_audit` table or by BigQuery's native query history and audit logs.

## 7. Unresolved / Risks
-   **`v_datum` variable usage:** The `v_datum` variable derived from `dwtk_meldungen` in `d_ausd_bp_ta_rn_vertrag.sql` appears unused in the provided SQL logic. Its intended use should be clarified; if it's meant for filtering or historization, that logic must be explicitly added to the BigQuery transformation.
-   **Restart Logic (`p_wiederanlaufWert`):** While parameters for a restart value (`p_wiederanlaufWert`) are passed, the provided SQL body does not explicitly implement the restart logic (i.e., only processing `DWH_VERTRAG_ID > Wiederanlaufwert` and deleting prior entries `>= value`). This needs to be carefully designed and implemented in BigQuery if it's a functional requirement.
-   **Post-processing scripts (commented out):** The `k_ausd_bp_ta_rn_vertrag.ksh` contains commented-out `sed`, `sort`, and `join` commands for post-processing and combining data files. If these steps are functionally required, they will need to be re-implemented in BigQuery (e.g., using SQL transformations or external tools like Dataflow for file processing). Based on the provided code, these are currently inactive.
-   **`trace.sql.cfg` and environment files:** The exact content and impact of `../trace.sql.cfg` and `.dw_init` are not fully known. Assumptions are made about their roles, but deeper analysis might be needed if they contain complex logic that affects job execution or data.
-   **`DW_DIR_UTL` variable:** The temporary file `tmpFile=\"$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_vertrag.tmp\"` relies on `DW_DIR_UTL`. This local file system interaction needs to be replaced, likely with in-memory operations within BigQuery procedures or GCS for temporary file storage if necessary.
-   **Oracle-specific hints:** Oracle hints like `/*+ full(rp) parallel(rp,4) */` will be ignored in BigQuery, and BigQuery's optimizer will handle query execution.

## 8. Build Plan
The migration will involve creating BigQuery objects in the following order:

1.  **Define BigQuery Tables:**
    *   `project.dataset.dwtk_meldungen` (schema matching source `isbert_schema.dwtk_meldungen`)
    *   `project.dataset.sof_ta_rn_einzeln` (schema matching source `sof$ta_rn_einzeln`)
    *   `project.dataset.sof_ta_rn_vertrag` (schema matching target `sof$ta_rn_vertrag`)
    *   `project.dataset.job_audit` (new table for logging: `job_kennung STRING, status STRING, message STRING, created_at TIMESTAMP, record_count INT64`)
2.  **Create BigQuery Stored Procedure for SQL transformation:**
    *   `project.dataset.d_ausd_bp_ta_rn_vertrag` (containing translated Oracle SQL DML for truncation, aggregation, and insertion)
    *   **Language:** BigQuery SQL
3.  **Create BigQuery Stored Procedure for internal control logic:**
    *   `project.dataset.k_ausd_bp_ta_rn_vertrag` (orchestrates date handling, parameter validation, and calls `d_ausd_bp_ta_rn_vertrag`)
    *   **Language:** BigQuery SQL
4.  **Create BigQuery Stored Procedure for main orchestration:**
    *   `project.dataset.ausd_bp_ta_rn_vertrag` (handles top-level parameters, logging, and calls `k_ausd_bp_ta_rn_vertrag`)
    *   **Language:** BigQuery SQL
5.  **Develop Cloud Composer DAG (Optional, for advanced orchestration/scheduling):**
    *   If complex parameter handling, file-based interactions (from commented-out code), or external system integrations are eventually required beyond BigQuery Stored Procedures, a Python-based Apache Airflow DAG in Cloud Composer would be created to orchestrate the BigQuery Stored Procedure calls.
    *   **Language:** Python