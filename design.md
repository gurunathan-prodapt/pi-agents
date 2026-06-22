# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh

## 1. Purpose & Scope
This document outlines the migration design for the ETL job identified by `job_id 5af228f1`, originating from the KornShell script `k_ausd_v_ta_action_assoc.ksh`. This job's primary purpose is to process and manage `ta_action_assoc` data. The KornShell script acts as an orchestration layer, handling job parameters, environment setup, error logging, and the execution of a core SQL script (`d_ausd_v_ta_action_assoc.sql`). The SQL script performs data extraction, transformation, and loading (ETL) by reading from source tables (`isbert_schema.dwtk_meldungen`, `cds$ta_action_assoc`) and inserting into a target table (`sof$ta_action_assoc`), based on a dynamically determined cutoff date. The current job is noted as having a "medium" stage distribution.

## 2. Source Inventory
The assembled job consists of two primary components: an orchestration KornShell script and a SQL script it invokes.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh`
    *   **Technology:** KornShell Script
    *   **Category:** `shell`
    *   **Tool:** KornShell
    *   **Purpose:** ETL Orchestration and Control Script. It sets up the execution environment, parses command-line parameters (`p_JobKennung`, `p_EintragsNr`), integrates error handling, and invokes the `d_ausd_v_ta_action_assoc.sql` script.
    *   **Complexity Tier:** Not available (no data from `file_complexity`).
    *   **Automation Bucket:** Not available (no data from `automation_rate`).
    *   **Key References:**
        *   Sources utility scripts: `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`.
        *   Invokes SQL script: `d_ausd_v_ta_action_assoc.sql`.
        *   Interacts with temporary file: `tmpFile` for record counts.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_action_assoc.sql`
    *   **Technology:** Oracle SQL (SQL*Plus)
    *   **Category:** SQL
    *   **Tool:** SQL*Plus (implied by directives like `DEFINE`, `COLUMN`, `WHENEVER SQLERROR`, `SPOOL`)
    *   **Purpose:** Data Transformation and Loading. This script calculates a date, truncates a target table, and inserts processed data.
    *   **Complexity Tier:** Not available (no data from `file_complexity`).
    *   **Automation Bucket:** Not available (no data from `automation_rate`).
    *   **Reads From:**
        *   `isbert_schema.dwtk_meldungen` (to determine `v_datum`)
        *   `cds$ta_action_assoc` (via a DB-Link `@pcrs1`)
    *   **Writes To:**
        *   `sof$ta_action_assoc`
    *   **Calls Package:** `isbert_schema.DWPA_UTIL_SKRIPT` (for truncating `sof$ta_action_assoc`)

## 3. Target Architecture
The target architecture for this job will leverage Google Cloud Platform (GCP) services:

*   **Orchestration Layer:** Apache Airflow DAG. The current UC4 scheduling (as inferred from lineage) will be replaced by an Airflow DAG, providing job scheduling, dependency management, and monitoring capabilities.
*   **Data Processing Layer:** Google BigQuery. All SQL logic will be converted to BigQuery Standard SQL.
*   **Data Storage:** Google BigQuery Tables. Source and target tables will reside in BigQuery.

### Target BigQuery Components & Layout:
*   **Datasets:** Existing or newly created BigQuery datasets will host the migrated tables, likely retaining schema names (e.g., `isbert_schema`, `cds_schema`).
*   **Tables:**
    *   `isbert_schema.dwtk_meldungen` (Source) -> `isbert_schema.dwtk_meldungen` (BigQuery)
    *   `cds$ta_action_assoc` (Source) -> `cds_ta_action_assoc` (BigQuery)
    *   `sof$ta_action_assoc` (Target) -> `sof_ta_action_assoc` (BigQuery)
*   **SQL Dialect:** BigQuery Standard SQL.

## 4. Data Flow & Lineage
### Legacy Data Flow:
1.  **UC4 Job Trigger:** An external UC4 job (`DW.BERT_AUSD_V_TA_ACTION_ASSOC.xml`) initiates the `k_ausd_v_ta_action_assoc.ksh` script.
2.  **KornShell Orchestration:** `k_ausd_v_ta_action_assoc.ksh` performs:
    *   Environment initialization (`.dw_init`).
    *   Parameter parsing (`-j`, `-f`).
    *   Error handling setup.
    *   Invocation of `d_ausd_v_ta_action_assoc.sql` via a helper function `starteSQLSkript`.
3.  **SQL Execution:** `d_ausd_v_ta_action_assoc.sql` (executed by `sqlplus` via the ksh script) performs:
    *   **Date Determination:** Queries `isbert_schema.dwtk_meldungen` to find the latest `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, establishing `v_datum`.
    *   **Target Truncation:** Executes `TRUNCATE TABLE sof$ta_action_assoc` via `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
    *   **Data Load:** Inserts data into `sof$ta_action_assoc` from `cds$ta_action_assoc` (potentially over a DB-Link to `@pcrs1`), applying filters based on `insert_at`, `valid_from`, `is_production`, `modified_at`, and `valid_to` against `v_datum`.
    *   **Logging:** Spools output to a trace file.

### Target Data Flow:
1.  **Airflow DAG Trigger:** The Airflow DAG `d_ausd_v_ta_action_assoc` is triggered (e.g., on a schedule or by an upstream dependency).
2.  **BigQuery Task Execution:** A single `BigQueryExecuteQueryOperator` task named `process_ta_action_assoc` executes the entire migrated SQL logic.
3.  **BigQuery ETL:** The BigQuery SQL performs the following steps within BigQuery:
    *   **Date Determination:** Declares `v_datum` by querying `isbert_schema.dwtk_meldungen` (BigQuery table).
    *   **Target Truncation:** Truncates the `sof_ta_action_assoc` table (BigQuery table).
    *   **Data Load:** Inserts filtered data from `cds_ta_action_assoc` (BigQuery table) into `sof_ta_action_assoc` (BigQuery table) using BigQuery Standard SQL.
    *   **Logging:** Airflow's native logging and BigQuery's query history will capture execution details.

## 5. Transformation Logic

### KornShell Script (`k_ausd_v_ta_action_assoc.ksh`) to Airflow DAG (Python):
*   **Orchestration:** The shell script's role as a control flow manager will be directly translated into an Airflow DAG written in Python.
*   **Parameter Handling:** Command-line parameters `p_JobKennung` and `p_EintragsNr` will be handled within the Airflow DAG. `p_JobKennung`'s value ('BERT_DROP_TEMP_TABLE') is directly used in the SQL. `p_EintragsNr` is currently unused in the generated SQL and its purpose will need to be re-evaluated if it affects the data processing. Airflow's `params` or XComs can be used for dynamic parameter passing if needed.
*   **Environment Sourcing:** The `.dw_init` and other utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) will be replaced. Environment variables will be configured in Airflow, and utility functions will be re-implemented in Python or as BigQuery UDFs/Stored Procedures if they represent reusable business logic. Error handling and date functions will leverage Airflow's built-in capabilities or standard Python libraries.
*   **SQL Execution:** The `starteSQLSkript` function call is replaced by a `BigQueryExecuteQueryOperator` directly executing the BigQuery SQL.
*   **Temporary File (`tmpFile`):** The logic to read record counts from `tmpFile` will be adapted to BigQuery. A `SELECT COUNT(*)` on the target table after the insert can provide the record count, which can then be pushed to Airflow XComs if needed for downstream tasks.

### Oracle SQL (`d_ausd_v_ta_action_assoc.sql`) to BigQuery Standard SQL:
*   **Variable Definition (`DEFINE`, `COLUMN`, `SELECT NVL(TO_CHAR(MAX(m.timecreated)...)`): The SQL*Plus specific variable definitions and date calculation will be translated into a `DECLARE` statement within BigQuery Standard SQL to define `v_datum`. `TO_CHAR` and `NVL` will be replaced by `FORMAT_DATE` (if required for string output) and `COALESCE` respectively, and `DATE()` for date conversions.
*   **Tracing and Spooling (`START`, `SPOOL`):** These features are specific to SQL*Plus and will be removed. Airflow's logging and BigQuery's automatic query history will provide equivalent monitoring and auditing capabilities.
*   **Error Handling (`WHENEVER SQLERROR`):** Oracle-specific error handling will be managed by Airflow's task failure and retry mechanisms.
*   **Target Truncation (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_action_assoc')`):** This will be directly translated to `TRUNCATE TABLE \`sof_ta_action_assoc\`;` in BigQuery Standard SQL.
*   **Data Insertion (`INSERT INTO ... SELECT ...`):** The Oracle `INSERT` statement will be converted to BigQuery Standard SQL.
    *   Oracle `TO_DATE('&v_datum', 'YYYYMMDD')` will become `DATE(v_datum)` or `PARSE_DATE('%Y%m%d', v_datum)` depending on the actual string format of `v_datum` within BigQuery. The generated SQL correctly uses `DATE(ac.insert_at) <= v_datum`.
    *   Oracle hints (`/*+ parallel(ac,4) full(ac) */`) are not required in BigQuery and will be removed, as BigQuery automatically handles query optimization and parallelism.
    *   The `&v_carmen` DB-Link for `cds$ta_action_assoc` needs to be addressed based on the migration strategy for this source table (see External Dependencies).
*   **Transaction Management (`commit;`):** BigQuery statements are atomic; an explicit `COMMIT` is not required.

## 6. External Dependencies

*   **UC4 Scheduler (`DW.BERT_AUSD_V_TA_ACTION_ASSOC.xml`):** The existing UC4 job that triggers `k_ausd_v_ta_action_assoc.ksh` will be replaced by the new Airflow DAG `d_ausd_v_ta_action_assoc`. This DAG will be scheduled or triggered based on the original UC4 job's schedule or upstream dependencies.
*   **Oracle Database (General):**
    *   **`isbert_schema.dwtk_meldungen`:** This source table needs to be migrated to a BigQuery table in the `isbert_schema` dataset.
    *   **`cds$ta_action_assoc` (via `@pcrs1` DB-Link):** This is a critical source table. The presence of a DB-Link suggests `cds$ta_action_assoc` resides in a separate Oracle instance (`pcrs1`). The migration strategy for this table must be determined:
        1.  **Full Migration:** Migrate `cds$ta_action_assoc` to a native BigQuery table. This is the preferred approach for performance and manageability.
        2.  **External Table:** If `pcrs1` remains an active source, BigQuery external tables can be used to query data directly from Oracle, although this might impact performance.
        3.  **Data Transfer Service:** Use BigQuery Data Transfer Service or custom ETL processes to regularly ingest data from `pcrs1` into a BigQuery `cds_ta_action_assoc` table.
    *   **`isbert_schema.DWPA_UTIL_SKRIPT` (PL/SQL Package):** The specific call `runstatement(0, 'TRUNCATE TABLE sof$ta_action_assoc')` is directly translated into BigQuery DDL. If other functionalities of this package are used elsewhere, they would require re-implementation in BigQuery UDFs/Stored Procedures or Airflow Python tasks.
*   **File System:** The legacy use of temporary files (`tmpFile`) for record counts and spool files for tracing will be replaced by Airflow's native logging and potentially BigQuery's query results or XComs for inter-task communication.

## 7. Unresolved / Risks

*   **Missing Complexity/Automation Data:** The absence of `file_complexity` and `automation_rate` data means the assessed effort and potential challenges for migration are based solely on code analysis. A detailed manual review might be necessary to confirm complexity and assign an automation bucket.
*   **`VIA` Table Discrepancy:** The `lineage_edges` indicate `d_ausd_v_ta_action_assoc.sql` writes to `TABLE:VIA`, but the provided SQL content only shows an `INSERT` into `sof$ta_action_assoc`. This suggests a potential historical reference in the lineage, or a part of the SQL logic that was not included or is conditional. This requires clarification to ensure no data target is missed. For this design, it is assumed the provided SQL is the current operational logic.
*   **`p_EintragsNr` Parameter Usage:** The KornShell script parses `p_EintragsNr` and passes it to `starteSQLSkript`, but this parameter is not explicitly used in the provided Oracle SQL or the generated BigQuery SQL. Its functional role in the original job needs to be confirmed to ensure no logic is omitted.
*   **`h_alis_sqlplus.ksh` Content:** This sourced script defines `starteSQLSkript`. While its core function (executing SQL) is understood, any other hidden functionalities within this helper script might need to be accounted for.
*   **Error Reporting/Alerting:** The `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler` from the original script likely provide specific error reporting or alerting mechanisms. These will need to be re-implemented in Airflow using appropriate notification channels (e.g., email, PagerDuty, Slack).

## 8. Build Plan
The migration will result in a single Airflow DAG Python file containing the transformed BigQuery SQL.

1.  **Migrate Oracle Tables to BigQuery:**
    *   Create BigQuery table: `isbert_schema.dwtk_meldungen`.
    *   Create BigQuery table: `cds_ta_action_assoc`.
    *   Create BigQuery table: `sof_ta_action_assoc`.
    *   Establish data ingestion for `cds_ta_action_assoc` if it remains an external Oracle source (e.g., using BigQuery Data Transfer Service).
2.  **Develop Airflow DAG:**
    *   **File Name:** `d_ausd_v_ta_action_assoc.py`
    *   **Language:** Python
    *   **Content:**
        *   Airflow DAG definition, including `dag_id`, schedule, default arguments, and tags.
        *   A single `BigQueryExecuteQueryOperator` task (`process_ta_action_assoc`) to encapsulate the entire BigQuery SQL logic.
        *   The embedded BigQuery SQL will handle:
            *   Declaration of `v_datum` from `isbert_schema.dwtk_meldungen`.
            *   Truncation of `sof_ta_action_assoc`.
            *   Insertion of data from `cds_ta_action_assoc` into `sof_ta_action_assoc` with the specified filtering conditions.
        *   Implement Airflow-native error handling and logging.
        *   If necessary, implement logic for `p_EintragsNr` and `v_records` in Python tasks.
3.  **Testing:**
    *   Unit tests for the Airflow DAG and embedded SQL.
    *   Integration tests to verify data correctness and end-to-end job execution in BigQuery.
    *   Performance testing and optimization in BigQuery.
4.  **Deployment:** Deploy the Airflow DAG to the GCP Composer environment.
5.  **Decommissioning:** Once the BigQuery job is verified, decommission the legacy UC4 job and KornShell/SQL scripts.