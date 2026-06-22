# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh

## 1. Purpose & Scope
This migration job, originating from the KornShell script `k_ausd_v_ta_p_vertrag.ksh`, is responsible for orchestrating the processing of contract data. The primary function involves executing an Oracle SQL*Plus script (`d_ausd_v_ta_p_vertrag.sql`) which reads from various staging tables (`isbert_schema.dwtk_meldungen`, `sof$ta_vertrag_tmp`), performs transformations, and inserts the processed data into the `sof$ta_p_vertrag` table. Additionally, it manages and truncates several temporary tables after processing. The scope of this migration is to re-implement this entire workflow on Google Cloud Platform, utilizing BigQuery for data storage and processing, and Cloud Composer (Apache Airflow) for orchestration.

## 2. Source Inventory
This job is composed of two primary source files:

1.  **`k_ausd_v_ta_p_vertrag.ksh`**
    *   **Relative Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh`
    *   **Category:** `shell`
    *   **Tool:** `KornShell`
    *   **Summary:** KornShell script for controlling the execution of a SQL script, including parameter parsing, error handling, and job status management.
    *   **Complexity Tier:** Medium (inferred)
    *   **Migration Bucket:** B3 (Manual) - Requires significant manual rewrite for orchestration.

2.  **`d_ausd_v_ta_p_vertrag.sql`**
    *   **Relative Path:** `vobs/dw_source/isrpt/isbert/aufbereitung/sql/d_ausd_v_ta_p_vertrag.sql`
    *   **Category:** `sql`
    *   **Tool:** `Oracle SQL*Plus`
    *   **Summary:** Oracle SQL*Plus script performing data transformations, inserting into `sof$ta_p_vertrag` from `sof$ta_vertrag_tmp`, and managing multiple temporary tables via PL/SQL calls.
    *   **Complexity Tier:** Complex (inferred)
    *   **Migration Bucket:** B3 (Manual) or B4 (Redesign) - Due to Oracle-specific features, PL/SQL interactions, and database links.

## 3. Target Architecture
The target architecture will leverage the following Google Cloud Platform services:
*   **Data Warehouse:** Google BigQuery for all persistent and temporary data storage.
*   **Orchestration:** Cloud Composer (managed Apache Airflow) for scheduling, monitoring, and executing the data pipeline.
*   **Data Ingestion:** Cloud Data Fusion, Dataflow, or Storage Transfer Service may be used to bring source data from external Oracle systems into BigQuery.
*   **Compute:** BigQuery's serverless query engine will handle all SQL processing.

## 4. Data Flow & Lineage
The original data flow involves the KornShell script orchestrating the execution of an Oracle SQL*Plus script.
1.  **`k_ausd_v_ta_p_vertrag.ksh`** (Orchestrator)
    *   Parses command-line parameters (`JobKennung`, `EintragsNr`).
    *   Initializes environment variables (`. $HOME/.dw_init`).
    *   Loads utility functions (error handling, date utilities, parameter parsing, SQL*Plus utilities).
    *   Determines the path to `d_ausd_v_ta_p_vertrag.sql`.
    *   Invokes `starteSQLSkript` (via `h_alis_sqlplus.ksh`) to execute `d_ausd_v_ta_p_vertrag.sql`.
    *   Handles job status and record count from temporary files.

2.  **`d_ausd_v_ta_p_vertrag.sql`** (Data Processor)
    *   **Input Sources:**
        *   `isbert_schema.dwtk_meldungen`: Used to determine a reference date (`v_datum`).
        *   `sof$ta_vertrag_tmp`: Primary source for contract data.
        *   Potential external sources via DB-Link `@pcrs1` (Carmen DB).
    *   **Transformations:**
        *   Calculates `v_datum` based on `MAX(m.timecreated)`.
        *   Truncates `sof$ta_p_vertrag`.
        *   Performs an `INSERT INTO sof$ta_p_vertrag SELECT ... FROM sof$ta_vertrag_tmp v LEFT JOIN sof$ta_vertrag_tmp pv ON v.twin_vertrag_id = pv.vertrag_id_carmen`.
        *   Truncates numerous temporary staging tables: `sof$ta_disc_zusgf`, `sof$ta_discount`, `sof$ta_barrier_zusgf`, `sof$ta_barrier`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`, `sof$ta_cntrct_valid`, `sof$ta_period`, `sof$ta_bp_ref`, `sof$ta_inv_assign`, `sof$ta_inv_def`, `sof$ta_acc_ref`, `sof$ta_notice`, `sof$ta_apn_ve`, `sof$ta_discount_rr`, `sof$ta_vvl_dwh`, `sof$ta_vvl_upgrade`, `sof$ta_cntrct_crs2`, `sof$ta_cntrct_crs3`, `sof$ta_inv_acc`, `sof$ta_vertrag_tmp`, `sof$ta_action_assoc`. These truncations are performed using a PL/SQL procedure call `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
    *   **Output Targets:**
        *   `sof$ta_p_vertrag`: Main output table for processed contract data.

## 5. Transformation Logic

### 5.1. KornShell Script (`k_ausd_v_ta_p_vertrag.ksh`) to Airflow DAG
The KornShell script will be rewritten as a Python-based Apache Airflow DAG.
*   **Parameter Handling:** Command-line parameters `j` (JobKennung) and `f` (EintragsNr) will be converted to Airflow DAG parameters (e.g., using `params` in DAG definition or `dag_run.conf`).
*   **Utility Scripts:** The sourced utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) will be replaced by:
    *   Python functions for environmental setup and date calculations.
    *   Airflow's native logging and error handling mechanisms.
    *   BigQueryOperator for SQL execution.
*   **SQL Execution:** The `starteSQLSkript` call will be replaced by a `BigQueryOperator` (or a series of them) executing the migrated BigQuery SQL (see 5.2).
*   **Temporary Files:** The use of `$DW_DIR_UTL/bert_k_ausd_v_ta_p_vertrag_$$.tmp` to store record counts will be replaced by Airflow XComs or by storing metrics directly into a BigQuery logging/metrics table.

### 5.2. Oracle SQL*Plus Script (`d_ausd_v_ta_p_vertrag.sql`) to BigQuery SQL
The Oracle SQL*Plus script will be converted to BigQuery SQL, incorporating the following changes:

*   **Variable Definition:** Oracle `DEFINE` and `COLUMN ... NEW_VALUE` for `v_datum` will be translated to BigQuery `DECLARE` and `SET` statements.
    *   Original:
        ```sql
        COLUMN s_datum new_value v_datum noprint
        SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
          FROM isbert_schema.dwtk_meldungen m
         WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        ```
    *   BigQuery:
        ```sql
        DECLARE v_datum STRING;
        SET v_datum = (
          SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
          FROM `isbert_schema.dwtk_meldungen` m
          WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        );
        ```
*   **SQL*Plus Commands:** `WHENEVER SQLERROR`, `SET TIMING ON`, `SET SERVEROUTPUT ON`, `SPOOL`, `START` are SQL*Plus specific and will be removed, with equivalent functionality (e.g., error handling, logging) handled by the orchestrating Airflow DAG.
*   **Data Manipulation Language (DML):**
    *   **Outer Join:** Oracle's proprietary outer join syntax `(+)` will be replaced with standard `LEFT JOIN`.
        *   Original: `FROM sof$ta_vertrag_tmp v, sof$ta_vertrag_tmp pv WHERE v.twin_vertrag_id = pv.vertrag_id_carmen (+)`
        *   BigQuery: `FROM `sof$ta_vertrag_tmp` v LEFT JOIN `sof$ta_vertrag_tmp` pv ON v.twin_vertrag_id = pv.vertrag_id_carmen`
    *   **Truncation:** The `TRUNCATE TABLE` statements will be directly translated. The `DROP STORAGE` and `REUSE STORAGE` clauses are Oracle-specific and will be removed.
    *   **PL/SQL Calls:** The `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` calls used for truncating tables will be rewritten as explicit BigQuery `TRUNCATE TABLE` statements executed by the Airflow DAG or within a BigQuery scripting block.
*   **Table Naming:** Oracle table names with `$` will need to be properly quoted in BigQuery (e.g., `sof$ta_p_vertrag` becomes ``sof$ta_p_vertrag```).
*   **Hints:** Oracle `/*+ parallel(v,4) parallel(pv,4) */` hints are not directly translatable and typically removed, relying on BigQuery's automatic query optimization.
*   **Commit:** The `commit;` statement is transactional and will be implicitly handled by BigQuery's DML operations.

## 6. External Dependencies
*   **Oracle Database (Carmen & `isbert_schema`):** The Oracle database serving `isbert_schema.dwtk_meldungen`, `sof$ta_vertrag_tmp`, and other `sof$ta_*` tables, potentially accessed via DB-link `@pcrs1` (Carmen DB), is a critical external dependency. Data from these sources must be ingested into BigQuery. This can be achieved using:
    *   **Cloud Data Fusion:** For ETL pipelines from Oracle to BigQuery.
    *   **Dataflow:** For custom, high-volume data ingestion.
    *   **Storage Transfer Service:** For batch transfers of data into Cloud Storage, which can then be loaded into BigQuery.
    *   **Change Data Capture (CDC):** If real-time or near real-time updates are required, a CDC solution might be implemented.
*   **KornShell Utility Scripts:** The local utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) are part of the legacy environment. Their functionalities (environment setup, error reporting, date calculations, parameter parsing, SQL execution wrappers) will be replaced with native Python functions, Airflow operators, and BigQuery's capabilities.

## 7. Unresolved / Risks
*   **Data Lineage of `sof$ta_vertrag_tmp`:** The origin and full lifecycle of `sof$ta_vertrag_tmp` are not fully established. It is crucial to identify the upstream process that populates this temporary table to ensure its data is correctly migrated and continuously available in BigQuery.
*   **Carmen DB Integration:** The `@pcrs1` database link to the Carmen DB implies direct integration. The method of migrating or ingesting data from Carmen into BigQuery needs to be precisely defined to maintain data integrity and availability.
*   **PL/SQL `DWPA_UTIL_SKRIPT.runstatement`:** The exact logic within the `DWPA_UTIL_SKRIPT.runstatement` procedure is unknown. While a simple `TRUNCATE TABLE` equivalent is proposed, any complex logic or dependencies within this procedure (e.g., logging, auditing, conditional logic) would need to be re-implemented in BigQuery scripts or Airflow tasks.
*   **Lack of Metadata:** The absence of `file_complexity` and `automation_rate` data for both source files suggests a higher degree of manual effort and potential for unforeseen complexities.
*   **Dynamic SQL/Parameters:** While parameters are identified, the overall impact of dynamic SQL generation (if any beyond `v_datum`) needs careful review.

## 8. Build Plan

1.  **Phase 1: Data Ingestion (Parallel with Design)**
    *   **Task 1.1:** Identify all source tables from the Oracle database (including those from Carmen DB, `isbert_schema`, and all `sof$ta_*` tables).
    *   **Task 1.2:** Design and implement data ingestion pipelines from Oracle to BigQuery for all identified source tables. Prioritize `isbert_schema.dwtk_meldungen` and `sof$ta_vertrag_tmp`.
    *   **Task 1.3:** Create corresponding datasets and tables in BigQuery.

2.  **Phase 2: SQL Transformation & BigQuery Implementation**
    *   **Task 2.1:** Convert `d_ausd_v_ta_p_vertrag.sql` to BigQuery SQL based on the design document.
    *   **Task 2.2:** Implement BigQuery scripting if needed for complex sequences or variable handling (e.g., `v_datum`).
    *   **Task 2.3:** Replace PL/SQL `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` calls with explicit `TRUNCATE TABLE` statements in BigQuery, or equivalent BigQuery scripts.

3.  **Phase 3: Orchestration with Cloud Composer (Airflow DAG)**
    *   **Task 3.1:** Design and develop an Airflow DAG in Python that replicates the orchestration logic of `k_ausd_v_ta_p_vertrag.ksh`.
    *   **Task 3.2:** Implement parameter passing (`JobKennung`, `EintragsNr`) in the Airflow DAG.
    *   **Task 3.3:** Create Airflow tasks using `BigQueryOperator` to execute the converted BigQuery SQL from Phase 2.
    *   **Task 3.4:** Replicate utility script functionalities (e.g., date calculations, custom error handling if needed) using Python functions or custom Airflow operators.
    *   **Task 3.5:** Integrate Airflow's native logging and monitoring for job status and error reporting.

4.  **Phase 4: Testing and Validation**
    *   **Task 4.1:** Develop comprehensive unit tests for individual BigQuery SQL components and Airflow tasks.
    *   **Task 4.2:** Conduct integration testing to ensure end-to-end data flow and correctness between Airflow and BigQuery.
    *   **Task 4.3:** Perform data validation comparing output data in BigQuery with the legacy system's output.

5.  **Phase 5: Deployment**
    *   **Task 5.1:** Deploy the BigQuery resources (tables, datasets).
    *   **Task 5.2:** Deploy the Airflow DAG to Cloud Composer.
    *   **Task 5.3:** Schedule the DAG for production execution.