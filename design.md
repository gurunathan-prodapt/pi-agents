# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh

## 1. Purpose & Scope
This document outlines the migration design for the ETL job identified by `run_id: 6d73ee79-8207-4271-b787-9644c913bf51` and `job_id: 6d73ee79`. The job, named `BERT_AUSTAUSCH_KSH` (derived from `r_ausd_austausch.ksh`), is responsible for preparing and providing a snapshot extract of the contract cache base table (`Stichtags-Abzug der Vertrags-Cache`) for the BERT report and for Forderungsscoring.

The scope of this migration involves re-engineering the existing KornShell scripts and Oracle SQL into a BigQuery-native solution, likely orchestrated by Google Cloud Composer (Airflow), to maintain the current data pipeline functionality and output.

## 2. Source Inventory
The job is primarily composed of a KornShell wrapper script (`r_ausd_austausch.ksh`) that orchestrates the execution of another KornShell script (`k_ausd_austausch.ksh`), which in turn executes a core Oracle SQL script (`d_ausd_austausch.sql`). The job is scheduled by UC4.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh**
    *   **Technology**: KornShell Script
    *   **Complexity Tier**: (Not available from `file_complexity`)
    *   **Automation Bucket**: semi_auto
    *   **Purpose**: Main job wrapper, handles parameter parsing, environment setup, logging, and invokes the core processing script.
    *   **Sourced Utility Scripts**:
        *   `$HOME/.dw_init`: Environment initialization.
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error handling framework.
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter parsing utilities.
        *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date manipulation utilities.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_austausch.ksh**
    *   **Technology**: KornShell Script
    *   **Complexity Tier**: (Not available from `file_complexity`)
    *   **Automation Bucket**: (Not explicitly available, but part of the overall `semi_auto` flow)
    *   **Purpose**: Core processing script, called by `r_ausd_austausch.ksh`, responsible for executing the Oracle SQL script and handling intermediate logging/status updates. It sources `h_alis_sqlplus.ksh` for SQL*Plus interaction.

*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_austausch.sql**
    *   **Technology**: Oracle SQL/PLSQL
    *   **Complexity Tier**: (Not available from `file_complexity`)
    *   **Automation Bucket**: (Not explicitly available, but part of the overall `semi_auto` flow)
    *   **Purpose**: Performs data transformation and updates various reporting tables (`RPT$TA_S_D1_RECH_EMPF`, `RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_RECH_KUNDE`, `RPT$TA_S_D1_DISCOUNT`, `RPT$TA_S_D1_DISCOUNT_RR`, `RPT$TA_S_D1_VPN`) using a staged refresh pattern involving temporary tables and `INSERT...SELECT` operations.

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services, primarily BigQuery for data storage and transformation, and Cloud Composer (Apache Airflow) for orchestration.

*   **BigQuery Dataset**: A dedicated BigQuery dataset (e.g., `project.dataset`) will house all migrated tables and stored procedures.
    *   **Staging Tables**: Temporary tables like `sof$ta_rechdef`, `sof$ta_kd_kto` will be materialized as temporary tables or Common Table Expressions (CTEs) within BigQuery stored procedures. Staging tables suffixed with `_new` (e.g., `rpt$ta_s_d1_rech_empf_new`) will be managed as part of a table-swapping or atomic overwrite strategy.
    *   **Target Tables**: The final `RPT$TA_S_D1_*` tables will be BigQuery tables.
    *   **Audit/Log Table**: A dedicated BigQuery table (e.g., `project.dataset.job_audit_log`) will capture job execution metadata, status, and error messages, replacing the file-based logging.
    *   **Job Sequence Table**: A BigQuery table (e.g., `project.dataset.job_sequence`) for generating unique job IDs, if required by the legacy system's logic.

*   **BigQuery Stored Procedures**:
    *   **`BERT_AUSTAUSCH_KSH` (wrapper SP)**: A BigQuery stored procedure (SP) to encapsulate the logic of `r_ausd_austausch.ksh`, handling parameter parsing, date defaulting, restart logic, and calling the core transformation SP.
    *   **`k_ausd_austausch` (core transformation SP)**: A BigQuery SP to encapsulate the logic of `k_ausd_austausch.ksh` and `d_ausd_austausch.sql`. This SP will perform the staged data loading and transformations.

*   **Cloud Composer (Airflow) DAG**:
    *   An Airflow DAG will replace the UC4 scheduler. It will orchestrate the execution of the BigQuery stored procedures.
    *   The DAG will manage the overall flow, error handling, and monitoring.

## 4. Data Flow & Lineage
The migrated job will follow this data flow:

1.  **Cloud Composer (Airflow)**:
    *   The Airflow DAG, replacing the UC4 scheduler, will trigger the main BigQuery stored procedure.
    *   It will pass parameters like `stichtag` and `wiederanlaufWert` to the stored procedure.

2.  **BigQuery Stored Procedure `BERT_AUSTAUSCH_KSH`**:
    *   Receives `p_stichtag` (processing date) and `p_wiederanlaufWert` (restart value) as input parameters.
    *   Initializes job metadata and logs the start of execution into `job_audit_log`.
    *   Determines `v_sysdate` and defaults `p_stichtag` if not provided, mimicking the `h_alis_date.ksh` and `r_ausd_austausch.ksh` logic.
    *   Validates parameters.
    *   Calls the core transformation stored procedure `k_ausd_austausch`.
    *   Logs the completion or error status into `job_audit_log`.

3.  **BigQuery Stored Procedure `k_ausd_austausch` (incorporating `d_ausd_austausch.sql` logic)**:
    *   Receives parameters from `BERT_AUSTAUSCH_KSH`.
    *   **Staged Table Refresh Pattern**:
        *   For each target table (`RPT$TA_S_D1_RECH_EMPF`, `RPT$TA_S_D1_VERTRAG`, `RPT$TA_S_D1_RECH_KUNDE`, `RPT$TA_S_D1_DISCOUNT`, `RPT$TA_S_D1_DISCOUNT_RR`, `RPT$TA_S_D1_VPN`):
            1.  **Preparation**: Drop the old table, rename `_new` table to current, or use `CREATE OR REPLACE TABLE AS SELECT` for atomic updates. Temporary tables (`sof$ta_rechdef`, `sof$ta_kd_kto`) will be managed as temporary tables or CTEs within this SP.
            2.  **Data Population**: Execute `INSERT INTO ... SELECT FROM ...` statements.
                *   Reads data from source tables:
                    *   `sof$ta_p_rech_empf`
                    *   `sof$ta_p_vertrag`
                    *   `sof$ta_p_basisprod`
                    *   `sof$ta_p_gesch_part`
                    *   `sof$ta_p_dn_nutzer`
                    *   `sof$ta_p_evn_empf`
                    *   `sof$ta_p_discount`
                    *   `sof$ta_p_discount_rr`
                    *   `sof$ta_p_d1_vpn`
                *   Applies transformations and joins as described in Section 5.
            3.  **Post-Load Operations**: BigQuery equivalents for index creation (e.g., clustering keys, partitioning) will be part of the table DDL or update statements, not separate operations. `ANALYZE TABLE` is not needed in BigQuery.

## 5. Transformation Logic
The core transformation logic resides within the `d_ausd_austausch.sql` script, which will be translated into a BigQuery stored procedure. The transformations involve several `INSERT INTO ... SELECT FROM ...` statements, each updating a specific target table.

**General Transformations:**
*   **Data Type Conversion**: Oracle-specific date functions (`to_date`) will be converted to BigQuery date literals (`DATE 'YYYY-MM-DD'`) or functions (`PARSE_DATE`). Numeric precision will be handled by appropriate BigQuery data types (e.g., `NUMERIC`, `BIGNUMERIC`, `INT64`).
*   **Function Equivalents**:
    *   `NVL(a,b)` will become `IFNULL(a,b)`.
    *   `DECODE(expr, ...)` will be converted to BigQuery's `CASE expr WHEN ... THEN ... ELSE ... END` structure.
    *   Oracle's outer join syntax `(+)` will be replaced with explicit `LEFT JOIN` clauses.
    *   `SUBSTR` remains `SUBSTR`. `TO_CHAR(x)` used for string conversion will become `CAST(x AS STRING)`.

**Specific Table Transformations:**

1.  **RPT$TA_S_D1_RECH_EMPF**:
    *   Source: `sof$ta_p_rech_empf`
    *   Mapping: Direct column mapping with `SUBSTR` for `strasse` and `firma` fields.

2.  **RPT$TA_S_D1_VERTRAG**:
    *   Sources: `sof$ta_p_vertrag` (v), `sof$ta_p_basisprod` (bp, bpt), `sof$ta_p_gesch_part` (gp), `sof$ta_p_dn_nutzer` (dn), `sof$ta_p_evn_empf` (ev).
    *   Complex `JOIN` conditions, including `LEFT JOIN` (from Oracle `(+)`).
    *   Extensive `CASE` statements for fields like `twincard`, `msisdn`, `evn`, `data96`, `fax`, `E_ID`, `CARD_TYPE_NAME`, `LINK_E_ID`, `LINK_CARD_TYPE_NAME`, `MS2_E_ID`, `MS2_CARD_TYPE_NAME`, `iccid`, `link_iccid`, `ms2_iccid`, `hlr`, `link_hlr`, `ms2_hlr`, and MultiSim 3 Plus related fields (`MS3_ICCID` to `MS10_HLR`). These `CASE` statements often involve checking `vertragsstatus = 'A'` and various status flags from `sof$ta_p_basisprod`.
    *   `IFNULL` for `data_option_rein`, `voice_option_rein`, `mix_option`, `multi_option`, `roaming_option`, `sonstige_option`, `apn`, `bcp_vertrag`, `bcp_iccid`, `bcp_hlr`.
    *   `DATE '1111-11-11'` for `rueckgewinn_datum`.
    *   Uses `UNION ALL` to combine data based on `v.cntrct_ty` (excluding `11` and `20` in the first part, and only `20` in the second part).

3.  **RPT$TA_S_D1_RECH_KUNDE**:
    *   This involves two intermediate temporary tables: `sof$ta_rechdef` and `sof$ta_kd_kto`.
    *   `sof$ta_rechdef` is populated from `RPT$TA_S_D1_RECH_EMPF`.
    *   `sof$ta_kd_kto` is populated from `RPT$TA_S_D1_VERTRAG`.
    *   Finally, `RPT$TA_S_D1_RECH_KUNDE_NEW` is populated by joining these two intermediate tables on `rechdef_id_carmen`. This intermediate table pattern can be directly translated to BigQuery using CTEs within a single stored procedure or by creating temporary tables.

4.  **RPT$TA_S_D1_DISCOUNT**:
    *   Source: `sof$ta_p_discount`
    *   Mapping: Direct column mapping for `contract_number` and `rabatt_alle`.

5.  **RPT$TA_S_D1_DISCOUNT_RR**:
    *   Source: `sof$ta_p_discount_rr`
    *   Mapping: Direct column mapping for `contract_number`, `std_vertrag`, `rabatt`, `rabattierte_rech_pos`, `rabatthoehe`, `cntrct_template_id`, `disc_invoice_item_id`.

6.  **RPT$TA_S_D1_VPN**:
    *   Source: `sof$ta_p_d1_vpn`
    *   Mapping: Direct column mapping for `vertrags_id` as `vertrag_id_carmen` and `vpn_id`.

**DDL and PL/SQL Handling**:
*   The Oracle `RENAME`, `TRUNCATE`, `CREATE INDEX`, `ALTER INDEX`, `DROP INDEX`, and `ANALYZE TABLE` statements, along with calls to `isbert_schema.dwpa_util_skript.runstatement`, will be replaced. In BigQuery, index management is typically replaced by clustering and partitioning strategies defined during table creation. Table renaming and truncation for staged updates will be achieved using BigQuery's `CREATE OR REPLACE TABLE` or by managing table swaps within the orchestration.

## 6. External Dependencies
The original job interacts with an Oracle database and local file system for logging.

*   **Oracle Database**: All source tables (`sof$ta_p_rech_empf`, `sof$ta_p_vertrag`, etc.) and target tables (`RPT$TA_S_D1_RECH_EMPF`, etc.) reside in an Oracle database.
    *   **Replacement**: Data from these Oracle tables will need to be ingested into BigQuery. This can be achieved via:
        *   **Batch ETL**: Tools like Cloud Data Fusion, Dataflow, or custom Python scripts to extract data from Oracle, transform if necessary, and load into BigQuery.
        *   **CDC (Change Data Capture)**: For real-time or near real-time updates, solutions like Striim or Debezium with Kafka Connect could be used to stream changes from Oracle to BigQuery.
*   **UC4 Scheduler**: The job is scheduled by UC4.
    *   **Replacement**: Cloud Composer (Apache Airflow) will be used to schedule and orchestrate the BigQuery stored procedures.
*   **Local File System Logging (`.log` file)**: The shell script writes log messages to a local `.log` file and echoes `p_Stichtag` to `${BERT_DIR_ROOT}/../../LOG/DATENSTAND.log`.
    *   **Replacement**: All logging will be directed to a BigQuery `job_audit_log` table for structured and searchable logs. Cloud Logging can also capture execution logs from Cloud Composer and BigQuery. The `DATENSTAND.log` file can be replaced by an entry in the audit log or a dedicated metadata table.
*   **Utility Shell Scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`)**: These provide environment setup, error handling, parameter parsing, date utilities, and SQL*Plus invocation.
    *   **Replacement**: Their functionalities will be absorbed into the BigQuery stored procedures (for parameter handling, date logic, error handling via `BEGIN...EXCEPTION`) or handled by the Airflow DAG (for environment variables and orchestration-level error capture).

## 7. Unresolved / Risks
*   **Oracle PL/SQL Calls**: The calls to `isbert_schema.dwpa_util_skript.runstatement` for DDL operations need to be carefully reviewed. These typically indicate custom utility functions. The exact logic within these `runstatement` calls will need to be understood to ensure correct BigQuery equivalent implementation for schema management and table swaps (e.g., atomic table replacement patterns in BigQuery). This represents a potential manual step or requires further investigation.
*   **Performance Tuning**: The original SQL uses Oracle `/*+ parallel */` hints. While BigQuery automatically handles parallelism, complex queries with many joins and `CASE` statements may require BigQuery-specific optimizations like appropriate table partitioning, clustering, and materialized views to achieve optimal performance.
*   **Data Volume**: The design assumes data will be efficiently loaded into BigQuery. Large volumes may require specific ingestion strategies (e.g., BigQuery Data Transfer Service, Dataflow).
*   **Historical Data**: No explicit historization logic was identified, but the "Stichtags-Abzug" suggests a snapshotting mechanism. Ensure the BigQuery implementation correctly preserves historical snapshots if that is the intent.
*   **Dynamic SQL**: The script `k_ausd_austausch.ksh` uses `starteSQLSkript` which is presumed to execute `d_ausd_austausch.sql`. If `d_ausd_austausch.sql` were dynamically generated or altered at runtime by `k_ausd_austausch.ksh` based on parameters (beyond simple variable substitution), this would add complexity. Based on current analysis, it appears to be a static script.

## 8. Build Plan
The build plan involves creating BigQuery assets and a Cloud Composer DAG.

1.  **BigQuery DDL for Source Tables**:
    *   Create BigQuery tables for all Oracle source tables (`sof$ta_p_rech_empf`, `sof$ta_p_vertrag`, `sof$ta_p_basisprod`, `sof$ta_p_gesch_part`, `sof$ta_p_dn_nutzer`, `sof$ta_p_evn_empf`, `sof$ta_p_discount`, `sof$ta_p_discount_rr`, `sof$ta_p_d1_vpn`).
    *   Ensure correct data type mapping from Oracle to BigQuery.
    *   (Language: BigQuery SQL)

2.  **BigQuery DDL for Target Tables**:
    *   Create BigQuery tables for the final output tables (`rpt$ta_s_d1_rech_empf`, `rpt$ta_s_d1_vertrag`, `rpt$ta_s_d1_rech_kunde`, `rpt$ta_s_d1_discount`, `rpt$ta_s_d1_discount_rr`, `rpt$ta_s_d1_vpn`).
    *   Consider partitioning and clustering keys based on query patterns and data access.
    *   (Language: BigQuery SQL)

3.  **BigQuery DDL for Audit/Metadata Tables**:
    *   Create `job_audit_log` table (e.g., `job_nr`, `job_kennung`, `event_type`, `event_ts`, `stichtag`, `restart_value`, `message`).
    *   Create `job_sequence` table if needed for auto-incrementing job IDs.
    *   (Language: BigQuery SQL)

4.  **BigQuery Stored Procedure: `k_ausd_austausch`**:
    *   Translate the `d_ausd_austausch.sql` logic into a BigQuery stored procedure.
    *   Implement the `INSERT INTO ... SELECT FROM ...` statements, `CASE` logic, and function conversions.
    *   Address the Oracle DDL (`RENAME`, `TRUNCATE`, `CREATE INDEX`, `DROP INDEX`, `ANALYZE TABLE`) and `isbert_schema.dwpa_util_skript.runstatement` calls by implementing BigQuery-native table management (e.g., `CREATE OR REPLACE TABLE AS SELECT`, atomic table swaps, or `MERGE` statements).
    *   (Language: BigQuery SQL)

5.  **BigQuery Stored Procedure: `BERT_AUSTAUSCH_KSH`**:
    *   Translate the `r_ausd_austausch.ksh` logic into a BigQuery stored procedure.
    *   Implement parameter handling, date defaulting, restart value logic, logging to `job_audit_log`, and the call to `k_ausd_austausch` stored procedure.
    *   Include `BEGIN ... EXCEPTION` blocks for error handling.
    *   (Language: BigQuery SQL)

6.  **Cloud Composer (Airflow) DAG**:
    *   Develop a Python-based Airflow DAG to:
        *   Define the schedule (replacing UC4).
        *   Define operators to call the BigQuery stored procedure `BERT_AUSTAUSCH_KSH`, passing the necessary parameters.
        *   Implement retry mechanisms and alerts.
        *   (Language: Python)

7.  **Data Ingestion Pipeline**:
    *   Set up a mechanism (e.g., Dataflow job, Data Transfer Service, custom script) to continuously or periodically ingest data from the Oracle source tables into their respective BigQuery counterparts.
    *   (Language: Python/SQL/JSON configuration for Cloud services)