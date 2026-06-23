# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh

## 1. Purpose & Scope
This document outlines the migration design for the ETL job identified by the seed `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh`. The primary purpose of this job is to extract and prepare selected "Basisprodukte" (base products), specifically APN (Access Point Name) contract data, for the BERT system. The job processes data based on a "Stichtag" (reference date) and populates a target table `sof$ta_apn_carmen` with relevant contract and access point name information. The scope of this migration is to re-platform the entire workflow, including orchestration and data transformation, to Google Cloud's BigQuery.

## 2. Source Inventory

**Seed File:**
*   **vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh**
    *   **Category:** shell
    *   **Tool:** KornShell
    *   **Tier:** medium
    *   **Automation Bucket:** semi_auto (B2)
    *   **Purpose:** Main orchestration script, handles parameter parsing, logging setup, and invokes the core logic script.

**Associated Files (Identified via code analysis):**
*   **vobs/dw_source/isrpt/isbert/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh**
    *   **Category:** Shell Script (Inferred)
    *   **Tool:** KornShell (Inferred)
    *   **Tier:** (Not assessed by `file_complexity`)
    *   **Automation Bucket:** (Not assessed by `automation_rate`)
    *   **Purpose:** Intermediate orchestration script, validates parameters, and executes the core SQL transformation script.
*   **vobs/dw_source/isrpt/isbert/aufbereitung/sql/d_ausd_bp_ta_apn_carmen.sql**
    *   **Category:** SQL Script (Inferred)
    *   **Tool:** Oracle SQL*Plus (Inferred)
    *   **Tier:** (Not assessed by `file_complexity`)
    *   **Automation Bucket:** (Not assessed by `automation_rate`)
    *   **Purpose:** Core data transformation logic, reads from source tables, and writes to the target table.
*   **vobs/dw_source/isrpt/isbert/allgemein/is/util/bin/h_alis_sqlplus.ksh**
    *   **Category:** Utility Shell Script (Inferred)
    *   **Tool:** KornShell (Inferred)
    *   **Purpose:** Contains helper functions for executing SQL*Plus scripts.
*   **vobs/dw_source/isrpt/isbert/aufbereitung/bin/gestern.ksh**
    *   **Category:** Utility Shell Script (Inferred)
    *   **Tool:** KornShell (Inferred)
    *   **Purpose:** Calculates today's and yesterday's date.
*   **vobs/dw_source/isrpt/isbert/allgemein/is/util/bin/f_alis_msgerr.ksh** (Inferred)
    *   **Purpose:** Custom error messaging utility.
*   **vobs/dw_source/isrpt/isbert/allgemein/is/util/bin/h_alis_parameter.ksh** (Inferred)
    *   **Purpose:** Custom parameter parsing utility.
*   **vobs/dw_source/isrpt/isbert/allgemein/is/util/bin/h_alis_date.ksh** (Inferred)
    *   **Purpose:** Custom date handling utility.

**Source Tables (Oracle):**
*   `isbert_schema.dwtk_meldungen` (Control table for `v_datum`)
*   `pds$ta_pdp_context_assoc` (via `&v_carmen` database link/service, likely remote)
*   `pds$ta_pdp_context` (via `&v_carmen` database link/service, likely remote)
*   `pds$ta_access_point` (via `&v_carmen` database link/service, likely remote)

**Target Table (Oracle):**
*   `sof$ta_apn_carmen`

**Stored Procedure (Oracle):**
*   `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` (Used for truncating the target table)

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform (GCP) services:
*   **Data Warehouse:** Google BigQuery will serve as the target data warehouse. All source Oracle tables involved in the transformation will either be replicated to BigQuery or accessed via federated queries. The target `sof$ta_apn_carmen` table will be created in BigQuery.
*   **Orchestration:** Apache Airflow on Cloud Composer will replace the KornShell scripts for job scheduling and execution. Python DAGs will be developed to manage the workflow, parameter passing, logging, and error handling.
*   **Transformation:** The core SQL logic from `d_ausd_bp_ta_apn_carmen.sql` will be converted to BigQuery Standard SQL and executed as a BigQuery job within the Airflow DAG.
*   **Utility Functions:** Existing KornShell utility scripts (`gestern.ksh`, `h_alis_sqlplus.ksh`, error/parameter/date handling utilities) will be re-implemented in Python or replaced with native Airflow/BigQuery functionalities.

## 4. Data Flow & Lineage
**Current Data Flow:**
1.  The `r_ausd_bp_ta_apn_carmen.ksh` script is the entry point, likely triggered by a scheduler like UC4.
2.  It initializes environment variables, sets up custom logging (`DWMSG_`), parses command-line arguments for "Stichtag" and "Wiederanlaufwert", and determines the `p_stichtag`.
3.  It then invokes `k_ausd_bp_ta_apn_carmen.ksh`, passing the determined parameters.
4.  `k_ausd_bp_ta_apn_carmen.ksh` further parses these parameters, calculates `p_datum_heute` and `p_datum_gestern` using `gestern.ksh`, and most importantly, executes `d_ausd_bp_ta_apn_carmen.sql` via `starteSQLSkript` from `h_alis_sqlplus.ksh`.
5.  `d_ausd_bp_ta_apn_carmen.sql` first reads the latest `timecreated` from `isbert_schema.dwtk_meldungen` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` to derive a `v_datum` (control date).
6.  It then truncates the `sof$ta_apn_carmen` table using an Oracle stored procedure call.
7.  Finally, it inserts data into `sof$ta_apn_carmen` by joining `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, and `pds$ta_access_point`. These source tables are accessed via the `@pcrs1` database link/service. The data selection includes date-based filtering using the `v_datum`.

**Target Data Flow (Airflow & BigQuery):**
1.  An Airflow DAG (`r_ausd_bp_ta_apn_carmen_dag.py`) will be created to manage the job.
2.  The DAG will define tasks for parameter acquisition (e.g., current date as "Stichtag").
3.  A PythonOperator will replace the functionality of `gestern.ksh` and other shell utilities to prepare parameters for the BigQuery SQL.
4.  A BigQueryOperator will execute the migrated BigQuery SQL script (`d_ausd_bp_ta_apn_carmen.sql`).
5.  The BigQuery SQL script will perform the data extraction and transformation.
    *   It will query `isbert_schema.dwtk_meldungen` (migrated to BigQuery) to determine the control date.
    *   It will truncate the `sof$ta_apn_carmen` target table (BigQuery).
    *   It will read from `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, and `pds$ta_access_point` (either replicated BigQuery tables or federated sources) and insert into `sof$ta_apn_carmen` (BigQuery).
6.  Airflow's native logging and error handling mechanisms will replace the custom KornShell implementations.

## 5. Transformation Logic
The core transformation logic resides in `d_ausd_bp_ta_apn_carmen.sql`.
*   **Date Determination:** A control date `v_datum` is derived from `MAX(m.timecreated)` in `isbert_schema.dwtk_meldungen` for a specific job key (`'BERT_DROP_TEMP_TABLE'`). If no record is found, `'19000101'` is used.
*   **Target Table Preparation:** The `sof$ta_apn_carmen` table is truncated prior to data insertion.
*   **Data Selection and Insertion:** Data is selected from three source tables (PDS$TA_PDP_CONTEXT_ASSOC, PDS$TA_PDP_CONTEXT, PDS$TA_ACCESS_POINT) and joined on `pdp_context_id` and `access_point_id`. The selection criteria include multiple date-based filters (`insert_at`, `modified_at`, `valid_from`, `valid_to`) against the `v_datum`, ensuring `pc.is_production = 1`, and `pca.cntrct_id is not null`. The `cntrct_id` and `access_point_name` are inserted into `sof$ta_apn_carmen`.

**Equivalent BigQuery SQL (from `hql_sql_to_bqsql_design`):**
```sql
-- BigQuery Script

DECLARE v_carmen STRING DEFAULT '@pcrs1'; -- Placeholder for external system reference
DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- step01: delete temporary table contents
TRUNCATE TABLE `sof$ta_apn_carmen`;

-- step10a: create local copy of carmen-apn table
INSERT INTO `sof$ta_apn_carmen`
  (CNTRCT_ID, ACCESS_POINT_NAME)
SELECT
  pca.cntrct_id,
  ap.access_point_name
FROM `pds$ta_pdp_context_assoc` pca
JOIN `pds$ta_pdp_context` pc
  ON pca.pdp_context_id = pc.pdp_context_id
JOIN `pds$ta_access_point` ap
  ON pc.access_point_id = ap.access_point_id
WHERE pca.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (pca.modified_at IS NULL OR pca.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND pca.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (pca.valid_to IS NULL OR pca.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND pc.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (pc.modified_at IS NULL OR pc.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND pc.is_production = 1
  AND ap.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (ap.modified_at IS NULL OR ap.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND pca.cntrct_id IS NOT NULL;
```

## 6. External Dependencies
*   **External Oracle Instance (`pcrs1`):** The source tables `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, and `pds$ta_access_point` are accessed via `@pcrs1`.
    *   **Replacement Strategy:** This external dependency must be addressed by either:
        1.  **Data Replication:** Implementing a data replication solution (e.g., Google Cloud Datastream or a custom ETL process) to continuously copy these Oracle tables to BigQuery. This is the preferred approach for performance and data freshness.
        2.  **Federated Queries:** If data freshness requirements allow for near real-time querying without replication, BigQuery Federated Queries can be used to directly query the Oracle database. This introduces dependency on Oracle's availability and network latency.
*   **Oracle Stored Procedure (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`):** Used for `TRUNCATE TABLE`.
    *   **Replacement Strategy:** This will be replaced by direct BigQuery DDL (`TRUNCATE TABLE \`project.dataset.table\``) within the BigQuery SQL script.
*   **Custom KornShell Utilities:**
    *   `f_alis_msgerr.ksh` (error handling), `h_alis_parameter.ksh` (parameter parsing), `h_alis_date.ksh` (date handling), `h_alis_sqlplus.ksh` (SQL*Plus wrapper), `gestern.ksh` (date calculation).
    *   **Replacement Strategy:** These utilities will be replaced by native Python functions within the Airflow DAG or standard Airflow operators, and BigQuery's built-in date/time functions. Airflow provides robust logging and parameter management.

## 7. Unresolved / Risks
*   **`pcrs1` External System Connectivity:** The exact nature and connectivity of the `@pcrs1` database link/service are not fully detailed. A thorough investigation is required to determine the best migration strategy (replication vs. federated query) and implement secure connectivity from GCP.
*   **Custom Utility Re-implementation:** While common functions like date calculations are straightforward, the custom error handling (`DWMSG_`) and parameter parsing (which might have specific logic) need careful re-implementation to ensure equivalent functionality and robustness in Python/Airflow.
*   **Missing Metadata for Utility Scripts:** `k_ausd_bp_ta_apn_carmen.ksh`, `d_ausd_bp_ta_apn_carmen.sql`, and other utility scripts were not found in `file_analysis`, `file_complexity`, or `automation_rate` for this job. This implies a higher manual effort for their migration as automated assessment and guidance are limited.
*   **Data Types and Schema Mapping:** While the `hql_sql_to_bqsql_design` tool provides conversion rules for general types, a detailed schema mapping is required for all source tables (`pds$*` and `dwtk_meldungen`) to ensure correct BigQuery table definitions (e.g., exact numeric precision, string lengths, nullability).
*   **Performance Tuning:** The `d_ausd_bp_ta_apn_carmen.sql` script uses `WHENEVER SQLERROR CONTINUE` and `WHENEVER SQLERROR EXIT FAILURE` as well as `set timing on`. These SQL*Plus specific settings will be replaced by Airflow's error handling and logging, but performance characteristics of the migrated BigQuery SQL will need to be monitored and tuned.

## 8. Build Plan
1.  **BigQuery Schema Definition (DDL):**
    *   Create BigQuery tables for `isbert_schema.dwtk_meldungen` and `sof$ta_apn_carmen` (target table), ensuring accurate data type mapping from Oracle.
    *   Define BigQuery schemas for the `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, and `pds$ta_access_point` tables based on their Oracle definitions.
2.  **Data Ingestion/Connectivity for Source Tables:**
    *   Implement a data replication solution (e.g., Cloud Datastream to Cloud Storage, then to BigQuery, or direct BigQuery Data Transfer Service if applicable) for `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, `pds$ta_access_point` from the external Oracle instance (`pcrs1`) to BigQuery.
    *   Migrate `isbert_schema.dwtk_meldungen` to BigQuery.
3.  **BigQuery SQL Script Conversion:**
    *   Finalize the BigQuery Standard SQL script for `d_ausd_bp_ta_apn_carmen.sql` based on the provided design, including placeholders for external table references (`@pcrs1`).
    *   Replace Oracle-specific syntax (e.g., `NVL`, `TO_DATE`, `TO_CHAR`) with BigQuery equivalents (`COALESCE`, `PARSE_DATE`, `FORMAT_DATE`).
    *   Ensure the `TRUNCATE TABLE` statement is correctly implemented for BigQuery.
4.  **Airflow DAG Development (Python):**
    *   Create an Airflow DAG (`r_ausd_bp_ta_apn_carmen_dag.py`) in Python.
    *   Implement parameter handling (e.g., for "Stichtag") using Airflow variables or DAG parameters.
    *   Replace `gestern.ksh` functionality with Python date logic.
    *   Develop a task using `BigQueryOperator` to execute the converted BigQuery SQL script.
    *   Integrate Airflow's logging and monitoring.
    *   Replace custom error handling (`DWMSG_`) with Airflow's retry mechanisms and alerts.
5.  **Testing:**
    *   Unit tests for BigQuery SQL logic.
    *   Integration tests for the Airflow DAG and its interaction with BigQuery.
    *   Data validation to ensure migrated job produces identical or equivalent results to the legacy job.