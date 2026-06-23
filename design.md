# Migration Design — DW.BERT_AUSD_V_TA_CNTRCT_TEMPL

## 1. Purpose & Scope
This document outlines the migration design for the ETL job `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` from its legacy UC4/KornShell/Oracle SQL environment to Google Cloud Platform, specifically leveraging Airflow for orchestration and BigQuery for data transformation and storage.

The primary purpose of this job, as indicated by its UC4 title, is to "Mirror Carmen contract templates". It involves extracting contract template data from source Oracle tables, applying filtering logic, and loading it into a target Oracle table. The job is scheduled and managed by UC4, with KornShell scripts handling environment setup, logging, parameter passing, and the execution of the core Oracle SQL transformation.

## 2. Source Inventory
The `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` job is composed of four key files:

| Relative Path                                                                                                   | Technology         | Category | Tool            | Purpose                                                                                                                                                                                                                                                                                                                                                                                                | Migration Bucket |
| :-------------------------------------------------------------------------------------------------------------- | :----------------- | :------- | :-------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.xml` | UC4 XML            | `uc4`    | `UC4/Automic`   | Defines a UC4 UNIX job named `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` that executes a KornShell script to mirror Carmen contract templates. This is the top-level orchestrator.                                                                                                                                                                                                                           | `semi_auto`      |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh`                        | KornShell Script   | `shell`  | `KornShell`     | This script acts as a wrapper, handling environment initialization (`.dw_init`), robust error handling (`f_alis_msgerr.ksh`), parameter parsing, and logging. It then invokes the core control script `k_ausd_v_ta_cntrct_templ.ksh`.                                                                                                                                                            | `semi_auto`      |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh`                        | KornShell Script   | `shell`  | `KornShell`     | This control script is responsible for environment setup, parameter parsing, error handling, and orchestrating the execution of an SQL script (`d_ausd_v_ta_cntrct_templ.sql`). It contains the `starteSQLSkript` function call to execute the SQL.                                                                                                                                              | `semi_auto`      |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_templ.sql`                        | Oracle SQL*Plus    | `sql`    | `Oracle SQL*Plus` | This SQL*Plus script truncates a target table (`sof$ta_cntrct_templ`) and then populates it by selecting and joining data from source tables (`cds$ta_cntrct_template` and `cds$ta_care_description`) through an Oracle DB link (`@pcrs1`), applying date-based filtering and `is_production` status.                                                                                                    | `semi_auto`      |

## 3. Target Architecture
The migrated job will run on Google Cloud Platform, utilizing the following components:
*   **Orchestration:** Apache Airflow on Cloud Composer.
*   **Data Transformation:** Google BigQuery for SQL execution.
*   **Data Ingestion/Staging:** Data from the external Oracle system will be staged in BigQuery tables, likely via data transfer services or federated queries.
*   **Data Storage:** BigQuery datasets will store the transformed data.

The overall architecture will involve an Airflow DAG that orchestrates the execution of BigQuery SQL statements. The shell script logic (environment setup, parameter passing, logging, error handling) will be integrated into the Airflow DAG's Python code using Airflow's native capabilities.

## 4. Data Flow & Lineage
The original data flow is sequential:
1.  **UC4 Job `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.xml`** initiates the process.
2.  It executes **`r_ausd_v_ta_cntrct_templ.ksh`**.
3.  **`r_ausd_v_ta_cntrct_templ.ksh`** in turn executes **`k_ausd_v_ta_cntrct_templ.ksh`**.
4.  **`k_ausd_v_ta_cntrct_templ.ksh`** executes **`d_ausd_v_ta_cntrct_templ.sql`**.
5.  **`d_ausd_v_ta_cntrct_templ.sql`**
    *   Reads from Oracle tables `cds$ta_cntrct_template@pcrs1` and `cds$ta_care_description@pcrs1` (accessed via DB link `@pcrs1`).
    *   Truncates the Oracle table `sof$ta_cntrct_templ`.
    *   Inserts transformed data into `sof$ta_cntrct_templ`.

The target data flow will be as follows:
1.  **Airflow DAG `dw_bert_ausd_v_ta_cntrct_templ`** (replacing the UC4 job) is triggered, likely on a schedule or external event.
2.  An Airflow task (e.g., `BigQueryOperator` or `PythonOperator` calling BigQuery client) will execute the converted BigQuery SQL. This task will encapsulate the combined logic of `r_ausd_v_ta_cntrct_templ.ksh` and `k_ausd_v_ta_cntrct_templ.ksh` (parameter handling, error trapping, logging) and the core SQL transformation.
3.  The BigQuery SQL will perform the following actions:
    *   Read from BigQuery tables `cds_ta_cntrct_template` and `cds_ta_care_description` (these tables will be pre-populated from the source Oracle system).
    *   Truncate the target BigQuery table `sof_ta_cntrct_templ`.
    *   Insert the transformed data into `sof_ta_cntrct_templ`.

The logical flow (`DAG start -> SQL execution -> DAG end`) will remain the same.

## 5. Transformation Logic
### Orchestration (UC4 and KornShell scripts)
The UC4 job `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.xml` will be converted to an Airflow DAG named `dw_bert_ausd_v_ta_cntrct_templ`. The DAG will have a single main task responsible for the data transformation.

The KornShell scripts (`r_ausd_v_ta_cntrct_templ.ksh` and `k_ausd_v_ta_cntrct_templ.ksh`) provide the orchestration logic:
*   **Environment Setup:** `. $HOME/.dw_init` and sourcing other utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). These will be replaced by Airflow connections, variables, and potentially a custom Python utility module for common functions.
*   **Parameter Passing:** The `getopts` logic for `j` (JobKennung) and `f` (EintragsNr) parameters will be translated to Airflow task parameters or XComs.
*   **Logging and Error Handling:** The `DWMSG_` functions and `trap` commands will be replaced by Airflow's native logging and error handling mechanisms. Task failures in Airflow will be managed by retry policies and `on_failure_callback` if needed.
*   **SQL Execution:** The `starteSQLSkript` function call, which executes `d_ausd_v_ta_cntrct_templ.sql`, will be directly translated to a BigQueryOperator task that runs the converted BigQuery SQL.

### Data Transformation (Oracle SQL)
The Oracle SQL script `d_ausd_v_ta_cntrct_templ.sql` will be converted to BigQuery SQL, as provided by the `hql_sql_to_bqsql_design` tool output:

**Source Oracle SQL Snippet:**
```sql
-- Stichtag ermitteln
COLUMN s_datum new_value v_datum noprint
SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
  FROM isbert_schema.dwtk_meldungen m
 WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

-- Truncate and Insert
INSERT INTO sof$ta_cntrct_templ
(CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION )
SELECT
        ct.cntrct_template_id,
        ct.cds_description_id,
        cd.cds_description
FROM
        cds$ta_cntrct_template     &v_carmen     ct,
        cds$ta_care_description    &v_carmen     cd
WHERE
        ct.cds_description_id    = cd.cds_description_id
AND     ct.insert_at <= TO_DATE('&v_datum','YYYYMMDD')
AND     (   ct.modified_at IS NULL OR ct.modified_at > TO_DATE('&v_datum','YYYYMMDD')     )
AND     ct.valid_from <= TO_DATE('&v_datum','YYYYMMDD')
AND     (   ct.valid_to IS NULL OR ct.valid_to > TO_DATE('&v_datum','YYYYMMDD')       )
AND     ct.is_production = 1
AND     cd.language = 1;
```

**Target BigQuery SQL:**
```sql
-- BigQuery SQL Script

DECLARE v_datum STRING DEFAULT (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

TRUNCATE TABLE `sof_ta_cntrct_templ`;

INSERT INTO `sof_ta_cntrct_templ`
(
  CNTRCT_TEMPLATE_ID,
  CDS_DESCRIPTION_ID,
  CDS_DESCRIPTION
)
SELECT
  ct.cntrct_template_id,
  ct.cds_description_id,
  cd.cds_description
FROM `cds_ta_cntrct_template` ct
JOIN `cds_ta_care_description` cd
  ON ct.cds_description_id = cd.cds_description_id
WHERE ct.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (ct.modified_at IS NULL OR ct.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND ct.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (ct.valid_to IS NULL OR ct.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND ct.is_production = 1
  AND cd.language = 1;
```
Key transformations:
*   Oracle-specific functions (`NVL`, `TO_CHAR`, `TO_DATE`) are replaced with BigQuery equivalents (`IFNULL`, `FORMAT_DATE`, `PARSE_DATE`).
*   SQL*Plus commands (`COLUMN`, `DEFINE`, `START`, `SPOOL`, `WHENEVER SQLERROR`) are removed as they are not applicable in BigQuery.
*   The `TRUNCATE TABLE` statement is adapted to BigQuery syntax.
*   Table references are updated to use BigQuery's backtick-delimited format (e.g., `isbert_schema.dwtk_meldungen` and `sof_ta_cntrct_templ`, `cds_ta_cntrct_template`, `cds_ta_care_description`). The `&v_carmen` DB link reference is removed, assuming the source data will be available in BigQuery.
*   The `COMMIT` statement is removed as BigQuery DML operations are atomic.

## 6. External Dependencies
The original job has a critical external dependency:
*   **Oracle Database via DB Link `@pcrs1`:** The SQL script `d_ausd_v_ta_cntrct_templ.sql` reads data from `cds$ta_cntrct_template` and `cds$ta_care_description` on a remote Oracle instance via the DB link `@pcrs1`.

**Replacement Strategy:**
The data from these Oracle source tables must be migrated or made accessible in BigQuery. The recommended approach is to establish a robust data ingestion pipeline that periodically transfers data from the source Oracle tables into dedicated staging tables within BigQuery. Options include:
*   **Change Data Capture (CDC):** Using tools like Google Cloud DataStream to continuously replicate changes from Oracle to BigQuery.
*   **Batch Ingestion:** Regularly exporting data from Oracle to Cloud Storage and then loading it into BigQuery using Data Transfer Service or custom scripts.
*   **BigQuery Federated Queries (Cloud SQL):** If the Oracle database can be migrated to Cloud SQL, BigQuery federated queries could directly access the data.

For this migration, it is assumed that `cds_ta_cntrct_template` and `cds_ta_care_description` BigQuery tables exist and contain the up-to-date data from the source Oracle system.

## 7. Unresolved / Risks
*   **Source Table Availability:** The primary risk is ensuring the continuous and timely availability of `cds_ta_cntrct_template` and `cds_ta_care_description` in BigQuery, reflecting the data from the external Oracle system. The chosen data ingestion strategy must be reliable.
*   **KornShell Utility Script Equivalents:** The `.dw_init` and other `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` utility scripts contain functions for environment setup, logging, parameter parsing, and database interaction. These need to be thoroughly reviewed and re-implemented in Python within the Airflow context, ensuring all functionalities (especially related to logging and error reporting) are fully replicated.
*   **"BERT_DROP_TEMP_TABLE" Job Dependency:** The SQL script determines a processing date (`v_datum`) by querying `isbert_schema.dwtk_meldungen` filtered by `job_kennung = 'BERT_DROP_TEMP_TABLE'`. This implies a dependency on another job (`BERT_DROP_TEMP_TABLE`) to update this metadata table. The migration of `BERT_DROP_TEMP_TABLE` and its interaction with `isbert_schema.dwtk_meldungen` must be considered to ensure the correct `v_datum` is derived in BigQuery.

## 8. Build Plan
The following components will be generated as part of the migration:

1.  **Airflow DAG (Python):**
    *   **Filename:** `dw_bert_ausd_v_ta_cntrct_templ.py`
    *   **Language:** Python
    *   **Content:** This DAG will replace the UC4 job. It will contain tasks for the following:
        *   Initial environment setup and parameter parsing (Python equivalent of KornShell logic).
        *   A BigQueryOperator task to execute the transformed SQL.
        *   Airflow-native logging and error handling.

2.  **BigQuery SQL Script:**
    *   **Filename:** `d_ausd_v_ta_cntrct_templ_bq.sql`
    *   **Language:** BigQuery SQL
    *   **Content:** The transformed SQL script derived from `d_ausd_v_ta_cntrct_templ.sql`, including the `DECLARE`, `TRUNCATE`, and `INSERT` statements with BigQuery-specific syntax.

3.  **BigQuery Tables (DDL/Schema Definitions):**
    *   **Target Table:** `sof_ta_cntrct_templ` (DDL for this table in BigQuery).
    *   **Source Staging Tables:** `cds_ta_cntrct_template` and `cds_ta_care_description` (DDL for these tables, to be populated by the data ingestion pipeline from Oracle).
    *   **Metadata Table:** `isbert_schema.dwtk_meldungen` (DDL for this table in BigQuery, assuming it's part of a broader metadata migration or recreation).

This build plan focuses on generating the core executable components for the migrated job. Additional utility modules or helper functions in Python may be required to fully replicate the KornShell script functionalities not directly covered by Airflow operators.