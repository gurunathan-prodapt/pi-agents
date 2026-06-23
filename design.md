# Migration Design — DW.BERT_AUSD_V_TA_CNTRCT_CRS3

## 1. Purpose & Scope
This document outlines the migration design for the ETL job `DW.BERT_AUSD_V_TA_CNTRCT_CRS3` from its legacy UC4, KornShell, and Oracle SQL environment to Google Cloud Platform (GCP) with BigQuery as the target data warehouse and Airflow for orchestration. The primary purpose of this job is to update contract data, including twin-bill information, into the `sof$ta_cntrct_crs3` table. The scope of this migration includes the conversion of the UC4 scheduler, the KornShell wrapper and control scripts, and the core Oracle SQL transformation logic.

## 2. Source Inventory

The `DW.BERT_AUSD_V_TA_CNTRCT_CRS3` assembled job comprises the following components:

| File Name | Relative Path | Technology | Category | Purpose/Summary | Migration Bucket | Complexity Tier |
|---|---|---|---|---|---|---|
| `DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml` | `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml` | UC4/Automic | uc4 | UC4 job definition for a Unix job that executes a KornShell script to update contracts. | semi_auto | (not directly assessed) |
| `r_ausd_v_ta_cntrct_crs3.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh` | KornShell | shell | Wrapper script for synchronizing contract data into the `ta_cntrct_crs3` table, handling parameter parsing, environment setup, and error logging. | semi_auto | (not directly assessed) |
| `k_ausd_v_ta_cntrct_crs3.ksh` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh` | KornShell | shell | Control script for `r_ausd_vertrag.ksh` (parent wrapper), handling job activation/deactivation, parameter parsing, and orchestrating the execution of an SQL script. | semi_auto | (not directly assessed) |
| `d_ausd_v_ta_cntrct_crs3.sql` | `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs3.sql` | Oracle SQL | sql | Core SQL script that truncates and populates the `SOF$TA_CNTRCT_CRS3` table with contract data, including twinbill information. | semi_auto | (not directly assessed) |

## 3. Target Architecture
The target architecture will leverage Google Cloud Platform services for scheduling, data processing, and storage.

*   **Orchestration:** Apache Airflow on Cloud Composer will replace the UC4 scheduler. A single DAG will be created for this job.
*   **Transformation:** The Oracle SQL will be converted to BigQuery SQL. The KornShell scripts will be re-implemented as Python scripts to manage parameters, environment, and execute the BigQuery SQL. These Python scripts can be executed via a DataprocSubmitJobOperator or a PythonOperator within the Airflow DAG. Given the UC4 design's recommendation, a PySpark placeholder is suggested for Dataproc execution, implying a Python-based transformation.
*   **Data Warehouse:** BigQuery will serve as the target data warehouse. All Oracle tables will be migrated to BigQuery datasets and tables.
*   **Logging & Monitoring:** Cloud Logging and Cloud Monitoring will replace existing logging mechanisms.

## 4. Data Flow & Lineage
The original job exhibits a chained execution flow:
1.  **UC4 Job (`DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml`)**: Triggers the execution.
2.  **KornShell Wrapper (`r_ausd_v_ta_cntrct_crs3.ksh`)**: Sets up the execution environment (e.g., sourcing `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`), handles parameters, and calls the control script.
3.  **KornShell Controller (`k_ausd_v_ta_cntrct_crs3.ksh`)**: Further prepares the environment, extracts job metadata, and executes the Oracle SQL script `d_ausd_v_ta_cntrct_crs3.sql`. This script also interacts with `isbert_schema.dwtk_meldungen` for a date variable and calls `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for SQL execution.
4.  **Oracle SQL (`d_ausd_v_ta_cntrct_crs3.sql`)**:
    *   Reads `isbert_schema.dwtk_meldungen` to determine a processing date.
    *   Truncates the target table `sof$ta_cntrct_crs3`.
    *   Reads data from `sof$ta_cntrct_crs2`.
    *   Performs a `UNION` operation involving two branches of contract data from `sof$ta_cntrct_crs2`, joining on `cntrct_parent` and filtering by `cntrct_ty` to identify twin-bill information.
    *   Inserts the processed data into `sof$ta_cntrct_crs3`.

**Target Data Flow:**
1.  **Airflow DAG (`dw_bert_ausd_v_ta_cntrct_crs3`)**: Triggered manually or by a defined schedule (once an `EVNT_TIME` equivalent is established).
2.  **Python Script (e.g., `r_ausd_v_ta_cntrct_crs3.py` on Dataproc)**: This script, replacing the KornShell wrappers, will:
    *   Handle environment setup and parameter parsing.
    *   Execute the converted BigQuery SQL. This can be done directly by calling the BigQuery API from Python or using `bq` command-line tools.
3.  **BigQuery SQL (`d_ausd_v_ta_cntrct_crs3_bq.sql`)**:
    *   Reads from `dwtk_meldungen` (migrated to BigQuery).
    *   Truncates the target table `sof_ta_cntrct_crs3` (migrated to BigQuery).
    *   Reads from `sof_ta_cntrct_crs2` (migrated to BigQuery).
    *   Performs the identical transformation logic as the original Oracle SQL, inserting into `sof_ta_cntrct_crs3`.

## 5. Transformation Logic
The core transformation logic resides in `d_ausd_v_ta_cntrct_crs3.sql`.

**Original Oracle SQL Logic:**
The script performs a `TRUNCATE` and `INSERT` operation. It first determines a processing date (`v_datum`) from the `isbert_schema.dwtk_meldungen` table. Then, it populates `sof$ta_cntrct_crs3` by combining two sets of contract data from `sof$ta_cntrct_crs2` using a `UNION`.
*   **First SELECT:** Selects contracts where `cntrct_ty` is not `10` or `20`, and optionally joins with `ctb` (a self-join alias for `sof$ta_cntrct_crs2`) for twin-bill information where `ctb.cntrct_ty = 20`.
*   **Second SELECT (UNIONed):** Selects twin-bill contracts specifically where `ctb.cntrct_ty = 20`, joined back to their parent contracts, also excluding `cntrct_ty = 10` or `20` for the parent.

**BigQuery SQL Conversion:**
The `hql_sql_to_bqsql_design` tool provided the following converted BigQuery SQL:

```sql
DECLARE v_datum STRING;

SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

TRUNCATE TABLE `sof$ta_cntrct_crs3`;

INSERT INTO `sof$ta_cntrct_crs3` (
        cntrct_id,
        obj_version,
        contract_number,
        cntrct_template_id,
        cntrct_validity_id,
        valid_from,
        com_per_ext_rea_cv,
        billcycle_id,
        vo_code,
        cntrct_start_date,
        cntrct_st,
        cntrct_parent,
        cntrct_ty,
        cost_centre,
        cost_centre_user,
        commitment_reference_date,
        order_number,
        rv_num,
        twinbill,
        twin_vertrag_id
)
SELECT
  c.cntrct_id,
  c.obj_version,
  c.contract_number,
  c.cntrct_template_id,
  c.cntrct_validity_id,
  c.valid_from,
  c.com_per_ext_rea_cv,
  c.billcycle_id,
  c.vo_code,
  c.cntrct_start_date,
  c.cntrct_st,
  c.cntrct_parent,
  c.cntrct_ty,
  c.cost_centre,
  c.cost_centre_user,
  c.commitment_reference_date,
  c.order_number,
  c.rv_num,
  CASE
    WHEN ctb.cntrct_id IS NOT NULL THEN 'TB'
  END AS twinbill,
  ctb.cntrct_id AS twin_vertrag_id
FROM `sof$ta_cntrct_crs2` c
LEFT JOIN `sof$ta_cntrct_crs2` ctb
  ON c.cntrct_id = ctb.cntrct_parent
 AND ctb.cntrct_ty = 20
WHERE c.cntrct_ty NOT IN (10, 20)

UNION ALL

SELECT
  ctb.cntrct_id,
  ctb.obj_version,
  ctb.contract_number,
  ctb.cntrct_template_id,
  ctb.cntrct_validity_id,
  ctb.valid_from,
  ctb.com_per_ext_rea_cv,
  ctb.billcycle_id,
  ctb.vo_code,
  ctb.cntrct_start_date,
  ctb.cntrct_st,
  ctb.cntrct_parent,
  ctb.cntrct_ty,
  ctb.cost_centre,
  ctb.cost_centre_user,
  ctb.commitment_reference_date,
  ctb.order_number,
  c.rv_num,
  'TB' AS twinbill,
  c.cntrct_id AS twin_vertrag_id
FROM `sof$ta_cntrct_crs2` c
JOIN `sof$ta_cntrct_crs2` ctb
  ON c.cntrct_id = ctb.cntrct_parent
WHERE ctb.cntrct_ty = 20
  AND c.cntrct_ty NOT IN (10, 20);
```

The conversion handles date formatting (`TO_CHAR` to `FORMAT_DATE`), `NVL` to `COALESCE`, and table/schema naming conventions for BigQuery. The procedural call `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for `TRUNCATE` is replaced by direct BigQuery `TRUNCATE TABLE` statement.

## 6. External Dependencies
The job has the following external dependencies in the legacy environment and their proposed replacements:

*   **Oracle Database:**
    *   **Description:** Source tables `sof$ta_cntrct_crs2`, target table `sof$ta_cntrct_crs3`, and metadata table `isbert_schema.dwtk_meldungen` reside in an Oracle database.
    *   **Replacement:** All these tables will be migrated to BigQuery. `isbert_schema` will likely become a BigQuery dataset, and the tables within it. Data will need to be ingested into BigQuery.
*   **UNIX Host (`DWHDWH1P`):**
    *   **Description:** The UC4 job runs on a specific UNIX host.
    *   **Replacement:** The Airflow DAG will run on Cloud Composer (managed Airflow). Python scripts will execute on a suitable GCP compute environment (e.g., Dataproc for PySpark scripts, or Cloud Run/Cloud Functions for simpler Python scripts, triggered by Airflow).
*   **KornShell (`ksh`) Environment:**
    *   **Description:** Shell scripts rely on the `ksh` interpreter and various sourced utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`).
    *   **Replacement:** The KornShell logic will be refactored into Python scripts. The functionality of the utility scripts will either be re-implemented in Python or replaced with equivalent Cloud SDK functions (e.g., Cloud Logging for error messages, environment variable handling). The `h_alis_sqlplus.ksh` utility will be replaced by BigQuery client libraries or `bq` CLI commands in Python.
*   **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement` (Oracle Procedure/Package):**
    *   **Description:** Used to execute SQL statements, specifically `TRUNCATE TABLE`.
    *   **Replacement:** In BigQuery, `TRUNCATE TABLE` is a standard DDL statement and can be executed directly within the BigQuery SQL script. No special utility package is required.
*   **DB-Link on CARMEN DB (`@pcrs1`):**
    *   **Description:** The Oracle SQL script defines `v_carmen = "@pcrs1"`, suggesting a database link to a CARMEN database. Although not explicitly used in the provided SQL, this indicates a potential cross-database dependency.
    *   **Replacement:** If the CARMEN database is a source, it will need to be identified and its relevant tables ingested into BigQuery. This might require additional data pipelines (e.g., using Datastream, Fivetran, or custom ETL).

## 7. Unresolved / Risks
*   **Missing UC4 Workflow Context:** The provided UC4 XML is for a single job and lacks parent JOBP (Job Plan) or JSCH (Job Schedule) definitions, as well as `EVNT_TIME` for a full schedule. The Airflow DAG design is therefore provisional and assumes a simple execution. The complete scheduling logic and dependencies from the legacy UC4 environment need to be fully analyzed for accurate Airflow DAG creation.
*   **KornShell Utility Scripts:** The detailed logic of the sourced KornShell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) needs to be fully understood to ensure accurate re-implementation in Python.
*   **Oracle `isbert_schema` and `sof$` Prefixes:** These schema/object prefixes will need to be mapped appropriately to BigQuery datasets. For example, `isbert_schema.dwtk_meldungen` could become `isbert_schema.dwtk_meldungen` or `project.dataset.dwtk_meldungen`.
*   **Oracle `PARALLEL` Hints:** The Oracle SQL uses `/*+ PARALLEL(c,4) PARALLEL (ctb,4) */` hints. BigQuery automatically handles parallelism, so these hints are not directly translated but imply a performance requirement that BigQuery's architecture should address.
*   **Variable `v_datum` usage:** The Oracle script defines `v_datum` but it's not explicitly used in the main `INSERT` statement within the provided snippet. A full review of the SQL might reveal its actual usage if it's implicitly used or passed to another context. The BigQuery design assumes it's for logging or conditional logic if used.
*   **Data Type Mismatches:** Although `hql_sql_to_bqsql_design` handles general type conversions, a detailed schema comparison between Oracle and BigQuery is required to catch any subtle data type or precision/scale mismatches.

## 8. Build Plan

The build plan will involve a staged approach, starting with schema migration, data ingestion, followed by code conversion and Airflow DAG development.

1.  **BigQuery Schema Migration:**
    *   Create BigQuery datasets corresponding to Oracle schemas (e.g., `isbert_schema`, `sof_schema`).
    *   Translate Oracle DDL for `sof$ta_cntrct_crs2`, `sof$ta_cntrct_crs3`, and `isbert_schema.dwtk_meldungen` into BigQuery DDL.
    *   Create the corresponding tables in BigQuery.
    *   **Language:** BigQuery DDL

2.  **Initial Data Ingestion:**
    *   Ingest historical data from Oracle `sof$ta_cntrct_crs2`, `sof$ta_cntrct_crs3`, and `isbert_schema.dwtk_meldungen` into their respective BigQuery tables. This can use tools like Oracle to BigQuery migration services, Datastream, or custom data transfer jobs.
    *   **Language:** Various, depending on ingestion tool.

3.  **BigQuery SQL Conversion:**
    *   Convert `d_ausd_v_ta_cntrct_crs3.sql` to `d_ausd_v_ta_cntrct_crs3_bq.sql` using the provided BigQuery SQL design.
    *   **Language:** BigQuery SQL

4.  **KornShell to Python Conversion:**
    *   Translate `r_ausd_v_ta_cntrct_crs3.ksh` and `k_ausd_v_ta_cntrct_crs3.ksh` into a single Python script (e.g., `contract_data_updater.py`).
    *   This script will handle parameter parsing, environment setup, and execute `d_ausd_v_ta_cntrct_crs3_bq.sql` using BigQuery client libraries or `bq` CLI commands.
    *   Implement equivalent logging and error handling using Cloud Logging.
    *   **Language:** Python

5.  **Airflow DAG Development:**
    *   Create an Airflow DAG file `dw_bert_ausd_v_ta_cntrct_crs3_dag.py`.
    *   The DAG will include a task (e.g., `PythonOperator` or `DataprocSubmitJobOperator`) to execute the `contract_data_updater.py` script.
    *   Define basic DAG properties (`dag_id`, `start_date`, `schedule`).
    *   Integrate monitoring and alerting.
    *   **Language:** Python (Airflow)

6.  **Review and Testing:**
    *   Conduct thorough unit, integration, and user acceptance testing of the BigQuery SQL and the Airflow DAG.
    *   Validate data correctness and performance against the legacy system.

7.  **Production Deployment:**
    *   Deploy the BigQuery schemas and data.
    *   Deploy the Python scripts to a suitable GCP location (e.g., GCS bucket for Dataproc).
    *   Deploy the Airflow DAG to Cloud Composer.