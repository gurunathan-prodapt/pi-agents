# Migration Design — DW.BERT_AUSD_V_TA_P_DISCOUNT_RR

## 1. Purpose & Scope
This job, `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR`, is an ETL workflow designed to enrich discount data by adding contract number and standard contract template information. It truncates a target table (`sof$ta_p_discount_rr`) and then populates it by joining data from several source tables, specifically `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, and `sof$ta_cntrct_templ`. The job is orchestrated by a UC4 Unix job definition, which in turn invokes a series of KornShell wrapper scripts that ultimately execute an Oracle SQL*Plus script containing the core transformation logic. The business purpose is to prepare and consolidate contract-related data for reporting or further downstream processing.

## 2. Source Inventory

The job consists of the following components:

| File Path                                                                                                   | Technology           | Category | Tool            | Tier     | Automation Bucket | Summary                                                                                                                                                                                                                                                                                                                                 |
| :---------------------------------------------------------------------------------------------------------- | :------------------- | :------- | :-------------- | :------- | :---------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml` | UC4 XML              | `uc4`    | `UC4/Automic`   | `medium` | `semi_auto`       | UC4 UNIX job definition that executes a KornShell script to add contract number and contract template to discounts.                                                                                                                                                                                                                         |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh`                     | KornShell Script     | `shell`  | `KornShell`     | `medium` | `semi_auto`       | This KornShell script acts as a wrapper for the 'k_ausd_v_ta_p_discount_rr.ksh' core script, handling environment setup, parameter parsing, error trapping, and logging for the 'ta_p_discount_rr' data reconciliation process.                                                                                                                              |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh`                     | KornShell Script     | `shell`  | `KornShell`     | `medium` | `semi_auto`       | This is a control script for `r_ausd_vertrag.ksh` that handles parameter parsing, environment setup, error handling, and orchestrates the execution of an SQL script to process data for `ta_p_discount_rr`.                                                                                                                                               |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_p_discount_rr.sql`                     | Oracle SQL*Plus Script | `sql`    | `Oracle SQL*Plus` | `medium` | `semi_auto`       | This SQL*Plus script truncates a target table and then populates it by joining data from multiple source tables, enriching discount information with contract details.                                                                                                                                                                                    |

## 3. Target Architecture

The target architecture will leverage Google Cloud Platform (GCP) services:
-   **Orchestration:** Airflow DAG running on Cloud Composer.
-   **Wrapper Logic / Environment Setup:** Re-implemented in PySpark, executed on Dataproc.
-   **Core Transformation Logic:** Migrated to BigQuery SQL, executed as part of the PySpark job or directly within the Airflow DAG if suitable for a `BigQueryOperator`.
-   **Data Storage:** All source and target tables will reside in BigQuery.

### Target BigQuery Components & Layout

-   **Target Table:** `sof$ta_p_discount_rr` will be migrated to a BigQuery table, likely within a dedicated dataset (e.g., `dw.ta_p_discount_rr`).
-   **Source Tables:** `isbert_schema.dwtk_meldungen`, `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, and `sof$ta_cntrct_templ` will be ingested into BigQuery. Their schema will be mapped to BigQuery-compatible types. We will assume a `dw` dataset for these tables.

## 4. Data Flow & Lineage

The legacy execution flow is as follows:
1.  **UC4 Job `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml`** is triggered.
2.  It invokes the KornShell wrapper script: **`r_ausd_v_ta_p_discount_rr.ksh`**.
3.  `r_ausd_v_ta_p_discount_rr.ksh` sets up the environment and calls the core KornShell script: **`k_ausd_v_ta_p_discount_rr.ksh`**.
4.  `k_ausd_v_ta_p_discount_rr.ksh` further prepares parameters and executes the Oracle SQL*Plus script: **`d_ausd_v_ta_p_discount_rr.sql`**.
5.  `d_ausd_v_ta_p_discount_rr.sql` connects to an Oracle database, determines a date from `isbert_schema.dwtk_meldungen`, truncates `sof$ta_p_discount_rr`, and then inserts data into `sof$ta_p_discount_rr` from `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, and `sof$ta_cntrct_templ`.

In the target BigQuery environment, this flow will be re-engineered:

**Target Data Flow (Airflow DAG):**
-   A single Airflow DAG (`dw_bert_ausd_v_ta_p_discount_rr`) will represent the job.
-   This DAG will contain one main task, likely a `DataprocSubmitJobOperator` (if PySpark is chosen for the wrapper logic) or a `BigQueryOperator` (if the wrapper logic is simple enough to be directly embedded or pre-processed).

`start_task` (DummyOperator)
  `-> run_transformation_task` (e.g., `DataprocSubmitJobOperator` or `BigQueryOperator`)
    `-> end_task` (DummyOperator)

## 5. Transformation Logic

The core transformation logic is contained within `d_ausd_v_ta_p_discount_rr.sql`.

**Source Logic Summary:**
1.  **Date Derivation:** Retrieves the maximum `timecreated` from `isbert_schema.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`, formatted as `YYYYMMDD`. Defaults to `19000101` if no record is found.
2.  **Target Table Preparation:** Truncates the table `sof$ta_p_discount_rr` using `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`.
3.  **Data Insertion:** Inserts data into `sof$ta_p_discount_rr` by performing inner joins on `sof$ta_discount_rr` (aliased as `da`), `sof$ta_cntrct_crs` (aliased as `c`), and `sof$ta_cntrct_templ` (aliased as `ct`).
    -   **Join Conditions:**
        -   `da.cntrct_id = c.cntrct_id`
        -   `da.cntrct_obj_version = c.obj_version`
        -   `da.cntrct_template_id = ct.cntrct_template_id`
    -   **Selected Columns for Insertion:**
        -   `da.cntrct_id`
        -   `da.discount_id`
        -   `da.disc_vector_ty`
        -   `da.cntrct_obj_version`
        -   `da.cntrct_template_id`
        -   `da.disc_invoice_item_id`
        -   `da.rabatt`
        -   `da.rabatthoehe`
        -   `da.rabattierte_rech_pos`
        -   `c.contract_number`
        -   `ct.cds_description` (aliased as `std_vertrag`)

**Target BigQuery SQL Equivalent:**

```sql
DECLARE v_datum STRING;

SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `dw.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

TRUNCATE TABLE `dw.ta_p_discount_rr`;

INSERT INTO `dw.ta_p_discount_rr` (
  cntrct_id,
  discount_id,
  disc_vector_ty,
  cntrct_obj_version,
  cntrct_template_id,
  disc_invoice_item_id,
  rabatt,
  rabatthoehe,
  rabattierte_rech_pos,
  contract_number,
  std_vertrag
)
SELECT
  da.cntrct_id,
  da.discount_id,
  da.disc_vector_ty,
  da.cntrct_obj_version,
  da.cntrct_template_id,
  da.disc_invoice_item_id,
  da.rabatt,
  da.rabatthoehe,
  da.rabattierte_rech_pos,
  c.contract_number,
  ct.cds_description AS std_vertrag
FROM `dw.ta_discount_rr` da
JOIN `dw.ta_cntrct_crs` c
  ON da.cntrct_id = c.cntrct_id
 AND da.cntrct_obj_version = c.obj_version
JOIN `dw.ta_cntrct_templ` ct
  ON da.cntrct_template_id = ct.cntrct_template_id;
```

**KornShell Wrapper Logic Migration:**
The logic within `r_ausd_v_ta_p_discount_rr.ksh` and `k_ausd_v_ta_p_discount_rr.ksh` for environment setup, parameter parsing, error trapping, and logging will be reimplemented in PySpark. This PySpark script will then execute the BigQuery SQL transformation. This approach allows for centralized logging, error handling, and environment management within the Dataproc/PySpark ecosystem, aligning with GCP best practices for ETL.

## 6. External Dependencies

-   **Oracle Database:** The original job relies on an Oracle database for all its tables (`isbert_schema.dwtk_meldungen`, `sof$ta_p_discount_rr`, `sof$ta_discount_rr`, `sof$ta_cntrct_crs`, `sof$ta_cntrct_templ`).
    -   **Replacement:** All Oracle tables will be migrated to BigQuery. The schema and data will be ingested into BigQuery datasets (e.g., `dw`).
-   **UNIX Host:** The UC4 job runs on a `DWHDWH1P` UNIX host using login `DW.UNIX.ISBERT`.
    -   **Replacement:** The execution environment will shift to Cloud Composer for Airflow and Dataproc for PySpark processing. Authentication will use GCP service accounts.

## 7. Unresolved / Risks

-   **No Schedule:** The UC4 object did not provide scheduling information (`EVNT_TIME` file). The Airflow DAG will initially be created without a defined schedule, which needs to be determined based on business requirements.
-   **No Workflow Context:** No `JOBP` or `JSCH` files were provided, so the full workflow context and dependencies within UC4 are unknown. This design assumes `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` is a standalone job. If it is part of a larger UC4 workflow, further analysis will be needed to integrate it correctly into a new Airflow workflow.
-   **KornShell Utilities:** The KornShell scripts source several utility scripts (`. $HOME/.dw_init`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`, `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`). The functionalities of these utilities (e.g., custom error handling, date formatting, SQL*Plus invocation) will need to be re-implemented or replaced with BigQuery/PySpark equivalents.
-   **`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`:** The Oracle SQL script uses a stored procedure call `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_p_discount_rr');`. This custom PL/SQL procedure will need to be replaced with a direct `TRUNCATE TABLE` statement in BigQuery SQL, as shown in the migration.
-   **Performance Optimization:** The Oracle SQL script uses `/*+ parallel(da,4) parallel(c,4) parallel(ct,4) */` hints. While BigQuery automatically handles parallelism, monitoring and potential optimization of the BigQuery SQL will be necessary after migration to ensure equivalent or better performance.
-   **`trace.sql.cfg` and `SPOOL`:** The `START ../trace.sql.cfg` and `SPOOL ./tmp/trace_d_ausd_v_ta_p_discount_rr` commands in the Oracle SQL script are specific to SQL*Plus. Tracing and logging in BigQuery will be handled by BigQuery's built-in logging and Cloud Logging.

## 8. Build Plan

The migration will involve generating the following artifacts:

1.  **Airflow DAG (`dw_bert_ausd_v_ta_p_discount_rr.py`)** - Python
    -   Orchestrates the PySpark job (or BigQueryOperator).
    -   Handles scheduling (once determined), retries, and task dependencies.
    -   Based on the `uc4_to_airflow_dag_design` output.

2.  **PySpark Wrapper Script (`r_ausd_v_ta_p_discount_rr.py`)** - Python (PySpark)
    -   Replaces the logic of `r_ausd_v_ta_p_discount_rr.ksh` and `k_ausd_v_ta_p_discount_rr.ksh`.
    -   Manages environment variables (e.g., `BERT_DIR_ROOT`), parses parameters, implements error handling, and executes the BigQuery SQL transformation.
    -   This script will be submitted to Dataproc by the Airflow DAG.

3.  **BigQuery SQL Transformation (`d_ausd_v_ta_p_discount_rr.sql.bq`)** - BigQuery SQL
    -   Contains the translated SQL logic from `d_ausd_v_ta_p_discount_rr.sql`.
    -   Will be executed by the PySpark script, or directly by a `BigQueryOperator` in the Airflow DAG if the wrapper logic is trivial.

4.  **BigQuery Table DDLs** - BigQuery SQL
    -   DDLs for `dw.ta_p_discount_rr` (target table).
    -   DDLs for `dw.dwtk_meldungen`, `dw.ta_discount_rr`, `dw.ta_cntrct_crs`, `dw.ta_cntrct_templ` (source tables, assuming they are part of a broader data ingestion plan).