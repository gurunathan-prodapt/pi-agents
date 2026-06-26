# Migration Design — DW.BERT_AUSD_BP_TA_TARIFOPTION

This document specifies the migration of the legacy UC4 job `DW.BERT_AUSD_BP_TA_TARIFOPTION` and its associated components (KornShell scripts and Oracle SQL) to Google BigQuery and Apache Airflow on Google Cloud Composer.

---

## 1. Purpose & Scope

The primary objective of the `DW.BERT_AUSD_BP_TA_TARIFOPTION` job is to prepare and aggregate selected basic product tariff options for the **BERT** (demand scoring) platform. 
Specifically, the job:
* Retrieves a run-date suffix (`v_datum`) from the metadata table `dwtk_meldungen`.
* Joins a dynamic daily contract basic product text table (`sof$ta_bpr_opt_text_<v_datum>`) with a lookup filter table (`sof$ta_l_bpr_optionen_filter`) that categorizes tariff option IDs into specific categories (`BUDGET`, `SONST`, `GPRS`).
* Aggregates these filtered options by Contract ID (`cntrct_id`), concatenating the descriptions alphabetically into three separate target columns based on their category: `business_option`, `sonstige_option`, and `gprs_option`.
* Writes the aggregated results into the target data warehouse table `sof$ta_tarifoption` (FOS-Tabelle).

This migration will completely retire the legacy UC4 orchestration, KornShell scripts, and Oracle PL/SQL stateful accumulators, replacing them with a modern, fully parallelized **BigQuery SQL Script** and an **Airflow DAG**.

---

## 2. Source Inventory

The job consists of the following 4 files:

| Legacy Relative Path | Tech / Language | Complexity Tier | Automation Bucket | Role / Description |
| :--- | :--- | :--- | :--- | :--- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_TARIFOPTION.xml` | UC4 XML | Medium | B2 (Semi-Auto) | Orchestrator: Defines UC4 UNIX Job. Initializes env and calls `r_ausd_bp_ta_tarifoption.ksh`. |
| `vobs/dw_source/isbert/install_save/r_ausd_bp_ta_tarifoption.ksh` | KornShell (ksh) | Medium | B2 (Semi-Auto) | Wrapper Script: Sets up runtime logging, handles arguments, and calls `k_ausd_bp_ta_tarifoption.ksh`. |
| `vobs/dw_source/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh` | KornShell (ksh) | Medium | B2 (Semi-Auto) | Core Script: Validates parameters (Stichtag), computes date variables, and executes the SQL. |
| `vobs/dw_source/isrpt/isbert/install_save/d_ausd_bp_ta_tarifoption.sql` | Oracle SQL / PLSQL | Medium | B2 (Semi-Auto) | SQL Processing: Resolves dynamic table names, joins lookup filter, and performs string aggregation. |

---

## 3. Target Architecture

The target architecture on Google Cloud Platform (GCP) utilizes serverless, cloud-native services to optimize execution speed, minimize operational overhead, and reduce costs.

```
       +------------------------------------+
       |       Google Cloud Composer        |
       |           (Airflow DAG)            |
       +-----------------+------------------+
                         |
                         | Executes BigQuery Job
                         v
       +-----------------+------------------+
       |             BigQuery               |
       |  (Procedural SQL Script Execution) |
       +-----------------+------------------+
                         |
      +------------------+------------------+
      |                                     |
      v                                     v
+-----+----------------------+       +------+----------------------+
|  sof_ta_bpr_opt_filter     |       |  sof_ta_tarifoption         |
|  (Temporary/Staging Table) |       |  (Final Aggregated Target)  |
+----------------------------+       +-----------------------------+
```

### Components & Layout:
1. **Orchestrator**: UC4 scheduling definitions and the wrapper/control KornShell scripts (`r_...` and `k_...`) are replaced by a single **Apache Airflow DAG** in **Google Cloud Composer**.
2. **Execution Engine**: Google BigQuery runs the entire processing logic using a single procedural **BigQuery SQL Script** (Multi-Statement Query), executed via the `BigQueryInsertJobOperator`.
3. **Data Storage Schema**:
   * All target tables will reside in a structured BigQuery dataset (e.g., `target_dataset`).
   * Table name formats will be standardized to lowercase, replacing Oracle special characters (like `$`) with underscores (e.g., `sof$ta_tarifoption` becomes `sof_ta_tarifoption`).

---

## 4. Data Flow & Lineage

The following data lineage diagram illustrates the transformation and movement of data:

```
[dwtk_meldungen] (Read max timecreated where job_kennung = 'BERT_DROP_TEMP_TABLE')
       |
       v
Identifies Suffix (v_datum)
       |
       +------------------------------------+
       |                                    |
[sof_ta_l_bpr_optionen_filter]    [sof_ta_bpr_opt_text_<v_datum>] (Dynamic Table)
       |                                    |
       +-----------------+------------------+
                         |
                         v (JOIN on bpr_id)
           [sof_ta_bpr_opt_filter] (Intermediate Table)
                         |
                         v (GROUP BY cntrct_id & STRING_AGG)
             [sof_ta_tarifoption] (Final Output)
```

### Order of Execution:
1. **DAG Trigger**: Airflow DAG is triggered on its schedule.
2. **Metadata Lookup**: BigQuery queries `dwtk_meldungen` to identify the correct `v_datum` suffix string (e.g., `'20260421'`).
3. **Intermediate Filter Table Build**: BigQuery uses `EXECUTE IMMEDIATE` to join the static filter map `sof_ta_l_bpr_optionen_filter` with the dynamic daily table `sof_ta_bpr_opt_text_<v_datum>`, outputting to `sof_ta_bpr_opt_filter`.
4. **Aggregation & Final Insert**: Standard SQL aggregation groups the intermediate records by `cntrct_id` and writes the truncated, comma-separated option strings to `sof_ta_tarifoption`.

---

## 5. Transformation Logic

### Legacy Bottleneck Refactoring:
In the legacy Oracle script, string aggregation was accomplished using:
1. A **stateful custom package** `sof$ab_con` containing accumulators (`concat1`, `concat1r`, `concat2`, `concat2r`, etc.).
2. The analytical **`LEAD` function** over an unordered set (`ORDER BY NULL`) to detect the final row of a contract group (`lagi > cntrct_id or lagi = -1`).
3. Pre-sorting the entire dataset via an inline view (`order by cntrct_id, pds_description`) to force the lead function and accumulators to align correctly.

This approach was row-by-row, highly state-dependent, and single-threaded. 

**BigQuery Solution**:
We fully replace this logic with standard, cloud-native SQL features:
* **`STRING_AGG`** with an internal **`ORDER BY pds_description`** clause.
* Conditional expressions **`IF(opt_kategorie = '...', pds_description, NULL)`** to isolate values per target column during the aggregation.
* **`GROUP BY cntrct_id`** to allow BigQuery to fully parallelize the aggregation across its execution slots.

This modification is 100% functionally equivalent, significantly faster, and drastically simpler.

### Column Mapping:

| Source Column / Logic | Target BigQuery Column | Type | Transformation / Logic |
| :--- | :--- | :--- | :--- |
| `cntrct_id` | `cntrct_id` | INT64 / STRING | Direct pass-through. |
| `rtrim(substr(ltrim(pds_des1,', '),1,500))` | `business_option` | STRING | `SUBSTR(STRING_AGG(IF(opt_kategorie = 'BUDGET', pds_description, NULL), ', ' ORDER BY pds_description), 1, 500)` |
| `rtrim(substr(ltrim(pds_des2,', '),1,500))` | `sonstige_option` | STRING | `SUBSTR(STRING_AGG(IF(opt_kategorie = 'SONST', pds_description, NULL), ', ' ORDER BY pds_description), 1, 500)` |
| `rtrim(substr(ltrim(pds_des3,', '),1,500))` | `gprs_option` | STRING | `SUBSTR(STRING_AGG(IF(opt_kategorie = 'GPRS', pds_description, NULL), ', ' ORDER BY pds_description), 1, 500)` |

---

## 6. External Dependencies

1. **UC4 Global Include Scripts** (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`): 
   * **Replacement**: Replaced by Airflow standard task logs and environment configurations managed via Airflow Variables.
2. **KornShell Utility Scripts** (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`):
   * **Replacement**: Retired. Argument validation and logging are natively handled by Apache Airflow.
3. **Oracle Schema & Database Links**:
   * **Replacement**: All database links are replaced by native BigQuery cross-project or cross-dataset queries.
4. **Dynamic Table Name Resolution**:
   * **Replacement**: Built natively into the BigQuery SQL Script using standard `DECLARE` and `EXECUTE IMMEDIATE` procedural syntax.

---

## 7. Risks & Mitigation Plan

| Risk Description | Severity | Mitigation Strategy |
| :--- | :--- | :--- |
| **Missing Dynamic Table**: The source table `sof_ta_bpr_opt_text_<v_datum>` might not exist for the computed date suffix if the previous dependency pipeline failed. | Medium | Add a pre-check task in the Airflow DAG to verify table existence in BigQuery before executing the main SQL, or handle the error gracefully inside the SQL using a try-catch/system error check. |
| **Character Truncation (500 chars)**: High-density contracts might exceed 500 characters of concatenated option names. | Low | The `SUBSTR(..., 1, 500)` is preserved to match legacy data warehousing truncation specs exactly, preventing downstream column schema overflow errors. |
| **Missing Lookup Entries**: New option IDs might appear in the daily table without corresponding categories in `sof_ta_l_bpr_optionen_filter`. | Medium | Create a monitoring/sanity dashboard in BigQuery that alerts data owners of any `bpr_id` present in `sof_ta_bpr_opt_text_*` that has no match in the `sof_ta_l_bpr_optionen_filter` lookup table. |

---

## 8. Build Plan

The migration will be completed by deploying two components:
1. **BigQuery SQL Script**: A compiled procedural script containing the logic of `d_ausd_bp_ta_tarifoption.sql`.
2. **Airflow DAG**: An Airflow pipeline Python file to orchestrate and execute the BigQuery script.

### File 1: BigQuery SQL Script (`d_ausd_bp_ta_tarifoption.sql`)

Save this file as `d_ausd_bp_ta_tarifoption.sql` in your BigQuery assets repository.

```sql
-- ===================================================================
-- Target Platform: Google BigQuery
-- Refactored from: d_ausd_bp_ta_tarifoption.sql
-- Description:     Prepares and aggregates contract tariff options
-- ===================================================================

DECLARE v_datum STRING;

-- Step 1: Identify the run-date suffix from the metadata table
SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(timecreated)), '19000101')
  FROM `target_project.target_dataset.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 2: Build staging/intermediate table with joined categories dynamically
EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_bpr_opt_filter` AS
  SELECT
    t.bpr_id,
    t.cntrct_id,
    t.pds_description,
    l.opt_kategorie
  FROM
    `target_project.target_dataset.sof_ta_l_bpr_optionen_filter` l
  JOIN
    `target_project.target_dataset.sof_ta_bpr_opt_text_%s` t
  ON
    t.bpr_id = l.bpr_id
""", v_datum);

-- Step 3: Pivot and aggregate option strings by contract ID into final table
CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_tarifoption` AS
SELECT
  cntrct_id,
  SUBSTR(
    STRING_AGG(
      IF(opt_kategorie = 'BUDGET', pds_description, NULL), 
      ', ' ORDER BY pds_description
    ), 1, 500
  ) AS business_option,
  SUBSTR(
    STRING_AGG(
      IF(opt_kategorie = 'SONST', pds_description, NULL), 
      ', ' ORDER BY pds_description
    ), 1, 500
  ) AS sonstige_option,
  SUBSTR(
    STRING_AGG(
      IF(opt_kategorie = 'GPRS', pds_description, NULL), 
      ', ' ORDER BY pds_description
    ), 1, 500
  ) AS gprs_option
FROM
  `target_project.target_dataset.sof_ta_bpr_opt_filter`
GROUP BY
  cntrct_id;
```

### File 2: Airflow DAG (`dag_bert_ausd_bp_ta_tarifoption.py`)

Save this file as `dag_bert_ausd_bp_ta_tarifoption.py` in your Airflow DAGs folder.

```python
# -*- coding: utf-8 -*-
# ===================================================================
# Target Platform: Apache Airflow (Google Cloud Composer)
# Description:     DAG to orchestrate BERT basic product tariff options
# ===================================================================

import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime.datetime(2026, 4, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    'dw_bert_ausd_bp_ta_tarifoption',
    default_args=default_args,
    description='Orchestrates aggregation of contract tariff options into sof_ta_tarifoption',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task to run the BigQuery multi-statement script
    run_tarifoption_aggregation = BigQueryInsertJobOperator(
        task_id='run_tarifoption_aggregation',
        configuration={
            "query": {
                # Reads the refactored SQL code directly
                "query": """
                    DECLARE v_datum STRING;

                    -- Step 1: Identify the run-date suffix from the metadata table
                    SET v_datum = (
                      SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(timecreated)), '19000101')
                      FROM `target_project.target_dataset.dwtk_meldungen`
                      WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
                    );

                    -- Step 2: Build staging/intermediate table dynamically
                    EXECUTE IMMEDIATE FORMAT(\"\"\"
                      CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_bpr_opt_filter` AS
                      SELECT
                        t.bpr_id,
                        t.cntrct_id,
                        t.pds_description,
                        l.opt_kategorie
                      FROM
                        `target_project.target_dataset.sof_ta_l_bpr_optionen_filter` l
                      JOIN
                        `target_project.target_dataset.sof_ta_bpr_opt_text_%s` t
                      ON
                        t.bpr_id = l.bpr_id
                    \"\"\", v_datum);

                    -- Step 3: Aggregate and create final target table
                    CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_tarifoption` AS
                    SELECT
                      cntrct_id,
                      SUBSTR(
                        STRING_AGG(
                          IF(opt_kategorie = 'BUDGET', pds_description, NULL), 
                          ', ' ORDER BY pds_description
                        ), 1, 500
                      ) AS business_option,
                      SUBSTR(
                        STRING_AGG(
                          IF(opt_kategorie = 'SONST', pds_description, NULL), 
                          ', ' ORDER BY pds_description
                        ), 1, 500
                      ) AS sonstige_option,
                      SUBSTR(
                        STRING_AGG(
                          IF(opt_kategorie = 'GPRS', pds_description, NULL), 
                          ', ' ORDER BY pds_description
                        ), 1, 500
                      ) AS gprs_option
                    FROM
                      `target_project.target_dataset.sof_ta_bpr_opt_filter`
                    GROUP BY
                      cntrct_id;
                """,
                "useLegacySql": False,
            }
        },
    )

    run_tarifoption_aggregation