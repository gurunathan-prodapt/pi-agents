# Migration Design — ausd_bp_ta_apn_vertrag

## 1. Purpose & Scope
The job **`ausd_bp_ta_apn_vertrag`** is part of the `BERT Stammdaten` (product master data) business domain. Its primary purpose is to aggregate and pivot Access Point Names (APNs) and contract references per contract ID, preparing this cached contract information for credit scoring and fraud analysis (BERT/FOS). 

Specifically, the workflow reads from a local staging table, aggregates multiple records associated with a single contract ID into comma-separated strings (with character length constraints), and writes the summarized result into a target contract cache table.

- **Legacy Job:** `DW.BERT_AUSD_BP_TA_APN_VERTRAG` (UC4 Job Definition)
- **Target Platform:** Google Cloud BigQuery
- **Orchestration Target:** Google Cloud Composer (Apache Airflow)

---

## 2. Source Inventory
The legacy component inventory contains four files, mapped below with their complexity tiers and recommended migration strategies:

| Legacy File Path | Technology | Complexity Tier | Automation Bucket | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml` | UC4 XML | Medium | **B2** (Semi-Auto) | Orchestrates job execution, environment setup, and wrapper script invocation. |
| `r_ausd_bp_ta_apn_vertrag.ksh` | KornShell (ksh) | Medium | **B2** (Semi-Auto) | Framework script (Rahmenskript) establishing logging contexts, parsing parameters, setting the default `Stichtag` date, and executing traps. |
| `k_ausd_bp_ta_apn_vertrag.ksh` | KornShell (ksh) | Medium | **B2** (Semi-Auto) | Core control script (Kontrollskript) validating run-date parameters, logging metrics, and invoking the SQL*Plus wrapper. |
| `d_ausd_bp_ta_apn_vertrag.sql` | Oracle PL/SQL | Complex | **B0** (Retire / Migrate to BQ SQL) | Main execution logic. Deletes target table data and runs a cursor loop over staging table `sof$ta_bpr_apn` to build aggregated strings into `sof$ta_apn_vertrag`. |

---

## 3. Target Architecture
In BigQuery, shell scripts and SQL*Plus scripts will be replaced by native SQL operations orchestrated via Google Cloud Composer.

### Target Dataset Layout
- **Target Table:** `sof_ta_apn_vertrag` (replaces legacy Oracle special-character naming `sof$ta_apn_vertrag`)
- **Staging Source Table:** `sof_ta_bpr_apn` (replaces `sof$ta_bpr_apn`)

### Target Schema Definition
The table `sof_ta_apn_vertrag` will be created using the following BigQuery DDL:

```sql
CREATE OR REPLACE TABLE `your_project.your_dataset.sof_ta_apn_vertrag`
(
  cntrct_id STRING OPTIONS(description="Contract ID"),
  apn STRING OPTIONS(description="Aggregated list of Access Point Names, comma-separated"),
  cntrct_ref STRING OPTIONS(description="Aggregated list of contract references, comma-separated")
)
OPTIONS(
  description="Aggregated APN and contract references per contract ID for BERT credit scoring cache."
);
```

---

## 4. Data Flow & Lineage
The lineage and execution sequence of this specific migration block are as follows:

```
[Staging Source: sof_ta_bpr_apn] 
              │
              ▼
[BigQuery Transformation Query / UDF Aggregation]
              │
              ▼
[Target Table: sof_ta_apn_vertrag]
```

### Execution Flow in Airflow:
1. **DAG Start:** Sourced from standard time-triggers or orchestrated upstream completions.
2. **Execution Task:** Airflow runs the BigQuery operation to truncate and reload the target table using native SQL transformation logic.
3. **Auditing/Logging:** Output metrics are naturally logged into Cloud Logging, replacing legacy logging tables (`dwtk_meldungen`) and shell handlers.

---

## 5. Transformation Logic

### Legacy PL/SQL Analysis
The core transformation in `d_ausd_bp_ta_apn_vertrag.sql` utilizes an Oracle PL/SQL cursor loop.
- It iterates over `sof$ta_bpr_apn` sorted by `cntrct_id`.
- For each record, it concatenates `access_point_name` and `cntrct_id_ref` (delimited by `, `) for identical `cntrct_id`s.
- It enforces a strict length constraint of **100 characters** for both concatenated strings (`v_apn` and `v_cntrct_ref`). If the next concatenated element pushes the total length over 100, that element is skipped (but the loop continues, meaning subsequent shorter values might still be appended if they fit).
- Finally, it RTRIMs trailing delimiters and inserts the result into `sof$ta_apn_vertrag`.
- It also includes legacy SQL*Plus declarations (`v_datum` select from `dwtk_meldungen`) which are remnants of dead code and are not used in the processing loop.

### BigQuery Migration Options

To translate this cursor behavior, we present two options:

#### Option A: Simplified SQL (Standard Aggregation with String Truncation)
This standard SQL pattern uses `STRING_AGG` and then cuts off the final string at 100 characters. This is the simplest and highest-performing approach in BigQuery but may truncate an APN string mid-word at the 100th character.

```sql
CREATE OR REPLACE TABLE `your_project.your_dataset.sof_ta_apn_vertrag` AS
WITH source_data AS (
  SELECT
    cntrct_id,
    cntrct_id_ref,
    access_point_name
  FROM `your_project.your_dataset.sof_ta_bpr_apn`
),
aggregated AS (
  SELECT
    cntrct_id,
    SUBSTR(RTRIM(STRING_AGG(access_point_name, ', ' ORDER BY access_point_name), ', '), 1, 100) AS apn,
    SUBSTR(RTRIM(STRING_AGG(cntrct_id_ref, ', ' ORDER BY cntrct_id_ref), ', '), 1, 100) AS cntrct_ref
  FROM source_data
  GROUP BY cntrct_id
)
SELECT
  cntrct_id,
  apn,
  cntrct_ref
FROM aggregated;
```

#### Option B: Strictly Equivalent SQL (Using JS UDF to fit complete elements)
To guarantee 100% functional parity with the PL/SQL cursor's character-fitting behavior (excluding entire elements that do not fit into the 100-character limit, rather than cutting them in half), we utilize a Temporary Javascript UDF:

```sql
CREATE TEMP FUNCTION aggregate_limited(arr ARRAY<STRING>, delimiter STRING, max_len INT64)
RETURNS STRING
LANGUAGE js AS """
  if (!arr) return null;
  let result = "";
  for (let i = 0; i < arr.length; i++) {
    let item = arr[i];
    if (!item) continue;
    let next_str = result ? result + delimiter + item : item;
    if (next_str.length <= max_len) {
      result = next_str;
    } else {
      continue; // Skip the elements that overflow, keeping the loop active
    }
  }
  return result;
""";

CREATE OR REPLACE TABLE `your_project.your_dataset.sof_ta_apn_vertrag` AS
SELECT
  cntrct_id,
  aggregate_limited(ARRAY_AGG(access_point_name ORDER BY access_point_name), ', ', 100) AS apn,
  aggregate_limited(ARRAY_AGG(cntrct_id_ref ORDER BY cntrct_id_ref), ', ', 100) AS cntrct_ref
FROM
  `your_project.your_dataset.sof_ta_bpr_apn`
GROUP BY
  cntrct_id;
```

---

## 6. External Dependencies
The legacy job relies on several environment and platform dependencies:
- **Oracle DB Links (`PCRS1`, `TDG`):** Not used in this specific step. The script only reads from staging table `sof$ta_bpr_apn` and writes to local table `sof$ta_apn_vertrag`.
- **Audit Table (`isbert_schema.dwtk_meldungen`):** Sourced at the start of the PL/SQL script to find `v_datum` for old partition dropping. This is dead code and is not needed in the BigQuery destination.
- **Audit Logging Utilities (`DWMSG_ErmittleNr`, `DWMSG_MeldeFehler`):** Handled natively by Apache Airflow run statistics and Google Cloud Logging alerts, removing the need to manage custom tables and shell utility libraries.

---

## 7. Unresolved / Risks

### String Length Limit Constraints
The legacy 100-character column limitation was dictated by standard relational warehouse space restrictions from 2001. In BigQuery, string fields can hold up to 10MB of data with no performance degradation.
- **Risk:** Truncating or omitting APN records at 100 characters might hide fraud/credit signals from downstream scoring models that require a complete listing.
- **Recommendation:** Align with business architects to lift the 100-character limit. If agreed, Option A can be deployed without the `SUBSTR` constraint, resulting in a cleaner, complete representation of all active APNs per contract.

### Dead Code Remnants
- Sourcing `v_datum` from `dwtk_meldungen` and defining `@pcrs1` (`v_carmen`) are safely retired as they represent deprecated staging strategies.

---

## 8. Build Plan

Deploy the migration objects in the following sequence:

1. **Deploy BigQuery Target Table:**
   Run the DDL schema script in Section 3 to establish `sof_ta_apn_vertrag`.
2. **Deploy BigQuery SQL Script:**
   Establish the aggregation query (using either Option A or Option B, pending limit decision).
3. **Deploy Composer (Airflow) DAG:**
   Deploy the Python DAG script `dw_bert_ausd_bp_ta_apn_vertrag.py` to your GCS DAGs bucket to trigger and monitor the BigQuery aggregation logic.

### Airflow DAG Implementation Code

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

default_args = {
    "owner": "data-migration-team",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_apn_vertrag",
    default_args=default_args,
    description="Orchestrations for BERT APN and Contract Reference aggregation",
    schedule_interval=None, # Triggered after staging loads are complete
    catchup=False,
    tags=["bert", "warehouse", "credit-scoring"],
) as dag:

    # Execute the BigQuery transformation using Option B (strict functional equivalence)
    aggregate_apn_data = BigQueryExecuteQueryOperator(
        task_id="aggregate_apn_contract_refs",
        sql="""
            CREATE TEMP FUNCTION aggregate_limited(arr ARRAY<STRING>, delimiter STRING, max_len INT64)
            RETURNS STRING
            LANGUAGE js AS r\"\"\"
              if (!arr) return null;
              let result = "";
              for (let i = 0; i < arr.length; i++) {
                let item = arr[i];
                if (!item) continue;
                let next_str = result ? result + delimiter + item : item;
                if (next_str.length <= max_len) {
                  result = next_str;
                } else {
                  continue;
                }
              }
              return result;
            \"\"\";

            CREATE OR REPLACE TABLE `your_project.your_dataset.sof_ta_apn_vertrag` AS
            SELECT
              cntrct_id,
              aggregate_limited(ARRAY_AGG(access_point_name ORDER BY access_point_name), ', ', 100) AS apn,
              aggregate_limited(ARRAY_AGG(cntrct_id_ref ORDER BY cntrct_id_ref), ', ', 100) AS cntrct_ref
            FROM
              `your_project.your_dataset.sof_ta_bpr_apn`
            GROUP BY
              cntrct_id;
        """,
        use_legacy_sql=False,
        location="EU" # Change to target GCS/BQ region
    )

    aggregate_apn_data
```