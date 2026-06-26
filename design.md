# Migration Design — BERT_V_TA_DISC_ZUSGF

This document outlines the migration design for the legacy UC4 job `BERT_V_TA_DISC_ZUSGF` (and its associated UNIX shell wrapper scripts and Oracle PL/SQL package logic) to Google Cloud Platform (GCP) with **BigQuery** as the data warehouse and **Google Cloud Composer (Apache Airflow)** as the orchestrator.

---

## 1. Purpose & Scope

The primary purpose of the `BERT_V_TA_DISC_ZUSGF` job is to **concatenate and aggregate contract discount descriptions** and populate a summary table. 

In the legacy Oracle system:
- Discount details are stored line-by-line in the table `SOF$TA_DISCOUNT`.
- A contract can have multiple active discounts (e.g., `Rabatt_A` at `10%` and `Rabatt_B` at `5%`).
- This job groups these discounts by contract (`cntrct_id`, `cntrct_obj_version`) and aggregates them into a comma-separated string, formatted as `Discount_Name (Discount_Percent%)` (e.g., `Rabatt_A (10%), Rabatt_B (5%)`), up to a cumulative length limit of 500 characters.
- The aggregated results are written to the target table `SOF$TA_DISC_ZUSGF` which is subsequently read by downstream contract and reporting processing jobs (such as `d_ausd_v_ta_p_discount.sql` and `d_ausd_v_ta_p_vertrag.sql`).

### Migration Scope
The migration covers the following legacy components:
1. **UC4 Job Plan / Scheduling Metadata**: `DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml`
2. **KornShell Wrapper Script**: `r_ausd_v_ta_disc_zusgf.ksh`
3. **KornShell Control Script**: `k_ausd_v_ta_disc_zusgf.ksh`
4. **Oracle SQL & PL/SQL Script**: `d_ausd_v_ta_disc_zusgf.sql` (including a custom pipelined table function and custom object types).

---

## 2. Source Inventory

The following table summarizes the source components identified for this job:

| File Path | Tech Category | Tool / Language | Complexity Tier | Automation Bucket | Summary / Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml` | UC4 XML | UC4 Job Definition | Medium | **B2 (Semi-Auto)** | UC4 job definition. Triggers the execution of `r_ausd_v_ta_disc_zusgf.ksh` with specific environment variables and logging inclusions. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh` | Shell Script | KornShell (`ksh`) | Medium | **B0 (Retire)** | Wrapper script. Sets up logging, handles error traps, obtains a run execution ID, and calls `k_ausd_v_ta_disc_zusgf.ksh`. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh` | Shell Script | KornShell (`ksh`) | Medium | **B0 (Retire)** | Control script. Validates execution parameters, sets SQLPlus settings, executes `d_ausd_v_ta_disc_zusgf.sql`, and outputs execution status. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql` | SQL / PL-SQL | Oracle PL/SQL | Complex | **B3 (Manual)** | Core ETL. Drops/creates object types and a pipelined PL/SQL table function `concat_discounts` to perform sorted, length-limited string aggregation, truncates `SOF$TA_DISC_ZUSGF`, and inserts the joined, aggregated records. |

### Migration Strategy Note: "Retire" Path for Shell Wrappers
The two shell scripts (`r_ausd_v_ta_disc_zusgf.ksh` and `k_ausd_v_ta_disc_zusgf.ksh`) serve *only* as orchestration and logging boilerplate around a single Oracle SQL execution. Running shell environments inside GCP to execute SQL via command-line utilities is anti-pattern and introduces unnecessary maintenance overhead.
**Design Choice**: Retire both shell scripts. Replace the entire flow with a native **BigQuery Operator** inside an **Airflow DAG**, moving the orchestration logic directly into Google Cloud Composer.

---

## 3. Target Architecture

The target architecture utilizes GCP native services to optimize cost, reliability, and maintainability:

```
[ Google Cloud Composer (Airflow) ]
              │
              ▼ (Triggers BigQuery Job)
    [ BigQuery SQL Query ]
              │
 ┌────────────┴────────────┐
 │  Reads From:            │ ──► [ isbert_schema.sof_ta_discount ]
 │  Writes To (Overwrite): │ ──► [ isbert_schema.sof_ta_disc_zusgf ]
 └─────────────────────────┘
```

### Components
1. **Google Cloud Composer (Airflow)**:
   - Replaces the UC4 scheduler.
   - Executes a daily/monthly DAG where `BERT_V_TA_DISC_ZUSGF` is a single task.
   - Uses `BigQueryInsertJobOperator` to run the aggregation query.
2. **BigQuery**:
   - Replaces Oracle 10g/11g.
   - **Source Table**: `isbert_schema.sof_ta_discount`
   - **Target Table**: `isbert_schema.sof_ta_disc_zusgf`
   - Data parallelism and performance optimization are handled natively by BigQuery’s execution engine (no more custom `PARALLEL(4)` hints or manual index maintenance).

---

## 4. Data Flow & Lineage

The end-to-end data lineage and execution order for this job is shown below:

```
[ SOF$TA_DISCOUNT ] (Source Table)
         │
         ▼
[ d_ausd_v_ta_disc_zusgf.sql ]  <── (Core Aggregation Logic via Temp JS UDF)
         │
         ▼ (Truncate & Load)
[ SOF$TA_DISC_ZUSGF ] (Target Table)
         │
         ├──► [ d_ausd_v_ta_p_discount.sql ] (Downstream Reader)
         └──► [ d_ausd_v_ta_p_vertrag.sql ]  (Downstream Reader)
```

### Execution Order in Composer DAG
1. Wait for upstream job `DW.BERT_AUSD_V_TA_DISCOUNT` to successfully complete (which populates the source table `isbert_schema.sof_ta_discount`).
2. Run task `bert_v_ta_disc_zusgf` (the migrated BigQuery SQL query).
3. Trigger downstream tasks:
   - `bert_ausd_v_ta_p_discount`
   - `bert_ausd_v_ta_p_vertrag`

---

## 5. Transformation Logic

### Legacy Logic Analysis
In Oracle, string aggregation is performed using a PL/SQL **pipelined table function** named `sof$sp_discount_functions.concat_discounts`.
- It processes a cursor of sorted discounts: `ORDER BY cntrct_id, cntrct_obj_version, rabatt_alle`.
- For each contract and version, it aggregates up to `500` characters, separating values with `', '`.
- Crucially, it does **not** perform hard truncation mid-word. If adding the next sorted discount pushes the length above `500` characters, it skips that discount and terminates appending.

### BigQuery Translation
We can implement this logic in BigQuery using two approaches. Both are presented below. **Approach A (JavaScript TEMP UDF)** is recommended because it guarantees 100% logical parity with the legacy Oracle PL/SQL function.

#### Approach A: JavaScript TEMP UDF (Recommended for 100% Logical Parity)
By passing an ordered array of discounts to a lightweight JavaScript UDF, we reproduce the exact character limit and skip logic:

```sql
-- Define temporary JS function to aggregate array items up to a 500-char limit
CREATE TEMP FUNCTION concat_discounts_js(arr ARRAY<STRING>) 
RETURNS STRING 
LANGUAGE js AS """
  if (!arr || arr.length === 0) return null;
  let result = arr[0];
  for (let i = 1; i < arr.length; i++) {
    // Check if adding the next discount (and separator) exceeds 500 characters
    if ((result + ', ' + arr[i]).length <= 500) {
      result += ', ' + arr[i];
    }
  }
  return result;
""";

-- Perform Truncate & Load of the target table
CREATE OR REPLACE TABLE `your_project.isbert_schema.sof_ta_disc_zusgf` AS
WITH dsc_distinct AS (
  -- Retrieve unique contract and version combinations
  SELECT DISTINCT
    cntrct_id,
    disc_vector_ty,
    cntrct_obj_version
  FROM
    `your_project.isbert_schema.sof_ta_discount`
),
pre_agg AS (
  -- Prepare discount values formatted as 'Discount_Name (Discount_Percent%)'
  SELECT DISTINCT
    cntrct_id,
    cntrct_obj_version,
    CONCAT(rabatt, ' (', COALESCE(SAFE_CAST(rabatthoehe AS STRING), '0'), '%)') AS rabatt_val
  FROM
    `your_project.isbert_schema.sof_ta_discount`
  WHERE rabatt IS NOT NULL
),
ordered_agg AS (
  -- Aggregate discount strings into sorted arrays per contract group
  SELECT
    cntrct_id,
    cntrct_obj_version,
    ARRAY_AGG(rabatt_val ORDER BY rabatt_val) AS rabatt_list
  FROM
    pre_agg
  GROUP BY
    cntrct_id,
    cntrct_obj_version
)
SELECT
  dzg.cntrct_id,
  dzg.cntrct_obj_version,
  dzg.disc_vector_ty,
  con.rabatt_alle
FROM
  dsc_distinct dzg
LEFT JOIN (
  SELECT
    cntrct_id,
    cntrct_obj_version,
    concat_discounts_js(rabatt_list) AS rabatt_alle
  FROM
    ordered_agg
) con
ON dzg.cntrct_id = con.cntrct_id
AND dzg.cntrct_obj_version = con.cntrct_obj_version;
```

#### Approach B: Standard BigQuery SQL (Alternative)
If the business decides that hard-truncation at 500 characters is acceptable (since contract descriptions rarely exceed 500 characters in practice), a standard BigQuery SQL implementation can be used:

```sql
CREATE OR REPLACE TABLE `your_project.isbert_schema.sof_ta_disc_zusgf` AS
WITH dsc_distinct AS (
  SELECT DISTINCT
    cntrct_id,
    disc_vector_ty,
    cntrct_obj_version
  FROM
    `your_project.isbert_schema.sof_ta_discount`
),
pre_agg AS (
  SELECT DISTINCT
    cntrct_id,
    cntrct_obj_version,
    CONCAT(rabatt, ' (', COALESCE(SAFE_CAST(rabatthoehe AS STRING), '0'), '%)') AS rabatt_val
  FROM
    `your_project.isbert_schema.sof_ta_discount`
  WHERE rabatt IS NOT NULL
),
aggregated AS (
  SELECT
    cntrct_id,
    cntrct_obj_version,
    -- Concatenate with sorting, and hard truncate the resulting string at 500 chars
    SUBSTR(STRING_AGG(rabatt_val, ', ' ORDER BY rabatt_val), 1, 500) AS rabatt_alle
  FROM
    pre_agg
  GROUP BY
    cntrct_id,
    cntrct_obj_version
)
SELECT
  dzg.cntrct_id,
  dzg.cntrct_obj_version,
  dzg.disc_vector_ty,
  con.rabatt_alle
FROM
  dsc_distinct dzg
LEFT JOIN
  aggregated con
ON dzg.cntrct_id = con.cntrct_id
AND dzg.cntrct_obj_version = con.cntrct_obj_version;
```

---

## 6. External Dependencies

1. **DB Link (`@pcrs1`)**:
   - The legacy SQL script includes `DEFINE v_carmen = "@pcrs1"`. 
   - **Resolution**: This DB link is a legacy leftover and is **not used** in the execution of this job. No external database connection is required for this pipeline on GCP; both tables are resident in BigQuery.
2. **Metadata Date Fetching (`dwtk_meldungen`)**:
   - The legacy script queries `dwtk_meldungen` to fetch a runtime date based on the `BERT_DROP_TEMP_TABLE` execution.
   - **Resolution**: This runtime variable `v_datum` is defined in the script header but never actually referenced in any queries. Therefore, this dependency on the metadata table `dwtk_meldungen` is retired for this specific pipeline.

---

## 7. Unresolved Issues & Risks

1. **Data Schema Differences (Special Characters in Names)**:
   - Oracle allows schema/table names containing `$` (such as `SOF$TA_DISCOUNT`). 
   - **Risk**: BigQuery does not support `$` in dataset or table names.
   - **Mitigation**: Standardize table naming in GCP during schema setup by replacing `$` with `_` (e.g., `sof_ta_discount` and `sof_ta_disc_zusgf`).
2. **Upstream Data Availability**:
   - This job depends on `sof_ta_discount` being fully populated. If upstream runs fail, this script will run on empty or stale data.
   - **Mitigation**: Configure proper task dependencies in Apache Airflow, preventing this task from executing unless the task migrating/loading `sof_ta_discount` has succeeded.

---

## 8. Build Plan

The following is the step-by-step build and deployment plan for migrating this job:

### Step 1: BigQuery DDL Setup
Deploy target table `isbert_schema.sof_ta_disc_zusgf` in BigQuery if it does not already exist:
```sql
CREATE OR REPLACE TABLE `your_project.isbert_schema.sof_ta_disc_zusgf` (
  cntrct_id INT64,
  cntrct_obj_version INT64,
  disc_vector_ty INT64,
  rabatt_alle STRING
);
```

### Step 2: Airflow DAG Development
Create an Airflow DAG file `bert_v_ta_disc_zusgf_dag.py` containing a native BigQuery task.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'data-migration',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bert_v_ta_disc_zusgf_dag',
    default_args=default_args,
    description='Concatenates and aggregates contract discount descriptions',
    schedule_interval='@daily',  # Align to upstream run frequencies
    catchup=False,
) as dag:

    # Task to aggregate discounts and overwrite the target table
    run_aggregation = BigQueryInsertJobOperator(
        task_id='aggregate_discounts_to_zusgf',
        configuration={
            "query": {
                "query": """
                    CREATE TEMP FUNCTION concat_discounts_js(arr ARRAY<STRING>) 
                    RETURNS STRING 
                    LANGUAGE js AS \"\"\"
                      if (!arr || arr.length === 0) return null;
                      let result = arr[0];
                      for (let i = 1; i < arr.length; i++) {
                        if ((result + ', ' + arr[i]).length <= 500) {
                          result += ', ' + arr[i];
                        }
                      }
                      return result;
                    \"\"\";

                    CREATE OR REPLACE TABLE `your_project.isbert_schema.sof_ta_disc_zusgf` AS
                    WITH dsc_distinct AS (
                      SELECT DISTINCT
                        cntrct_id,
                        disc_vector_ty,
                        cntrct_obj_version
                      FROM
                        `your_project.isbert_schema.sof_ta_discount`
                    ),
                    pre_agg AS (
                      SELECT DISTINCT
                        cntrct_id,
                        cntrct_obj_version,
                        CONCAT(rabatt, ' (', COALESCE(SAFE_CAST(rabatthoehe AS STRING), '0'), '%)') AS rabatt_val
                      FROM
                        `your_project.isbert_schema.sof_ta_discount`
                      WHERE rabatt IS NOT NULL
                    ),
                    ordered_agg AS (
                      SELECT
                        cntrct_id,
                        cntrct_obj_version,
                        ARRAY_AGG(rabatt_val ORDER BY rabatt_val) AS rabatt_list
                      FROM
                        pre_agg
                      GROUP BY
                        cntrct_id,
                        cntrct_obj_version
                    )
                    SELECT
                      dzg.cntrct_id,
                      dzg.cntrct_obj_version,
                      dzg.disc_vector_ty,
                      con.rabatt_alle
                    FROM
                      dsc_distinct dzg
                    LEFT JOIN (
                      SELECT
                        cntrct_id,
                        cntrct_obj_version,
                        concat_discounts_js(rabatt_list) AS rabatt_alle
                      FROM
                        ordered_agg
                    ) con
                    ON dzg.cntrct_id = con.cntrct_id
                    AND dzg.cntrct_obj_version = con.cntrct_obj_version;
                """,
                "useLegacySql": False,
            }
        },
    )

    run_aggregation
```

### Step 3: Integration & Testing
1. Load sample test data into the BigQuery source table `sof_ta_discount` representing multiple contracts with varying discount combinations (including some test contracts with cumulative lengths > 500 characters).
2. Execute the Airflow DAG task manually or via a test trigger.
3. Validate that:
   - Unique contract IDs match.
   - Discount names and percentages are formatted correctly as `Name (Percentage%)`.
   - The aggregated values match the expected strings.
   - Long strings are gracefully capped at `500` characters without mid-string word fragmentation (e.g., matching the logic of the temporary JavaScript function).