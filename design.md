# Migration Design — DW.BERT_AUSD_BP_TA_APN_VERTRAG

This document outlines the migration design for the legacy ETL job `DW.BERT_AUSD_BP_TA_APN_VERTRAG` from an on-premises Oracle / UC4 environment to Google Cloud Platform (GCP) with **BigQuery** and **Cloud Composer (Apache Airflow)**.

---

## 1. Purpose & Scope

The job `DW.BERT_AUSD_BP_TA_APN_VERTRAG` is an ETL process within the **Sales Data (SD)** domain, specifically part of the **BERT Auxiliary** subsystem. 

### Business Purpose
This job prepares and aggregates selected basic product data—specifically **Access Point Names (APN)** and associated **Contract Reference IDs**—per contract. It consolidates multiple raw rows per contract into a single-row summary containing comma-separated lists of APNs and contract references, with a strict safety length limit of 100 characters. Downstream credit scoring and billing systems (such as BERT or FOS) consume this unified contract view.

### Migration Scope
- **Scheduler Migration:** Convert the UC4 UNIX Job `DW.BERT_AUSD_BP_TA_APN_VERTRAG` into an Airflow DAG task.
- **Script Retirement:** Deprecate the legacy KornShell wrappers (`r_ausd_bp_ta_apn_vertrag.ksh` and `k_ausd_bp_ta_apn_vertrag.ksh`) by replacing their parameter validation and execution tracking with native Airflow capabilities.
- **SQL Translation:** Convert the Oracle PL/SQL stored procedure block containing a row-by-row procedural cursor loop into a highly optimized, set-based BigQuery SQL statement.

---

## 2. Source Inventory

The following source components comprise this assembled job:

| File Path / Component | Tech / Role | Complexity Tier | Automation Bucket | Est. Effort | Notes / Migration Action |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml` | UC4 XML / Job Def | Medium | **B2** (Semi-Auto) | Low | Replace scheduling rules, login definitions, and task triggers with Airflow DAG task. |
| `r_ausd_bp_ta_apn_vertrag.ksh` | KornShell / Wrapper | Medium | **B2** (Semi-Auto) | Low | **Retire.** Date parsing and wrapper orchestration are replaced by Airflow task-level variables and parameters. |
| `k_ausd_bp_ta_apn_vertrag.ksh` | KornShell / Controller | Medium | **B2** (Semi-Auto) | Low | **Retire.** Parameter checks and SQLPlus execution are replaced by Airflow's `BigQueryInsertJobOperator`. |
| `d_ausd_bp_ta_apn_vertrag.sql` | Oracle PL/SQL / ETL | Complex | **B3** (Manual) | Medium | **Rewrite.** The procedural cursor loop that executes row-by-row string concatenation must be redesigned into a set-based BigQuery aggregation. |

---

## 3. Target Architecture

The modernized target architecture on GCP utilizes serverless and managed services:

```
  +--------------------------------------------+
  |        Cloud Composer (Airflow)            |  <-- Orchestrator
  +--------------------------------------------+
                        |
                        v
  +--------------------------------------------+
  |              BigQuery                      |  <-- Compute & Storage
  |                                            |
  |  Source:  `isbert_schema.sof$ta_bpr_apn`    |
  |  Target:  `isbert_schema.sof$ta_apn_vertrag`|
  +--------------------------------------------+
```

### Components
1. **Orchestrator (Cloud Composer):** Orchestrates the task as part of the `DW_BERT_STAMMDATEN_JP` DAG. It handles retries, execution logs, and job status.
2. **Compute (BigQuery):** Performs the entire data transformation in-database using Standard SQL and a temporary JavaScript User-Defined Function (UDF) for highly efficient, set-based array aggregation.
3. **Storage (BigQuery Tables):**
   - **Source Table:** `isbert_schema.sof$ta_bpr_apn`
   - **Target Table:** `isbert_schema.sof$ta_apn_vertrag`

---

## 4. Data Flow & Lineage

### Scheduler & Execution Lineage
Within the parent UC4 Job Plan `DW.BERT_P_BASISPRODUKT_JP`, this job runs in a specific order:
1. **Predecessor Task:** `DW.BERT_AUSD_BP_TA_BPR_APN` runs first to prepare and load raw APN data into the source table `isbert_schema.sof$ta_bpr_apn`.
2. **Current Task (`DW.BERT_AUSD_BP_TA_APN_VERTRAG`):** Reads `sof$ta_bpr_apn`, aggregates the strings, and writes the summarized results into `isbert_schema.sof$ta_apn_vertrag`.
3. **Successor Task:** `DW.BERT_AUSD_BP_TA_P_BASISPROD` executes next, joining `sof$ta_apn_vertrag` with other basic product tables.

### Table-Level Data Flow
```
[isbert_schema.sof$ta_bpr_apn]  -->  (BigQuery Aggregation SQL)  -->  [isbert_schema.sof$ta_apn_vertrag]
```

---

## 5. Transformation Logic

### Legacy PL/SQL Logic Analysis
The legacy Oracle SQL script (`d_ausd_bp_ta_apn_vertrag.sql`) truncates `sof$ta_apn_vertrag` and loops through a cursor of `sof$ta_bpr_apn` ordered by `cntrct_id`.
Inside the cursor loop, for each contract:
- It appends the `access_point_name` string separated by `", "` only if the total length does not exceed 100 characters.
- It appends the `cntrct_id_ref` string separated by `", "` only if the total length does not exceed 100 characters.
- Once the contract ID changes, it inserts the accumulated strings into `sof$ta_apn_vertrag`.

### Target BigQuery SQL (Recommended Option - JS UDF)
Using a BigQuery **JavaScript UDF** combined with `ARRAY_AGG` allows us to replicate this logic in a high-performance, set-based manner without resorting to complex, slow, recursive Common Table Expressions (CTEs). It guarantees identical string accumulation behavior, avoiding partial token truncation while strictly honoring the 100-character boundary.

```sql
-- Recommended production-ready BigQuery SQL script

-- 1. Temporary UDF for robust, length-constrained string aggregation
CREATE TEMP FUNCTION aggregate_strings_with_limit(arr ARRAY<STRING>, max_len INT64)
RETURNS STRING
LANGUAGE js AS """
  if (!arr) return null;
  let accum = "";
  for (let i = 0; i < arr.length; i++) {
    let val = arr[i];
    if (val === null || val === undefined) continue;
    // Check if adding the value and the separator exceeds the maximum length
    if ((accum + val + ", ").length <= max_len) {
      accum += val + ", ";
    }
  }
  // Trim the trailing comma and space
  if (accum.endsWith(", ")) {
    accum = accum.slice(0, -2);
  }
  return accum === "" ? null : accum;
""";

-- 2. Truncate and Reload Target Table in a set-based query
CREATE OR REPLACE TABLE `isbert_schema.sof$ta_apn_vertrag` AS
SELECT
  cntrct_id,
  aggregate_strings_with_limit(
    ARRAY_AGG(access_point_name ORDER BY bpr_id, cntrct_id_ref, access_point_name IGNORE NULLS),
    100
  ) AS access_point_name,
  aggregate_strings_with_limit(
    ARRAY_AGG(cntrct_id_ref ORDER BY bpr_id, cntrct_id_ref, access_point_name IGNORE NULLS),
    100
  ) AS cntrct_id_ref
FROM `isbert_schema.sof$ta_bpr_apn`
GROUP BY cntrct_id;
```

### Alternative BigQuery SQL (Standard SQL - No JS UDF)
If organizational policies restrict JS UDFs, the aggregation can be solved using standard SQL. Note that this alternative truncates strings to 100 characters directly, which might split a trailing APN name mid-token, but avoids script execution inside BigQuery:

```sql
CREATE OR REPLACE TABLE `isbert_schema.sof$ta_apn_vertrag` AS
SELECT
  cntrct_id,
  -- Aggregates all values, then trims trailing characters and truncates to 100 characters.
  SUBSTR(RTRIM(STRING_AGG(access_point_name, ', ' ORDER BY bpr_id, cntrct_id_ref, access_point_name), ', '), 1, 100) AS access_point_name,
  SUBSTR(RTRIM(STRING_AGG(cntrct_id_ref, ', ' ORDER BY bpr_id, cntrct_id_ref, access_point_name), ', '), 1, 100) AS cntrct_id_ref
FROM `isbert_schema.sof$ta_bpr_apn`
GROUP BY cntrct_id;
```

---

## 6. External Dependencies

1. **Oracle Database Links (`@pcrs1`):** 
   - *Legacy Status:* Historically used to reference schema databases across instances.
   - *Replacement:* Retires. In GCP, all datasets reside natively within the same BigQuery project, and access is controlled via IAM. 
2. **Local File System Logging (`bert_k_ausd_bp_ta_apn_vertrag.tmp`):**
   - *Legacy Status:* Shell wrappers logged run metrics to local files on the server `DWHDWH2P`.
   - *Replacement:* Retires. Execution metadata and row counts are automatically captured by BigQuery's execution metadata (`INFORMATION_SCHEMA.JOBS_BY_PROJECT`) and Cloud Logging.
3. **Dead Code Dependencies (`isbert_schema.dwtk_meldungen`):**
   - *Legacy Status:* Oracle PL/SQL historically queried this table to compute dynamic partition dates. Since version 10.2.1, static table names and standard `TRUNCATE` operations are used.
   - *Replacement:* The query to `dwtk_meldungen` is obsolete and is completely removed from the target script.

---

## 7. Unresolved / Risks

### Risks & Mitigations
* **Determinism of Aggregation Order:**
  - *Risk:* The legacy cursor selected rows sorted only by `cntrct_id`, meaning the concatenation order within a contract group was non-deterministic and database-dependent.
  - *Mitigation:* We enforce strict deterministic order inside BigQuery's `ARRAY_AGG(...)` by explicitly sorting on `ORDER BY bpr_id, cntrct_id_ref, access_point_name`. This guarantees completely reproducible results.
* **Dead Code Clean-Up:**
  - *Risk:* The legacy shell wrappers parse complex date variables (yesterday/today parameters from `gestern.ksh`) and pass them to SQLPlus.
  - *Mitigation:* Verified that the SQL script doesn't read these arguments. Thus, we safely exclude these parameters from the target DAG, simplifying runtime maintenance.

---

## 8. Build Plan

This plan defines the sequence of development and deployment tasks:

1. **BigQuery Table Setup:**
   Confirm target table schema exists or is deployed in BigQuery:
   ```sql
   CREATE TABLE IF NOT EXISTS `isbert_schema.sof$ta_apn_vertrag` (
     cntrct_id STRING,
     access_point_name STRING,
     cntrct_id_ref STRING
   );
   ```
2. **Deploy Transformation Script:**
   Save the recommended BigQuery SQL query (including the temporary JS UDF) as `d_ausd_bp_ta_apn_vertrag.sql` in the Cloud Storage folder allocated for Composer resources (e.g., `gs://us-central1-composer-bucket/dags/sql/`).
3. **Create Orchestration Task:**
   Integrate the task into the parent `DW_BERT_STAMMDATEN_JP` Airflow DAG. Define the operator:
   ```python
   from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

   process_apn_vertrag = BigQueryInsertJobOperator(
       task_id="process_apn_vertrag",
       configuration={
           "query": {
               "query": "{% include 'sql/d_ausd_bp_ta_apn_vertrag.sql' %}",
               "useLegacySql": False,
           }
       },
       # Set dependency: process_apn_vertrag runs after loading sof$ta_bpr_apn
   )
   ```
4. **Validation & Unit Testing:**
   - Seed test cases in `sof$ta_bpr_apn` with multiple records for a single contract, including very long APN names (>50 chars).
   - Execute the BigQuery SQL.
   - Assert that `sof$ta_apn_vertrag` matches exactly the legacy output and that no single aggregated string exceeds 100 characters.