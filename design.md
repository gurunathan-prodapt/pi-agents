# Migration Design — ausd_bp_ta_bpr_apn

This document provides a comprehensive, production-grade migration blueprint for migrating the legacy DWH batch job `ausd_bp_ta_bpr_apn` to Google Cloud Platform (GCP). The target architecture transitions scheduling from UC4/Automic to Google Cloud Composer (Apache Airflow) and standardizes data transformations within Google BigQuery.

---

## 1. Purpose & Scope

### 1.1 Business Purpose
The legacy job `ausd_bp_ta_bpr_apn` (associated with the BERT system) prepares and provisions basic product instances (e.g., VPN, IV-VPN, WAP-Intranet, Telemetry, and Blackberry solutions) mapped to their corresponding contract access point names. This curated slice of contract data is staged and provided to downstream systems, specifically the **Forderungsscoring (FOS) / Debt Collection Scoring Engine**, to evaluate contracts based on active enterprise or data-only products.

### 1.2 Migration Scope
The migration encompasses the following legacy source elements:
1. **UC4 Job Definition (`DW.BERT_AUSD_BP_TA_BPR_APN.xml`)**: Schedules and executes the shell script wrapper.
2. **KornShell Orchestration (`r_ausd_bp_ta_bpr_apn.ksh` & `k_ausd_bp_ta_bpr_apn.ksh`)**: Responsible for execution logging, run date calculations (using `gestern.ksh`), checking status records, parameter checks, and calling the SQL engine.
3. **Oracle SQL*Plus Script (`d_ausd_bp_ta_bpr_apn.sql`)**: Performs target table truncation and is the primary DML engine joining the basis product instances to contract reference details.

---

## 2. Source Inventory

The components of this job are categorized and structured as follows:

| Legacy File Path / Key | Tech Category | Complexity Tier | Automation Bucket | Est. Effort | Role / Functional Summary |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `vobs/dw_source/.../DW.BERT_AUSD_BP_TA_BPR_APN.xml` | UC4 XML | Medium | Semi-Automated (0.65) | Medium | Main UC4 Unix Job setting environment parameters and triggering execution. |
| `vobs/dw_source/.../r_ausd_bp_ta_bpr_apn.ksh` | KornShell | Medium | Semi-Automated (0.65) | Medium | Primary execution wrapper. Parses parameters, initializes logging, manages errors. |
| `vobs/dw_source/.../k_ausd_bp_ta_bpr_apn.ksh` | KornShell | Medium | Semi-Automated (0.65) | Medium | Validation controller. Checks parameters, formats target dates, and triggers SQL execution. |
| `vobs/dw_source/.../d_ausd_bp_ta_bpr_apn.sql` | Oracle SQL | Medium | Semi-Automated (0.65) | Medium | Database execution logic. Truncates target and loads joined results into `sof$ta_bpr_apn`. |

---

## 3. Target Architecture

The modernized architecture moves away from physical hosts and local shell file manipulation to GCP cloud-native components.

```
       [Cloud Composer (Airflow DAG)] 
                   │
                   ▼  (Orchestrates)
       [BigQuery SQL DML Job]
         ├── Reads: dw_bert.sof_ta_bpr_instance
         ├── Reads: dw_bert.sof_ta_apn_carmen
         └── Writes (Truncate/Insert): dw_bert.sof_ta_bpr_apn
```

### 3.1 Google BigQuery Layout
To align with standard data-warehousing conventions on BigQuery, special characters in Oracle table names (such as `$`) are replaced with underscores (`_`), and schema/dataset structures are modeled in a multi-layered design.

*   **Project Workspace**: `gcp-enterprise-dwh`
*   **Operational Dataset**: `dw_bert`
*   **Table Mapping**:

| Legacy Oracle Table / View | BigQuery Target Table | Column Name Mapping | Partitioning / Clustering (BigQuery) |
| :--- | :--- | :--- | :--- |
| `sof$ta_bpr_instance` | `dw_bert.sof_ta_bpr_instance` | Maintain structural columns: `cntrct_id`, `bpr_id`, `cntrct_id_ref` | Cluster by `bpr_id`, `cntrct_id_ref` |
| `sof$ta_apn_carmen` | `dw_bert.sof_ta_apn_carmen` | Maintain structural columns: `cntrct_id`, `access_point_name` | Cluster by `cntrct_id` |
| `sof$ta_bpr_apn` | `dw_bert.sof_ta_bpr_apn` | `CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, `ACCESS_POINT_NAME` | Cluster by `bpr_id`, `cntrct_id` |
| `isbert_schema.dwtk_meldungen`| `dw_bert.dwtk_meldungen` | Maintain legacy monitoring schema for completeness checks if required. | Partition by `timecreated` (Day) |

### 3.2 Job Orchestration & Execution
The UC4 scheduling configuration and KornShell scripts are consolidated into a clean, cloud-native **Apache Airflow DAG** executed in **Google Cloud Composer**.
*   **Logging**: Shell functions like `DWMSG_Logdateiname` and `f_alis_msgerr.ksh` are replaced by standard **Cloud Logging** integration in Airflow.
*   **Parameter Passing**: Execution Date parameters (`p_stichtag`) are natively managed via Airflow's execution contexts (`{{ ds }}`).

---

## 4. Data Flow & Lineage

The migrated data flow steps are executed in a sequential pipeline modeled within the Airflow DAG:

```
Step 1: Check upstream dependencies (Sensor tasks for source table fresh updates)
  │
  ▼
Step 2: Clean target table (DML Truncate or Airflow Write Disposition)
  │
  ▼
Step 3: Join and Insert Core Data (Query executing in BigQuery)
  │
  ▼
Step 4: Execute row-count verification and logging task (Metadata tracking)
```

### 4.1 Upstream Execution Order
1. Upstream extraction pipelines must first land and merge fresh data into the staging/dw tables:
    *   `dw_bert.sof_ta_bpr_instance`
    *   `dw_bert.sof_ta_apn_carmen`
2. Once the upstream loads complete, the Airflow DAG `dag_bert_ausd_bp_ta_bpr_apn` is triggered.
3. The query filters on `bpr_id` matching key product keys: `(2828, 2829, 2830, 2831, 2925, 2926, 2998, 2999, 3000)`.
4. Resulting records are persisted in `dw_bert.sof_ta_bpr_apn`.

---

## 5. Transformation Logic

The core logic from the legacy SQL script `d_ausd_bp_ta_bpr_apn.sql` has been clean-compiled and optimized for Google BigQuery. 

### 5.1 Optimization Details
*   **Optimizer Hints**: The Oracle optimizer hints `/*+ full(bp) parallel(bp,4) full(ap) parallel(ap,4) */` are stripped out. BigQuery utilizes a distributed execution plan that automates physical parallel scanning without requiring manual hints.
*   **Schema & Variable Resolution**: The legacy script dynamically evaluated `v_datum` from `dwtk_meldungen`. Since the column layout modifications in version `10.2.1` removed the suffix from the table name, the query is simplified to a static, standard SQL query execution.

### 5.2 BigQuery SQL Implementation

This script is stored as a SQL file (`d_ausd_bp_ta_bpr_apn.sql`) in the Airflow workspace/dags directory and executed via the Airflow BigQuery operator.

```sql
-- ===================================================================
-- Target: dw_bert.sof_ta_bpr_apn
-- Source: dw_bert.sof_ta_bpr_instance & dw_bert.sof_ta_apn_carmen
-- Process: Provision BERT basis product mappings to APNs
-- ===================================================================

-- Step 1: Truncate current target table (Replaces legacy TRUNCATE REUSE STORAGE)
TRUNCATE TABLE `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn`;

-- Step 2: Insert mapped enterprise products
INSERT INTO `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn` (
  CNTRCT_ID,
  BPR_ID,
  CNTRCT_ID_REF,
  ACCESS_POINT_NAME
)
SELECT DISTINCT
  bp.cntrct_id,
  bp.bpr_id,
  bp.cntrct_id_ref,
  ap.access_point_name
FROM
  `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_instance` bp
INNER JOIN
  `gcp-enterprise-dwh.dw_bert.sof_ta_apn_carmen` ap
ON
  bp.cntrct_id_ref = ap.cntrct_id
WHERE
  bp.bpr_id IN (
    2828, -- VPN
    2829, -- IV_VPN
    2830, -- WAP-Intranet
    2831, -- Telemetry
    2925, -- Mobile IP VPN (50% discount)
    2926, -- Mobile IP VPN (100% discount)
    2998, -- Blackberry Solution
    2999, -- Blackberry Solution (10% discount)
    3000  -- Blackberry Solution (20% discount)
  );
```

---

## 6. External Dependencies

The table below outlines external legacy subsystems and dependencies, alongside their strategic GCP replacement strategies:

| Legacy Dependency | Source System / Details | GCP Replacement Strategy |
| :--- | :--- | :--- |
| **Oracle DBMS** | Source system hosting `sof$ta_...` schemas | Entirely migrated to **Google BigQuery** as a consolidated enterprise data platform. |
| **Local Filesystem** | `tmpFile` at `/tmp/bert_k_ausd_bp_ta_bpr_apn.tmp` | BigQuery SQL query jobs natively track and report modified and inserted row counts in task execution logs. No temporary storage files are required. |
| **`gestern.ksh`** | Calculated yesterday's and today's dates | Native Airflow environment variables (`{{ ds }}` and `{{ macros.ds_add(ds, -1) }}`) are injected directly into metadata tasks if dynamic dates are required. |
| **Error Concept** | `f_alis_msgerr.ksh` and `DWMSG_...` procedures | Handled via **Cloud Composer (Airflow)** task state management, routing error alerts directly to Cloud Monitoring or Slack/PagerDuty webhooks. |

---

## 7. Unresolved / Risks

### 7.1 Deprecated Shell Processing
In `k_ausd_bp_ta_bpr_apn.ksh`, there is a commented-out section of legacy script lines that processed text files using Unix utilities:
```bash
#sed s/\ //g ... cibasis_data24.dat
#sort -u -k 1 ...
#join -j1 1 ...
```
*   **Action Required**: Confirm with business stakeowners if these CSV exports (`cibasisprodukt.csv`) are indeed deprecated and obsolete, or if this scoring job requires a Google Cloud Storage output export.

### 7.2 Stichtag Date Restrictions
The wrapper scripts include parameters for `p_wiederanlaufWert` and date controls to evaluate validity dates on contracts:
```
Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag
```
*   **Action Required**: The legacy SQL code (`d_ausd_bp_ta_bpr_apn.sql`) does not enforce this dynamic date logic directly inside the main `INSERT` statement (it is a standard static join). Ensure that filtering logic has not been mistakenly omitted in legacy SQL modifications, or confirm if temporal slicing is executed inside the upstream staging view layers.

---

## 8. Build Plan

This ordered build plan must be followed to deploy the job successfully to production:

```
[1. Build Target BQ Tables] ──► [2. Deploy BigQuery SQL] ──► [3. Code Composer Airflow DAG] ──► [4. Orchestrate & Test]
```

### 8.1 Ordered Execution Steps
1.  **BigQuery Infrastructure Provisioning**:
    *   Create target table `dw_bert.sof_ta_bpr_apn` with clustering set on column `bpr_id`.
    *   Provision testing datasets with mocked tables for `sof_ta_bpr_instance` and `sof_ta_apn_carmen`.
2.  **SQL Deployment**:
    *   Commit the refactored BigQuery SQL file to the version control system (Git) under `dags/sql/d_ausd_bp_ta_bpr_apn.sql`.
3.  **Airflow DAG Construction**:
    *   Develop the DAG file `dag_bert_ausd_bp_ta_bpr_apn.py` implementing the `BigQueryInsertJobOperator`.
    *   Implement data quality validations to verify that the target table output matches expected counts.
4.  **Integration Testing**:
    *   Deploy the code to the Composer Airflow DAG bucket.
    *   Execute dry-runs, validate row-count matching against the legacy Oracle database execution, and register logs in Cloud Logging.