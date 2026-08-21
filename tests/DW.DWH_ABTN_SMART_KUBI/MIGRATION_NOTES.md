# Migration Notes: DW.DWH_ABTN_SMART_KUBI

This document provides comprehensive migration notes for the transition of the legacy UC4 job `DW.DWH_ABTN_SMART_KUBI` and its supporting shell-based execution framework to Google Cloud Platform (GCP).

---

## 1. Summary

The standalone UNIX job `DW.DWH_ABTN_SMART_KUBI` has been migrated from its legacy UC4/Automic and Oracle database environment to **Google Cloud Platform (GCP)**. 

* **Orchestration Target**: Google Cloud Composer (Apache Airflow)
* **Execution Target**: Google BigQuery (Standard SQL Scripting)
* **Migration Scope**: 
  * The master UC4 job orchestration.
  * The environment initialization script (`.dw_init`).
  * The core PL/SQL aggregation script (`d_abtn_x_smart_kubi.sql`).
  * Sibling shell utilities for error handling, logging, and SQL execution (`r_sqlscript`, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`).

### Business Purpose
This pipeline aggregates access logs and transaction records from partitioned fact tables (`dwh$ta_f_d1_twvv_tn`) for a dynamically calculated reporting month (`MONATSID`). It resolves current and historical tariff mappings and populates the target temporary table `dwh$ta_t_smart_kubi` (mapped to `dw.dwh_ta_t_smart_kubi` in BigQuery) for downstream reporting.

---

## 2. Generated Artifacts

The migration process has generated the following clean, modular Python and SQL files:

| Target File Path | Language | Role / Description |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi.py` | Python (Airflow) | **Airflow DAG**: Orchestrates the workflow, calculates the dynamic `MONATSID` based on the logical execution date, and triggers the execution wrapper. |
| `local/home/gurunathan_t/kubi/dw_init.py` | Python | **Environment Initializer**: Replaces `.dw_init`. Maps legacy directory structures to local paths or Google Cloud Storage (`gs://`) bucket paths. |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | SQL (BigQuery) | **Core SQL Script**: Replaces the Oracle PL/SQL anonymous block. Implements modern ANSI SQL-92 joins and performs the core data aggregation. |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Python | **Error & Status Utility**: Replaces `f_alis_msgerr.ksh`. Manages centralized logging and job status updates by calling BigQuery stored procedures. |
| `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Python | **SQL Execution Helper**: Replaces `h_alis_sqlplus.ksh`. Validates SQL script readability and executes scripts natively via the BigQuery Python client. |
| `local/home/gurunathan_t/kubi/r_sqlscript.py` | Python | **Execution Wrapper**: Replaces the `r_sqlscript` shell wrapper. Parses arguments, resolves relative SQL paths, and coordinates execution by importing helper modules directly. |

---

## 3. Key Design Decisions

### Direct Python Imports vs. Subprocess Spawning
In the legacy environment, shell scripts sourced other scripts or executed them as subprocesses. In the migrated architecture, `r_sqlscript.py` and `h_alis_sqlplus.py` import `f_alis_msgerr.py` directly as a Python module. This eliminates process overhead, simplifies error propagation, and ensures clean stack traces.

### BigQuery Scripting & Stored Procedures
The Oracle PL/SQL anonymous block was refactored into a native BigQuery script (`DECLARE...BEGIN...EXCEPTION...END`). Legacy Oracle package calls (e.g., `dwpa_meldung.fehler`) are mapped to BigQuery stored procedures (e.g., `CALL dw.dwpa_meldung_fehler(...)`) to preserve the existing operational monitoring and auditing model.

### ANSI SQL-92 Join Modernization
Legacy Oracle-style outer joins `(+)` were refactored into explicit `LEFT OUTER JOIN` clauses. Join filter conditions involving date boundaries were moved into the `ON` clause to preserve exact semantic equivalence and prevent Cartesian products.

### Idempotent Date Calculation
The `MONATSID` calculation is performed within the Airflow DAG using the logical execution date (`logical_date`). This ensures that re-running a historical DAG run yields the same reporting month identifier, maintaining strict pipeline idempotency.

### Preservation of German Print Literals
In compliance with strict translation rules, all original logging, diagnostic messages, and error strings are preserved verbatim in German (e.g., `"Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus"`). This maintains operational consistency and compatibility with legacy log parsers.

---

## 4. Manual Steps Before Go-Live

Before deploying the pipeline to production, the following manual setup steps must be completed:

### 1. Schema and Dataset Creation
Ensure the target BigQuery dataset (default: `dw`) exists in your project. Create the target table and ensure the source tables/views are populated:
* Target Table: `dw.dwh_ta_t_smart_kubi`
* Source Tables/Views:
  * `dw.dwh_vi_l_map_fa_tarif`
  * `dw.bl_d_tarif`
  * `dw.dwh_ta_f_d1_twvv_tn`
  * `dw.dwh_ta_c_vertrag`

### 2. Pre-deploy Logging Stored Procedures
The metadata logging dataset (default: `dwpa_meldung`) must contain the following stored procedures and functions:
* `dwpa_meldung_setze_status_ok`
* `dwpa_meldung_setze_status_abbruch`
* `dwpa_meldung_erzeuge_eintrag`
* `dwpa_meldung_fehler`
* `dwpa_meldung_set_stichtag_info`
* `dwpa_meldung_append_timing_infos`
* `dwpa_meldung_next_val` (Sequence generator function)

### 3. IAM & Permissions
The service account running the Cloud Composer workers must be granted:
* `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the target datasets.
* `roles/storage.objectViewer` (and `roles/storage.objectCreator` for logs) on the GCS bucket configured in Airflow.

### 4. Airflow Variables Configuration
Configure the following Airflow Variables in the Cloud Composer environment:
* `GCS_BUCKET`: The GCS bucket name used for storing logs and scripts (e.g., `my-composer-bucket`).
* `GCP_CONN_ID`: The Airflow connection ID for GCP/BigQuery (typically `google_cloud_default`).
* `R_SQLSCRIPT_PATH`: The absolute path to `r_sqlscript.py` on the Composer worker filesystem or GCS.

### 5. Scheduling
The DAG is currently configured with `schedule=None` (triggered externally), matching the legacy UC4 configuration. If a time-based schedule is required, update the `schedule` parameter in `dw_dwh_abtn_smart_kubi.py`.

---

## 5. Known Gaps & Unresolved References

### Legacy Bug in `r_sqlscript` Path Validation
The legacy script contains a condition `if [ -f "$l_DBskript" ]` which raises `ErrNr=198` ("Parameter value unknown") if the file **does** exist. This logical inversion was preserved in `r_sqlscript.py` to maintain exact compatibility but is flagged for redesign/review (B4 item).

### Oracle-to-BigQuery Function Mapping
The custom PL/SQL package `dwpa_globals.k_alis_err_unknown` was mapped to a hardcoded generic error code `-20001` in the SQL script. If specific error codes are required, they must be registered in the target BigQuery metadata tables.

### Empty String vs. NULL
Oracle treats empty strings as `NULL`. In BigQuery, `TRIM(fact.vo_kenn_bearb) IS NULL OR TRIM(fact.vo_kenn_bearb) = ''` was used to prevent functional variance. This should be monitored during validation.

---

## 6. Validation

To validate the migration, execute the pipeline and verify the outputs.

### How to Run the Tests
1. **Airflow Trigger**: Trigger the DAG `dw_dwh_abtn_smart_kubi` manually via the Airflow UI.
2. **CLI Execution**: Alternatively, run the Python wrapper script directly in a test environment:
   ```bash
   export GCP_PROJECT="your-gcp-project"
   export BQ_DATASET="dw"
   python3 r_sqlscript.py -j ABTN_SMART_KUBI -f d_abtn_x_smart_kubi.sql -i 202308
   ```

### What "Passing" Means
* The Airflow DAG completes with a `SUCCESS` status.
* The target table `dw.dwh_ta_t_smart_kubi` is truncated and populated with aggregated records.
* The execution log contains the exact German output:
  * `Berichtsmonat: <MONATSID>`
  * `<X> rows inserted in DWH$TA_T_SMART_KUBI`
* The BigQuery metadata tables (e.g., `dwpa_meldung`) record the execution status as successful.

---

## 7. Rollback Procedure

If a critical issue is detected post-go-live, follow these steps to revert to the legacy system:

1. **Pause the Airflow DAG**:
   ```bash
   airflow dags pause dw_dwh_abtn_smart_kubi
   ```
2. **Restore Target Table (Optional)**:
   If the target table `dw.dwh_ta_t_smart_kubi` needs to be restored to its pre-execution state, use BigQuery's time travel feature:
   ```sql
   CREATE OR REPLACE TABLE dw.dwh_ta_t_smart_kubi AS 
   SELECT * FROM dw.dwh_ta_t_smart_kubi FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
3. **Re-enable Legacy Job**:
   Re-enable the legacy UC4 job `DW.DWH_ABTN_SMART_KUBI` on the legacy environment to resume processing.