# Migration Notes: DW.DWH_PFIS_MPS_VBA_KORR

This document details the migration of the legacy UC4 UNIX job `DW.DWH_PFIS_MPS_VBA_KORR` ("Korrektur nicht ermittelbarer VBA-IDs") to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) and Google BigQuery.

---

## 1. Summary

The legacy standalone UC4 UNIX job `DW.DWH_PFIS_MPS_VBA_KORR` has been migrated to a modern cloud-native architecture. 

* **Source Components:**
  * UC4 UNIX Job: `DW.DWH_PFIS_MPS_VBA_KORR`
  * Orchestration Wrapper: KornShell script `r_pfis_mps_vba_korrektur`
  * Database Logic: Oracle SQL*Plus script `d_pfis_mps_vba_korrektur.sql`
* **Target Platform:** 
  * **Orchestration:** Apache Airflow (GCP Cloud Composer)
  * **Execution Wrapper:** Python 3 Scripting (`r_pfis_mps_vba_korrektur.py`)
  * **Database Engine:** Google BigQuery (Standard SQL Scripting)

The job corrects non-determinable Sales Channel (VBA) IDs in the facts table `dwh$ta_f_mps_nutzung` by matching text descriptions against the lookup view `dwh$vi_l_m2_vba`.

---

## 2. Generated Artifacts

The migration process generated three core files, each serving a distinct role in the target environment:

| File Name | Target Path / Location | Role |
| :--- | :--- | :--- |
| `dw_dwh_pfis_mps_vba_korr.py` | `dags/` (Cloud Composer DAGs folder) | **Airflow DAG:** Defines the orchestration workflow, execution parameters, environment variables, and triggers the execution wrapper. |
| `r_pfis_mps_vba_korrektur.py` | `dags/scripts/` or execution container | **Python Wrapper:** Replaces the legacy KornShell script. Manages execution logging, parses command-line arguments, and executes the BigQuery SQL script using the `google-cloud-bigquery` client library. |
| `d_pfis_mps_vba_korrektur.sql` | `dags/sql/` or GCS bucket | **BigQuery SQL Script:** Replaces the legacy Oracle SQL*Plus script. Contains the transactional, case-insensitive update statements optimized for BigQuery. |

---

## 3. Key Design Decisions

### Python Wrapper instead of Pure SQL
The legacy KornShell script did not merely execute SQL; it managed process-level logging, registered execution states via database-backed `DWMSG_*` utilities, and handled verbose output flags. Migrating this wrapper to Python 3 preserves these operational wrappers, ensures robust error trapping, and allows seamless integration with the Google Cloud SDK.

### BigQuery Scripting with Transactions
The SQL logic performs four sequential updates on the facts table `dwh$ta_f_mps_nutzung` (updating Level 6 IDs, clearing Level 6 texts, updating Level 7 IDs, clearing Level 7 texts). To ensure data consistency and prevent partial updates in the event of a failure, these statements are wrapped in a BigQuery scripting transaction block (`BEGIN TRANSACTION ... COMMIT TRANSACTION`).

### Elimination of Oracle-Specific Constructs
* **ROWID Elimination:** Oracle's physical `ROWID` was used in the legacy script to correlate self-joins. This was refactored into standard correlated scalar subqueries in BigQuery, which logically match records without relying on physical row addresses.
* **Outer Join `(+)` Syntax:** Legacy Oracle outer join syntax was replaced with standard SQL subqueries wrapped in `COALESCE` functions.
* **NVL to COALESCE:** All occurrences of `NVL` were converted to standard `COALESCE` functions.

### Idempotency and Retries
The operational notes state: *"fehlgeschlagener oder unterbrochener Prozeß kann ohne weitere Arbeiten erneut ausgeführt werden"* (failed or interrupted processes can be executed again without further manual work). The logic is naturally idempotent because it only updates rows where text descriptions are not null, and sets those descriptions to null upon successful ID resolution. Consequently, the Airflow DAG is safely configured with automatic retries (`retries: 1`).

---

## 4. Manual Steps Before Go-Live

Before activating the migrated workflow in production, the following setup steps must be completed:

### 1. BigQuery Schema and Dataset Setup
Ensure that the target dataset and tables exist in BigQuery:
* Dataset: `dwh` (or your environment-specific equivalent)
* Target Table: `dwh.dwh$ta_f_mps_nutzung`
* Lookup View: `dwh.dwh$vi_l_m2_vba`

### 2. Deploy Logging Stub Procedure
The exception block in `d_pfis_mps_vba_korrektur.sql` calls a legacy logging procedure. You must deploy a stub or equivalent logging procedure in BigQuery:
```sql
CREATE OR REPLACE PROCEDURE `dwh_utility.dwpa_meldung_fehler`(
  p_type STRING, 
  p_eintrags_nr INT64, 
  p_fehler_nr INT64, 
  p_err_text STRING, 
  p_err_code STRING
)
BEGIN
  -- Insert log into an audit table or output to console
  INSERT INTO `dwh_utility.audit_logs` (log_time, log_type, entry_id, error_number, message, code)
  VALUES (CURRENT_TIMESTAMP(), p_type, p_eintrags_nr, p_fehler_nr, p_err_text, p_err_code);
END;
```

### 3. IAM & Permissions
The service account running the Cloud Composer workers must have the following IAM roles on the target BigQuery dataset:
* `roles/bigquery.dataEditor` (to update `dwh$ta_f_mps_nutzung`)
* `roles/bigquery.jobUser` (to run query jobs)

### 4. Airflow Variables Configuration
Configure the following Airflow Variables in the Composer environment (Admin -> Variables):
* `GCP_PROJECT`: The GCP Project ID hosting your BigQuery instance.
* `GCP_REGION`: The GCP region (e.g., `europe-west3`).
* `GCS_BUCKET`: The GCS bucket where the SQL script is staged.
* `legacy_host`: The connection identifier for the legacy host (if fallback execution is required).

### 5. Environment Variables
Ensure that the environment variable `DW_DIR_ROOT` is set in your execution environment (or within the Airflow DAG environment configuration) to point to the root directory where `d_pfis_mps_vba_korrektur.sql` is stored.

---

## 5. Known Gaps & Unresolved References

### Legacy Tracking Utilities (`DWMSG_*`)
The Python wrapper script references external legacy binaries (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK`). 
* **Current Status:** The Python script contains fallback logic that auto-generates run IDs and logs to local files if these binaries are missing.
* **Redesign Recommendation (B4):** In a future phase, these legacy database-backed tracking utilities should be completely decommissioned and replaced with native Google Cloud Logging and Cloud Monitoring alerts.

### Oracle Package Constants
The SQL script references `dwpa_globals.k_alis_err_unknown`. This has been hardcoded to `-99999` in the BigQuery SQL script. The exact numerical value of this legacy constant should be verified with the DBA team.

### Duplicate Lookup Risk
The refactored subqueries assume that the lookup view `dwh$vi_l_m2_vba` contains unique mappings for each text description. If duplicate text entries exist in the lookup view, the scalar subqueries in Updates 1 and 3 will fail at runtime with a *"Scalar subquery produced more than one element"* error. If duplicates are possible, the SQL must be modified to include a `LIMIT 1` or explicit analytical ordering.

---

## 6. Validation

To validate the migration, perform the following test execution:

### Test Execution Steps
1. **Prepare Test Data:** Populate a test version of `dwh$ta_f_mps_nutzung` with sample rows where `m2_vba_ebene6_text` and `m2_vba_ebene7_text` are populated with valid descriptions from `dwh$vi_l_m2_vba`, and the corresponding ID columns are set to default values.
2. **Trigger DAG:** Manually trigger the DAG `dw_dwh_pfis_mps_vba_korr` from the Airflow UI or via the gcloud CLI:
   ```bash
   gcloud composer environments run <env-name> \
       --location <region> dags trigger -- dw_dwh_pfis_mps_vba_korr
   ```
3. **Monitor Logs:** Verify the task logs in the Airflow UI. Ensure that the Python wrapper successfully connects to BigQuery and executes the SQL script.

### Definition of "Passing"
The test is successful if:
* The Airflow DAG run completes with a `SUCCESS` status.
* The target table `dwh$ta_f_mps_nutzung` has its `m2_vba_ebene6_id` and `m2_vba_ebene7_id` columns updated to the correct IDs from the lookup view.
* The text columns (`m2_vba_ebene6_text` and `m2_vba_ebene7_text`) are set to `NULL` for all successfully matched rows.
* Unmatched rows retain their original text descriptions and default IDs.

---

## 7. Rollback Procedure

In the event of a critical failure during or after deployment, execute the following rollback steps:

### 1. Code Rollback
1. Pause the migrated DAG in the Airflow UI:
   ```bash
   gcloud composer environments run <env-name> \
       --location <region> dags pause -- dw_dwh_pfis_mps_vba_korr
   ```
2. Revert the DAG and script files in your Git repository and redeploy the previous stable version.

### 2. Data Rollback
Because this job performs in-place updates and clears text columns, you cannot simply "re-run" to undo changes. To restore the data to its pre-execution state, restore the target table from a backup or use BigQuery Time Travel:

```sql
-- Restore the table to its state 1 hour ago (adjust interval as needed)
CREATE OR REPLACE TABLE `dwh.dwh$ta_f_mps_nutzung`
AS SELECT * FROM `dwh.dwh$ta_f_mps_nutzung`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```