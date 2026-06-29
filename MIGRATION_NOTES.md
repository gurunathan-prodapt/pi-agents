# Migration Notes: `gestern.ksh` Date Calculation Utility

This document details the migration of the legacy KornShell date calculation utility `gestern.ksh` to Google Cloud Platform (GCP), utilizing BigQuery and Cloud Composer (Apache Airflow).

---

## 1. Summary

The legacy utility script `gestern.ksh` (originally located at `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh`) has been migrated from its on-premises Unix environment to a cloud-native architecture on **Google Cloud Platform (GCP)**. 

### Scope of Migration
*   **Legacy Function**: Calculated reporting date parameters (`Var_Datum_Heute`, `Var_Datum_Gestern`, `Var_Monat_Heute`, `Var_Monat_Gestern`) using manual shell-based date arithmetic and custom leap-year logic. It outputted these values to `stdout` for downstream consumption by UC4-scheduled scripts.
*   **Target Platform**: **BigQuery** (for database-level date calculations) and **Cloud Composer / Apache Airflow** (for orchestration-level parameter injection).
*   **Key Improvements**: 
    *   Eliminated manual, error-prone leap-year calculations.
    *   Resolved a legacy bug where centurial leap years (e.g., Year 2000) were incorrectly processed.
    *   Enforced explicit timezone handling (`Europe/Berlin`) to prevent UTC rollover discrepancies.

---

## 2. Generated Artifacts

The migration process generated two primary artifacts to support both database-centric and orchestration-centric execution patterns:

### 1. `src/bigquery/isbert_aufbereitung/v_reporting_dates.sql`
*   **Role**: Database-level deployment script.
*   **Components**:
    *   **View (`isbert_aufbereitung.v_reporting_dates`)**: A lightweight, dynamic view that calculates and exposes the four target date variables in the exact legacy string formats (`YYYYMMDD` and `YYYYMM`).
    *   **Stored Procedure (`isbert_aufbereitung.get_reporting_dates`)**: A backwards-compatible stored procedure that outputs these values into SQL variables for use in legacy-style procedural SQL scripts.

### 2. `src/dags/isbert_reporting_dag.py`
*   **Role**: Orchestration-level workflow script.
*   **Components**:
    *   **Airflow DAG (`isbert_reporting_dates_dag`)**: Demonstrates how to leverage native Airflow Jinja macros (`ds_nodash`, `yesterday_ds_nodash`, etc.) to dynamically inject the correct date parameters directly into BigQuery jobs, bypassing the need for database-level lookups entirely.

---

## 3. Key Design Decisions

### Cloud-Native Date Arithmetic
Instead of translating the complex conditional shell logic (which manually decremented days and checked month boundaries), we adopted native BigQuery and Python date functions. This ensures 100% accuracy for leap years, month lengths, and year transitions.

### Timezone Pinning (`Europe/Berlin`)
*   **Decision**: All target calculations explicitly reference the `Europe/Berlin` timezone.
*   **Reasoning**: Legacy scripts ran on local servers configured to German system time (CET/CEST). GCP services default to UTC. Without explicit timezone pinning, jobs running between 11:00 PM and midnight CET would calculate dates based on the next day, causing critical data mismatches.

### Dual-Path Integration (Orchestration vs. Database)
*   **Decision**: Provide both an Airflow macro implementation and a BigQuery view/stored procedure.
*   **Reasoning**: 
    *   *Airflow Macros* represent the strategic target state, enabling idempotent, historical backfills by tying date calculations to the DAG's `logical_date`.
    *   *BigQuery Views/Procedures* provide a tactical, low-risk migration path for downstream SQL scripts that cannot yet be fully refactored into parameterized Airflow tasks.

### Bug Fix: Centurial Leap Years
The legacy shell script contained a logical bug that failed to recognize centurial leap years divisible by 400 (such as the year 2000) as leap years, resulting in February having 28 days instead of 29. By migrating to native BigQuery and Python date engines, this bug is fully resolved.

---

## 4. Manual Steps Before Go-Live

To deploy and run the migrated utility successfully, complete the following manual steps:

### 1. Schema & Dataset Creation
Ensure that the target BigQuery dataset exists in your designated GCP region (e.g., `europe-west3`):
```bash
bq mk --dataset --location=europe-west3 your_project_id:isbert_aufbereitung
```

### 2. IAM & Permissions
Ensure that the service account running Cloud Composer / Airflow has the following IAM roles:
*   `roles/bigquery.dataEditor` (on the `isbert_aufbereitung` dataset)
*   `roles/bigquery.jobUser` (on the project level to execute queries)

### 3. Deploy BigQuery Artifacts
Execute the SQL script to deploy the view and stored procedure:
```bash
bq query --use_legacy_sql=false < src/bigquery/isbert_aufbereitung/v_reporting_dates.sql
```

### 4. Deploy Airflow DAG
Copy the generated DAG file to your Cloud Composer DAGs bucket:
```bash
gsutil cp src/dags/isbert_reporting_dag.py gs://<your-composer-bucket>/dags/
```

### 5. Scheduling Alignment
Verify that the Airflow DAG schedule (`@daily` or custom cron) aligns with the legacy UC4 execution window. The DAG should be scheduled to run after the daily source data load is complete.

---

## 5. Known Gaps & Unresolved References

### 1. Upstream Wrapper Auditing
*   **Gap**: The calling parent scripts (the wrappers that executed `gestern.ksh` and parsed its standard output) must be identified and updated.
*   **Remediation**: For every legacy wrapper script migrated to GCP, replace shell-based parsing of `gestern.ksh` with either:
    *   Direct references to the `isbert_aufbereitung.v_reporting_dates` view.
    *   Airflow task parameterization using the Jinja macros demonstrated in `isbert_reporting_dag.py`.

### 2. Idempotency vs. Real-Time Execution
*   **Gap**: The BigQuery view `v_reporting_dates` uses `CURRENT_DATE()`, which evaluates to the real-world execution time. This is not idempotent and will fail to produce historical dates during backfills.
*   **Remediation (Redesign B4)**: For historical backfills, downstream jobs must be refactored to accept date parameters passed from Airflow's `logical_date` rather than querying the live view.

---

## 6. Validation

To validate that the migrated logic matches the legacy output, execute the following tests:

### Test 1: BigQuery View Validation
Run the following query in the BigQuery console:
```sql
SELECT * FROM `isbert_aufbereitung.v_reporting_dates`;
```
*   **Expected Output**: A single row containing four columns:
    *   `Var_Datum_Heute`: Current date in Germany (`YYYYMMDD`)
    *   `Var_Datum_Gestern`: Yesterday's date in Germany (`YYYYMMDD`)
    *   `Var_Monat_Heute`: Current month (`YYYYMM`)
    *   `Var_Monat_Gestern`: Month of yesterday's date (`YYYYMM`)

### Test 2: Stored Procedure Validation
Run the following block in the BigQuery console:
```sql
DECLARE heute, gestern, m_heute, m_gestern STRING;
CALL `isbert_aufbereitung.get_reporting_dates`(heute, gestern, m_heute, m_gestern);
SELECT heute AS heute, gestern AS gestern, m_heute AS m_heute, m_gestern AS m_gestern;
```
*   **Expected Output**: Identical string values to Test 1.

### Test 3: Airflow Macro Dry-Run
1. Navigate to the Airflow UI.
2. Trigger a manual run of `isbert_reporting_dates_dag`.
3. Once the task `run_reporting_job` completes, click on the task, select **Rendered Templates**, and verify that the Jinja expressions have compiled into the correct date strings matching today's and yesterday's dates.

---

## 7. Rollback Procedure

If issues are detected post-go-live, execute the following steps to revert to the legacy execution state:

1.  **Pause Airflow DAG**: Go to the Airflow UI and toggle the switch for `isbert_reporting_dates_dag` to **Off**.
2.  **Re-enable Legacy Scheduling**: Re-activate the legacy UC4 job stream that triggers the on-premises execution of `gestern.ksh`.
3.  **Revert Downstream References**: If downstream SQL scripts were modified to query `isbert_aufbereitung.v_reporting_dates`, revert those scripts to use their legacy parameter injection mechanisms.
4.  **Clean Up Cloud Artifacts (Optional)**:
    If a clean rollback is required, drop the BigQuery artifacts:
    ```sql
    DROP VIEW IF EXISTS `isbert_aufbereitung.v_reporting_dates`;
    DROP PROCEDURE IF EXISTS `isbert_aufbereitung.get_reporting_dates`;
    ```