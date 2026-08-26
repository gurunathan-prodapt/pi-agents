# Migration Notes: DW.DWH_ABTN_SMART_KUBI

These migration notes detail the transition of the legacy UC4 job **DW.DWH_ABTN_SMART_KUBI** and its associated scripts, wrappers, and environment configurations to Google Cloud Platform (GCP) using **Cloud Composer (Apache Airflow)** and **BigQuery**.

---

## 1. Summary

The legacy UC4 job `DW.DWH_ABTN_SMART_KUBI` was a Unix-based database utility job designed to aggregate monthly transaction data and populate a temporary target table (`DWH$TA_T_SMART_KUBI`). 

### Key Characteristics of the Legacy Job:
* **Dynamic Date Calculation**: It dynamically calculated a reporting month parameter (`MONATSID`) based on the execution date. If run before the 15th of the month, it processed data for the previous month; otherwise, it processed the current month.
* **Execution Wrapper**: It sourced an environment initialization profile (`.dw_init`) and executed an Oracle PL/SQL anonymous block (`d_abtn_x_smart_kubi.sql`) via a generic KornShell runner (`r_sqlscript`) and SQL*Plus wrappers (`h_alis_sqlplus.ksh`, `f_alis_msgerr.ksh`).
* **Scheduling**: The job was externally triggered (no native UC4 calendar schedule was defined in the source bundle).

### Target Platform:
* **Orchestration**: Google Cloud Composer (Apache Airflow 2.x).
* **Database & Compute**: Google BigQuery (Standard SQL Scripting).
* **Transitional Compatibility**: Python 3.x modules are provided to replicate legacy shell wrappers (`r_sqlscript`, `h_alis_sqlplus`, `f_alis_msgerr`) for hybrid environments still requiring Oracle database connectivity.

---

## 2. Generated Artifacts

The migration process generated the following files, each playing a specific role in the target architecture:

| Generated File | Language | Role / Description |
| :--- | :--- | :--- |
| `dw_dwh_abtn_smart_kubi.py` | Python (Airflow) | The primary Airflow DAG. It calculates the dynamic `MONATSID` using Jinja templates and orchestrates the execution of the BigQuery SQL script. |
| `d_abtn_x_smart_kubi.sql` | BigQuery SQL | The rewritten database transformation script. It replaces Oracle PL/SQL syntax with BigQuery Standard SQL scripting (`DECLARE`, `BEGIN/EXCEPTION`, ANSI joins, and native `TRUNCATE`). |
| `f_alis_msgerr.py` | Python 3 | A native Python module replacing `f_alis_msgerr.ksh`. It provides functional interfaces for legacy tracking, sequence generation, and status logging. |
| `h_alis_sqlplus.py` | Python 3 | A native Python module replacing `h_alis_sqlplus.ksh`. It validates SQL script readability and wraps external SQL*Plus executions. |
| `r_sqlscript.py` | Python 3 | A native Python runner replacing the `r_sqlscript` shell script. It parses command-line arguments, resolves relative paths, and imports logging/execution functions natively. |

---

## 3. Key Design Decisions

### Direct BigQuery Scripting over Python Wrappers
* **Decision**: The primary BigQuery migration path executes `d_abtn_x_smart_kubi.sql` directly using BigQuery's native scripting capabilities rather than wrapping the SQL execution inside a complex Python subprocess.
* **Reasoning**: This minimizes execution latency, leverages BigQuery's native query engine, and simplifies error handling by utilizing BigQuery's standard `BEGIN...EXCEPTION` blocks.

### ANSI Join Conversion
* **Decision**: Converted Oracle's proprietary implicit outer join syntax `(+)` to standard ANSI `LEFT JOIN` syntax.
* **Reasoning**: BigQuery does not support the `(+)` operator. Standardizing the joins ensures compatibility and improves query readability.

### Declarative Partition Pruning
* **Decision**: Stripped the Oracle-specific partition decorator `partition(dwh$ta_f_d1_twvv_tn_&1)` from the source query.
* **Reasoning**: BigQuery handles partition pruning automatically. By applying the filter `FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)` in the `WHERE` clause, BigQuery will scan only the required partitions, provided the table is partitioned on `gueltigkeitszeitpunkt`.

### Retirement of `.dw_init`
* **Decision**: The environment initialization script `.dw_init` was retired.
* **Reasoning**: Hardcoded directory paths and Oracle client configurations are obsolete in a cloud-native environment. Global variables (such as GCS bucket names and GCP project IDs) are managed natively via Airflow Variables and Environment Variables.

### Native Python Imports for Legacy Wrappers
* **Decision**: In the transitional Python modules (`r_sqlscript.py` and `h_alis_sqlplus.py`), legacy shell-based subprocess calls to sibling scripts were replaced with native Python imports.
* **Reasoning**: This prevents runtime `FileNotFoundError`s and allows the scripts to share memory and execution states cleanly.

---

## 4. Manual Steps Before Go-Live

Before deploying the migrated DAG and executing the pipeline, the following manual setup steps must be completed:

### 1. Schema and Dataset Creation
Ensure the target BigQuery dataset and tables are created in your GCP project:
* **Dataset**: `dwh`
* **Tables**:
  * `dwh.dwh$ta_t_smart_kubi` (Target table)
  * `dwh.dwh$vi_l_map_fa_tarif` (Source view/table)
  * `dwh.bl_d_tarif` (Source table)
  * `dwh.dwh$ta_f_d1_twvv_tn` (Source transaction table)
  * `dwh.dwh$ta_c_vertrag` (Source contract table)

### 2. Table Partitioning Setup
* **Crucial**: The source table `dwh.dwh$ta_f_d1_twvv_tn` must be configured as a partitioned table in BigQuery, using `gueltigkeitszeitpunkt` as the partitioning column. Failure to do so will result in full table scans and significantly higher query costs.

### 3. IAM & Permissions
The Cloud Composer Service Account must be granted the following IAM roles:
* `roles/bigquery.jobUser` (To run BigQuery jobs)
* `roles/bigquery.dataEditor` (On the `dwh` dataset to truncate and insert data)
* `roles/storage.objectViewer` (On the GCS bucket containing the SQL scripts)

### 4. Airflow Variables
Configure the following Airflow Variables in the Cloud Composer environment:
* `GCP_PROJECT`: The ID of your Google Cloud Project.
* `GCS_BUCKET`: The name of the GCS bucket where SQL scripts are staged (e.g., `my-dwh-migration-bucket`).

### 5. SQL Script Staging
Upload the converted SQL script to GCS:
* **Source**: `d_abtn_x_smart_kubi.sql`
* **Destination**: `gs://<YOUR_GCS_BUCKET>/sql/d_abtn_x_smart_kubi.sql`

### 6. Secrets (For Transitional Oracle Connections Only)
If utilizing the transitional Python modules (`f_alis_msgerr.py`, `h_alis_sqlplus.py`) to connect to an Oracle database:
* Store the Oracle connection string (`DW_ORAUSER`) in **Google Secret Manager**.
* Expose it to the execution environment as an environment variable named `DW_ORAUSER`.

---

## 5. Known Gaps & Unresolved References

### Oracle Package Dependencies (`dwpa_util_skript` & `dwpa_meldung`)
* **Gap**: The legacy script relied on `dwpa_util_skript.runstatement` to truncate tables and `dwpa_meldung.fehler` to log errors to an Oracle-specific metadata table.
* **Resolution**: 
  * Truncation is handled natively via standard BigQuery `TRUNCATE TABLE` DML.
  * Error logging is handled via BigQuery's `EXCEPTION` block, which outputs error details to standard output (captured by Cloud Logging).
* **Follow-up**: If your organization requires centralized audit logging in BigQuery, the `EXCEPTION` block in `d_abtn_x_smart_kubi.sql` must be manually modified to perform an `INSERT` into a centralized logging table.

### Partitioning Strategy Verification
* **Gap**: The legacy code queried partition-specific tables dynamically. In BigQuery, this is replaced by standard date filtering.
* **Follow-up**: Verify that the partitioning field type of `gueltigkeitszeitpunkt` matches the string-based comparison `FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)`. If `gueltigkeitszeitpunkt` is stored as a `TIMESTAMP` or `DATE` instead of `DATETIME`, adjust the SQL casting functions accordingly.

---

## 6. Validation

To validate the migration, execute the following testing steps:

### Step 1: BigQuery Dry Run
Run a dry run of the SQL script in the BigQuery console to verify syntax and estimate bytes scanned:
```sql
-- Declare mock variables for testing
DECLARE l_monats_id INT64 DEFAULT 201509;
DECLARE EintragsNr INT64 DEFAULT 12345;

-- Execute the query body...
```
* **Passing Criteria**: The query compiles successfully with zero syntax errors, and the estimated bytes scanned is minimal (confirming partition pruning is active).

### Step 2: Manual DAG Execution
Trigger the Airflow DAG manually via the Airflow UI or CLI:
```bash
gcloud composer environments run <ENVIRONMENT_NAME> \
    --location <LOCATION> \
    dags trigger -- dw_dwh_abtn_smart_kubi
```

### Step 3: Log Verification
Inspect the task logs in Cloud Logging / Airflow UI:
* Verify that the calculated reporting month is printed correctly:
  `Berichtsmonat: YYYYMM`
* Verify that the row count output is logged:
  `X rows inserted in DWH$TA_T_SMART_KUBI`

### Step 4: Data Reconciliation
Run a comparison query between the legacy Oracle target table and the new BigQuery target table for a specific month (e.g., `201509`):
```sql
SELECT monats_id, COUNT(*), SUM(anzahl) 
FROM `dwh.dwh$ta_t_smart_kubi` 
GROUP BY monats_id;
```
* **Passing Criteria**: Row counts and aggregated `anzahl` values match the legacy Oracle database exactly.

---

## 7. Rollback Procedure

In the event of an unrecoverable failure or data discrepancy post-go-live, execute the following rollback steps:

1. **Pause the Airflow DAG**:
   Disable the DAG in the Airflow UI or via the CLI to prevent further executions:
   ```bash
   gcloud composer environments run <ENVIRONMENT_NAME> \
       --location <LOCATION> \
       dags pause -- dw_dwh_abtn_smart_kubi
   ```
2. **Re-enable the UC4 Job**:
   In the UC4 client, locate the job `DW.DWH_ABTN_SMART_KUBI` and set its status to **Active=1**.
3. **Revert Downstream Consumers**:
   If downstream jobs were migrated to read from BigQuery, point them back to the legacy Oracle database tables.
4. **Investigate Logs**:
   Analyze Cloud Composer task logs and BigQuery job history to identify the root cause of the failure.