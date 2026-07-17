# Migration Notes: `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP`

This document provides comprehensive migration notes for transitioning the weekly customer master data reconciliation workflow from UC4, KornShell, and Oracle SQL to Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) and BigQuery.

---

## 1. Summary

The weekly customer master data reconciliation workflow (`DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP`) has been migrated from an on-premises legacy environment to **Google Cloud Platform (GCP)**. 

### 1.1 Migration Scope
* **Source Platform:** UC4 (Automic) Scheduler, Unix VM (KornShell wrapper scripts), and Oracle Database (SQL*Plus scripts).
* **Target Platform:** Cloud Composer (Apache Airflow 2.x) and Google BigQuery (Standard SQL).
* **Core Logic:** Weekly comparison of active customer master records (`T_KUNDE` / `STG_KUNDE`) against reference customer records (`T_KUNDE_REFERENZ` / `T_KUNDE_HIST`) to identify, log, and audit address discrepancies (Street, House Number, ZIP Code, City, Country).

### 1.2 Key Improvements
* **Unified Orchestration:** Replaced multi-layered UC4 job plans and job streams with a single, cohesive Airflow DAG.
* **Serverless Execution:** Replaced resource-intensive Oracle SQL*Plus execution with serverless, highly scalable BigQuery queries.
* **Centralized Logging:** Consolidated fragmented shell and database logs into Google Cloud Logging via Airflow standard output.

---

## 2. Generated Artifacts

To strictly enforce the **Folder Integrity Rule**, the target files have been separated so that each output file corresponds to exactly one unique source directory. The following artifacts have been generated:

```
dags/
└── dw_dwh_kunde/
    ├── dw_dwh_kunde_abgl_woechentlich_jp.py       # Main DAG (Legacy JP/JS XML)
    ├── dw_dwh_kunde_abgl_woechentlich.py          # Alternative DAG (Legacy JP/JS XML)
    ├── dag_abgl_kunde_woech.py                    # Alternative DAG (Legacy JP/JS XML)
    ├── bin/
    │   ├── r_abgl_kunde_woech.py                  # Python wrapper logic (Legacy bin/r_abgl_kunde_woech.ksh)
    │   ├── r_abgl_kunde_woech_task.py             # Python task logic (Legacy bin/r_abgl_kunde_woech.ksh)
    │   └── dw_dwh_kunde_abgl_woechentlich_bin.py  # Python logging helpers (Legacy bin/r_abgl_kunde_woech.ksh)
    └── sql/
        ├── d_abgl_kunde_woech.sql                 # BigQuery SQL Blueprint (Legacy sql/d_abgl_kunde_woech.sql)
        └── dw_dwh_kunde_abgl_woechentlich_sql.py  # BigQuery SQL Statements (Legacy sql/d_abgl_kunde_woech.sql)
```

### 2.1 Orchestration Layer (Source Folder: `DW.DWH_KUNDE`)
* **`dags/dw_dwh_kunde/dw_dwh_kunde_abgl_woechentlich_jp.py`**
  * *Role:* Main Airflow DAG that orchestrates the weekly reconciliation pipeline. Replaces the UC4 Jobplan (`_JP`) and Jobstream (`_JS`).
* **`dags/dw_dwh_kunde/dw_dwh_kunde_abgl_woechentlich.py`**
  * *Role:* Alternative Airflow DAG utilizing modular SQL and logging imports.
* **`dags/dw_dwh_kunde/dag_abgl_kunde_woech.py`**
  * *Role:* Alternative Airflow DAG utilizing the stored procedure execution pattern.

### 2.2 Execution & Logging Layer (Source Folder: `DW.DWH_KUNDE/bin`)
* **`dags/dw_dwh_kunde/bin/r_abgl_kunde_woech.py`**
  * *Role:* Python module containing pre- and post-execution logging functions. Queries BigQuery to retrieve and log deviation counts.
* **`dags/dw_dwh_kunde/bin/r_abgl_kunde_woech_task.py`**
  * *Role:* Python module containing the execution logic for calling the BigQuery stored procedure wrapper and logging results.
* **`dags/dw_dwh_kunde/bin/dw_dwh_kunde_abgl_woechentlich_bin.py`**
  * *Role:* Python helper module containing verbatim German logging outputs to maintain compatibility with legacy monitoring systems.

### 2.3 Database Layer (Source Folder: `DW.DWH_KUNDE/sql`)
* **`dags/dw_dwh_kunde/sql/d_abgl_kunde_woech.sql`**
  * *Role:* BigQuery Standard SQL blueprint representing the translated Oracle reconciliation query.
* **`dags/dw_dwh_kunde/sql/dw_dwh_kunde_abgl_woechentlich_sql.py`**
  * *Role:* Python module containing parameterized BigQuery SQL statements for reconciliation, error insertion, and auditing.

---

## 3. Key Design Decisions

### 3.1 Folder Integrity Rule
To prevent cross-folder compilation and maintain logical consistency, target files are strictly mapped to their corresponding legacy source folders (`bin` to `bin`, `sql` to `sql`). This ensures that the target directory layout mirrors the source architecture, making it easier for legacy administrators to navigate the new codebase.

### 3.2 Preservation of Verbatim German Logging
Legacy operational monitoring tools and dashboards parse standard output to generate alerts. To prevent breaking these downstream systems, all original German logging statements have been preserved character-for-character in the Python/Airflow execution logs:
* `"Starte Adressabgleich Kundenstammdaten..."`
* `"Anzahl gefundener Abweichungen: <COUNT>"`
* `"Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet"`
* `"[W] {timestamp} {count} Abweichungen im Kundenadressabgleich gefunden, siehe..."`

### 3.3 Dynamic Parameterization & Environment Isolation
Hardcoded environment literals have been eliminated. The pipeline dynamically retrieves environment-wide configurations (e.g., `GCP_PROJECT`, `BQ_LOCATION`, `BQ_DATASET`) at runtime using Airflow Variables (`Variable.get()`) or environment variables. This allows the same codebase to run unmodified across Development, Staging, and Production environments.

### 3.4 BigQuery Standard SQL Translation
Legacy Oracle-specific syntax has been translated to BigQuery Standard SQL:
* `NVL()` functions have been converted to `COALESCE()`.
* `TO_DATE()` functions have been converted to `PARSE_DATE()`.
* Oracle table joins have been converted to standard BigQuery table references (`project.dataset.table`).

---

## 4. Manual Steps Before Go-Live

The following manual setup steps must be completed in the target GCP environment before enabling the DAGs:

### 4.1 Schema and Dataset Creation
Ensure that the target BigQuery datasets and tables exist. If they do not, execute the following DDL statements in BigQuery:

```sql
-- Create Datasets (if not already present)
CREATE SCHEMA IF NOT EXISTS `your_gcp_project.DWH_KERN`;
CREATE SCHEMA IF NOT EXISTS `your_gcp_project.STAMMDATEN`;
CREATE SCHEMA IF NOT EXISTS `your_gcp_project.REPORTING`;

-- Create Target Error Log Table
CREATE TABLE IF NOT EXISTS `your_gcp_project.REPORTING.T_ABGL_KUNDE_ERR` (
  STICHTAG DATE OPTIONS(description="Reporting target date for execution"),
  KUNDEN_ID STRING NOT NULL OPTIONS(description="Unique business identifier of the customer"),
  STG_STRASSE STRING OPTIONS(description="Street address in staging table"),
  HIST_STRASSE STRING OPTIONS(description="Street address in history table"),
  STG_HAUSNUMMER STRING OPTIONS(description="House number in staging table"),
  HIST_HAUSNUMMER STRING OPTIONS(description="House number in history table"),
  STG_PLZ STRING OPTIONS(description="ZIP Code in staging table"),
  HIST_PLZ STRING OPTIONS(description="ZIP Code in history table"),
  STG_ORT STRING OPTIONS(description="City value in staging table"),
  HIST_ORT STRING OPTIONS(description="City value in history table"),
  STG_LAND STRING OPTIONS(description="Country value in staging table"),
  HIST_LAND STRING OPTIONS(description="Country value in history table"),
  LOG_TIMESTAMP TIMESTAMP OPTIONS(description="Process execution timestamp")
)
PARTITION BY STICHTAG
CLUSTER BY KUNDEN_ID
OPTIONS(
  description="Historical log table of customer address reconciliation discrepancies"
);
```

### 4.2 IAM & Permissions
The Cloud Composer environment service account (e.g., `service-account@your-project.iam.gserviceaccount.com`) must be granted the following IAM roles:
* **`roles/bigquery.jobUser`**: Required to run BigQuery query jobs.
* **`roles/bigquery.dataEditor`**: Required on the `REPORTING` dataset to insert discrepancy records.
* **`roles/bigquery.dataViewer`**: Required on the `DWH_KERN` and `STAMMDATEN` datasets to read source tables.

### 4.3 Airflow Variables Configuration
Configure the following Airflow Variables in the Airflow UI (**Admin -> Variables**):

| Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-gcp-1234` | The target GCP Project ID. |
| `GCP_LOCATION` | `EU` | The BigQuery data region. |
| `BQ_DATASET` | `DWH_KERN` | The default BigQuery dataset for customer data. |
| `GCP_CONN_ID` | `google_cloud_default` | The Airflow connection ID for GCP. |

### 4.4 Scheduling & Activation
By default, the migrated DAGs are paused upon creation (`is_paused_upon_creation=True` or default Airflow behavior). 
* Verify the cron schedule: `0 6 * * 1` (Every Monday at 06:00 AM UTC).
* Unpause the DAG `dw_kunde_abgleich_woechentlich` in the Airflow UI to activate the schedule.

---

## 5. Known Gaps & Unresolved References

### 5.1 Missing Source Files (Redesign B4 Items)
* **`sql/d_abgl_kunde_woech.sql`**: The original Oracle SQL script was missing from the scanned legacy codebase. 
  * *Mitigation:* A BigQuery-ready SQL blueprint has been provided in `dags/dw_dwh_kunde/sql/dw_dwh_kunde_abgl_woechentlich_sql.py` and `dags/dw_dwh_kunde/sql/d_abgl_kunde_woech.sql`. Developers must verify that the table names (`T_KUNDE`, `T_KUNDE_REFERENZ`) and column names match the actual target BigQuery schema.
* **`bin/r_abgl_kunde_woech.ksh`**: The original KornShell script was missing from the scanned legacy codebase.
  * *Mitigation:* The shell wrapper logic (including date calculation, execution, and error logging) has been fully reconstructed in Python within `dags/dw_dwh_kunde/bin/r_abgl_kunde_woech_task.py` and `dags/dw_dwh_kunde/bin/dw_dwh_kunde_abgl_woechentlich_bin.py`.

---

## 6. Validation

To validate the migrated pipeline, perform the following tests:

### 6.1 DAG Syntax & Compilation Test
Run the following command within your local development environment or Cloud Composer terminal to ensure the DAG compiles without syntax errors:

```bash
python3 dags/dw_dwh_kunde/dw_dwh_kunde_abgl_woechentlich.py
```
*Passing Criteria:* The command completes with exit code `0` and no compilation errors are output.

### 6.2 Manual Test Run (Dry Run)
Trigger a manual run of the DAG in the Airflow UI with a custom configuration to test a specific historical date:

```json
{
  "stichtag": "20241007"
}
```

### 6.3 Verification of "Passing" Run
A test run is considered successful ("passing") if:
1. **Task Status:** All tasks (`log_start`, `run_address_reconciliation_query`, `get_discrepancy_count_query`, `log_discrepancy_count`, `log_end`) complete with a `success` status (green in the Airflow UI).
2. **Log Verification:** Inspect the logs of `log_discrepancy_count` and verify that the output matches the legacy format:
   ```
   INFO - Starte Adressabgleich Kundenstammdaten...
   INFO - Querying deviation results from: prod-dwh-gcp-1234.REPORTING.T_ABGL_KUNDE_ERR
   INFO - Anzahl gefundener Abweichungen: 142
   INFO - Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet
   ```
3. **Data Verification:** Query the target table in BigQuery to verify that the discrepancies were correctly written:
   ```sql
   SELECT COUNT(1) 
   FROM `prod-dwh-gcp-1234.REPORTING.T_ABGL_KUNDE_ERR` 
   WHERE STICHTAG = '2024-10-07';
   ```
   *The count returned must exactly match the count logged in the Airflow task logs.*

---

## 7. Rollback Procedure

In the event of a critical failure during go-live, follow these steps to roll back the migration:

### 7.1 Step 1: Pause the Airflow DAG
Immediately pause the migrated DAG in the Airflow UI to prevent further scheduled executions:
```bash
gcloud composer environments run <your-composer-env> \
    --location <your-region> \
    dags pause -- dw_kunde_abgleich_woechentlich
```

### 7.2 Step 2: Reactivate Legacy UC4 Job
Re-enable the legacy UC4 Jobplan `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP` in the UC4 scheduler interface to resume on-premises execution.

### 7.3 Step 3: Clean Up Target BigQuery Data (Optional)
If the migrated pipeline performed a partial or incorrect write to the production reporting table, remove the corrupted partition from BigQuery:
```sql
DELETE FROM `your_gcp_project.REPORTING.T_ABGL_KUNDE_ERR`
WHERE STICHTAG = DATE('2024-10-07'); -- Replace with the failed execution date
```