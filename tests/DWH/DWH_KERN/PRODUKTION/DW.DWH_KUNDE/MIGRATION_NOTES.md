# Migration Notes: Weekly Customer Address Alignment Workflow
**Source Job Plan**: `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP.xml`  
**Target Platform**: Google Cloud Platform (Cloud Composer / BigQuery / Dataform)

---

## 1. Summary
This document details the migration of the weekly customer address alignment and anomaly detection workflow (`DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP`) from its legacy Automic/UC4 and Oracle SQL*Plus environment to Google Cloud Platform.

### Legacy Architecture
*   **Orchestration**: Automic/UC4 Job Plan (`_JP`) and Job Scheduler (`_JS`) objects.
*   **Execution Wrapper**: A KornShell script (`r_abgl_kunde_woech.ksh`) managing parameters, logging, and execution.
*   **Transformation Engine**: Oracle SQL*Plus script (`d_abgl_kunde_woech.sql`) executing comparison queries on Oracle.

### Target Architecture
*   **Orchestration**: Apache Airflow 2.x (Google Cloud Composer) DAGs.
*   **Transformation Engine**: BigQuery SQL executed natively via Cloud Dataform (SQLX) or parameterized BigQuery client calls.
*   **Logging & Alerts**: Python-native logging within Airflow tasks, preserving legacy German log outputs character-for-character.

---

## 2. Generated Artifacts

The following target files have been generated to replace the legacy components, strictly adhering to the repository folder-integrity rules:

| Target File Path | Role / Description |
| :--- | :--- |
| `dags/dw_dwh_kunde_abgl_woechentlich.py` | **Primary Airflow DAG**: Orchestrates the weekly execution schedule, handles dynamic date calculations, and triggers the Dataform reconciliation run. |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/dag_abgl_kunde_woech.py` | **Mirrored Orchestration DAG**: Replicates the parent UC4 Job Plan (`_JP`) structure in the mirrored legacy folder path. |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/dag_abgl_kunde_woech_js.py` | **Mirrored Sequence DAG**: Replicates the child UC4 Job Scheduler (`_JS`) structure in the mirrored legacy folder path. |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/dag_abgl_kunde_woech_bin.py` | **Python Action Module**: Replaces the KornShell wrapper logic (`r_abgl_kunde_woech.ksh`). Handles anomaly evaluation, branching, and exact-match logging. |
| `bin/r_abgl_kunde_woech.py` | **Standalone Python Runner**: A direct Python port of the legacy shell script, allowing independent execution of the BigQuery reconciliation query. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sqlx` | **Dataform Model (SQLX)**: Incremental table definition that compares customer master data against reference data and stores mismatches. |
| `sql/d_abgl_kunde_woech.sql` | **BigQuery SQL Template**: Parameterized SQL script used by the standalone Python runner to validate addresses. |

---

## 3. Key Design Decisions

### 1. Mirrored Folder Layout vs. Unified DAGs
To satisfy both strict repository folder-integrity rules and modern Airflow deployment patterns, a dual-delivery approach was implemented:
*   **Mirrored Layout**: The exact directory structure of the legacy DWH is preserved under `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/`. This ensures that configuration-controlled paths remain valid and traceable.
*   **Unified DAGs**: Clean, root-level DAGs are provided for standard Cloud Composer deployments where deep nested structures are undesirable.

### 2. Character-for-Character Log Preservation
The legacy system relied on automated log-scraping tools to trigger downstream alerts. To prevent breaking these operational monitors, all log outputs are preserved verbatim in German:
*   **Start Message**: `"Starte Adressabgleich Kundenstammdaten fuer Stichtag {l_Stichtag}"`
*   **Warning Message**: `"[W] {l_Abweichungen} Abweichungen im Kundenadressabgleich gefunden, siehe {Protokoll_Datei}"`
*   **Completion Message**: `"Kundenadressabgleich fuer Lauf {LAUF_WOCHE} angestossen"`

### 3. Incremental Partitioning in Dataform
The Dataform model (`d_abgl_kunde_woech.sqlx`) is configured as an `incremental` table partitioned by `stichtag` (reporting date). This ensures:
*   **Idempotency**: The `pre_operations` block deletes existing records for the target partition before execution, preventing duplicate entries during retries.
*   **Performance**: BigQuery only scans and writes to the partition matching the execution date.

---

## 4. Manual Steps Before Go-Live

### 1. BigQuery Schema & Dataset Creation
Ensure that the target datasets exist in your BigQuery project. If they do not, create them using the following commands:

```bash
# Create the core DWH dataset
bq mk --dataset --location=europe-west3 my-gcp-project:dw_dwh_kunde

# Create the work/audit dataset
bq mk --dataset --location=europe-west3 my-gcp-project:work
```

### 2. IAM & Permissions
The Cloud Composer / Airflow worker Service Account must be granted the following IAM roles:
*   `roles/bigquery.jobUser` (To run queries and Dataform compilations)
*   `roles/bigquery.dataEditor` on the `work` and `dw_dwh_kunde` datasets
*   `roles/bigquery.dataViewer` on the `DWH_KERN` and `STAMMDATEN` source datasets
*   `roles/storage.objectAdmin` on the GCS log bucket

### 3. Airflow Variables Setup
Import or manually configure the following Airflow Variables in the Composer UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `gcp-production-data-project` | Target Google Cloud Project ID. |
| `GCP_LOCATION` | `europe-west3` | Target GCP Region for Dataform and BigQuery. |
| `GCS_LOG_BUCKET` | `gcp-production-data-project-logs` | GCS Bucket name where execution logs are archived. |
| `dw_dwh_kunde_dataform_repo` | `kunden-master-reconciliations` | Name of the Cloud Dataform repository. |
| `BQ_DATASET_DWH_KERN` | `dw_dwh_kunde` | Target BigQuery dataset name. |

### 4. Dataform Repository Configuration
1. Create a Dataform repository named `kunden-master-reconciliations` in your GCP project.
2. Link the repository to your Git workspace containing the `definitions/` folder.
3. Ensure the Dataform Service Account has read access to the source tables.

---

## 5. Known Gaps & Unresolved References

### 1. Source Table Availability
The Dataform model references two source tables:
*   `gcp-production-data-project.DWH_KERN.T_KUNDE`
*   `gcp-production-data-project.STAMMDATEN.T_KUNDE_REFERENZ`

These tables must be migrated and populated by their respective upstream pipelines before this weekly alignment job is activated. If these tables are located in a different GCP project, cross-project BigQuery permissions must be configured.

### 2. PII Compliance & Policy Tags
The customer master tables contain Personally Identifiable Information (PII) such as names (`NACHNAME`, `VORNAME`) and addresses (`STRASSE`, `PLZ`, `ORT`). 
*   **Gap**: Policy tags and column-level security are not defined in the migrated SQLX models.
*   **Follow-up**: Apply appropriate BigQuery Policy Tags to these columns in the target schema before loading production data.

---

## 6. Validation

### How to Run the Tests

#### 1. Airflow DAG Dry-Run
Verify that the Airflow DAG compiles without syntax or import errors:

```bash
# Access the Composer worker or local development environment
python3 dags/dw_dwh_kunde_abgl_woechentlich.py
```
*If no output or errors are returned, the DAG is syntactically correct.*

#### 2. Manual DAG Trigger
1. Navigate to the Airflow UI.
2. Locate the DAG `dw_dwh_kunde_abgl_woechentlich`.
3. Click **Trigger DAG w/ Config** and pass a manual execution date:
   ```json
   {
     "stichtag": "20260301"
   }
   ```

### What "Passing" Means
The run is successful if:
1. The task `run_dataform_reconciliation` completes with status `SUCCESS`.
2. The task `check_anomalies` prints the exact legacy log format to the Airflow task logs:
   * Look for: `Starte Adressabgleich Kundenstammdaten fuer Stichtag 20260301`
   * If anomalies exist, look for: `[W] <count> Abweichungen im Kundenadressabgleich gefunden, siehe gs://...`
   * Look for: `Kundenadressabgleich fuer Lauf 20260301 angestossen`
3. The table `work.wrk_kunden_abweichungen` contains the expected mismatch records for partition `2026-03-01`.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during the deployment of this migrated workflow, execute the following steps:

### Step 1: Pause the Airflow DAGs
Immediately disable the weekly schedule in the Airflow UI:
```bash
gcloud composer environments run <composer-env-name> \
    --location <location> \
    dags pause -- dw_dwh_kunde_abgl_woechentlich
```

### Step 2: Revert BigQuery Data (If Required)
If the migration run wrote corrupted or incorrect data to the target work table, drop the affected partition:
```sql
DELETE FROM `gcp-production-data-project.work.wrk_kunden_abweichungen`
WHERE stichtag = DATE('2026-03-01');
```

### Step 3: Reactivate Legacy Scheduling
Re-enable the Automic/UC4 Job Plan `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP` in the legacy environment to ensure weekly business continuity while troubleshooting the GCP pipeline.