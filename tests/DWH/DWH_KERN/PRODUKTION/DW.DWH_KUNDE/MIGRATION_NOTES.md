# Migration Notes: DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS

## 1. Summary
The legacy UC4 job `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS`, its corresponding KornShell wrapper script `r_abgl_kunde_woech.ksh`, and the underlying Oracle SQL logic `d_abgl_kunde_woech.sql` have been migrated to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**.

*   **Source Platform**: UC4 Scheduler, KornShell (KSH) on Unix, Oracle Database.
*   **Target Platform**: Google Cloud Composer (Apache Airflow), BigQuery Standard SQL.
*   **Migration Pattern**: `UC4+KSH+SQL_MEDIUM` (High Confidence).
*   **Functional Scope**: Performs a weekly reconciliation of customer master address records (`DWH_KERN.T_KUNDE`) against reference master data (`STAMMDATEN.T_KUNDE_REFERENZ`) to identify discrepancies in postal codes, cities, or streets.

---

## 2. Generated Artifacts
The following files have been generated and structured to mirror the source repository layout:

| Target File Path | Role / Description |
| :--- | :--- |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/d_abgl_kunde_woech_dag.py` | **Airflow Orchestration DAG**: Defines the weekly schedule, execution metadata, and triggers the reconciliation task. |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/d_abgl_kunde_woech_bin.py` | **Execution Logic Wrapper**: Migrated Python module replacing the KornShell script. Handles BigQuery client initialization, parameter binding, execution, and log parsing. |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/sql/d_abgl_kunde_woech.sql` | **BigQuery SQL Script**: Converted standard SQL query that performs the actual address comparison, replacing Oracle-specific syntax. |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/utils.py` | **Shared Utilities**: Provides reusable logging wrappers (`f_alis_msgerr`) and generic BigQuery client adapters. |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/dw_dwh_kunde_abgl_woechentlich.py` | **Alternative Airflow DAG**: Legacy-aligned DAG wrapper utilizing the command-line execution pattern. |
| `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_KUNDE/bin/r_abgl_kunde_woech.py` | **Alternative CLI Wrapper**: Direct Python translation of the original shell script for standalone execution. |

---

## 3. Key Design Decisions

### 3.1 Orchestration & Execution Pattern
*   **Cloud Composer (Airflow)**: Chosen to replace UC4. It provides native scheduling, dependency management, and deep integration with BigQuery.
*   **Python-Native Execution**: The KornShell script was migrated to Python (`d_abgl_kunde_woech_bin.py`) rather than running bash commands in Airflow. This allows robust error handling, native GCP SDK integration, and cleaner unit testing.

### 3.2 Strict Output & Print Literal Preservation
To prevent breaking downstream log parsers, automated alerting systems, or operational runbooks, **all original German and English output, print, and warning messages are preserved character-for-character**:
*   *XML Print:* `Kundenadressabgleich fuer Lauf &LAUF_WOCHE angestossen`
*   *KSH Echo 1:* `Starte Adressabgleich Kundenstammdaten fuer Stichtag $l_Stichtag`
*   *KSH Echo 2:* `Anzahl gefundener Abweichungen: $l_Abweichungen`
*   *KSH Echo 3:* `Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet`
*   *KSH Warning:* `$l_Abweichungen Abweichungen im Kundenadressabgleich gefunden, siehe $Protokoll_Datei`

### 3.3 SQL Dialect Translation
*   **Oracle to BigQuery Standard SQL**: Oracle-specific functions were mapped to BigQuery equivalents:
    *   `NVL(...)` $\rightarrow$ `COALESCE(...)`
    *   `TO_DATE(..., 'YYYYMMDD')` $\rightarrow$ `PARSE_DATE('%Y%m%d', ...)`
*   **Parameterized Queries**: The `stichtag` parameter is passed dynamically using BigQuery's safe scalar query parameters (`@p_Stichtag`) to prevent SQL injection and leverage execution plan caching.

---

## 4. Manual Steps Before Go-Live

### 4.1 Schema & Dataset Creation
Ensure the target BigQuery datasets and tables exist in your GCP project:
1.  **Datasets**: Create `DWH_KERN` and `STAMMDATEN` in your target region (e.g., `EU` or `US`).
2.  **Tables**:
    *   `DWH_KERN.T_KUNDE` (Must contain columns: `KUNDE`, `NACHNAME`, `VORNAME`, `PLZ`, `ORT`, `STRASSE`, `AKTUALISIERT_AM`).
    *   `STAMMDATEN.T_KUNDE_REFERENZ` (Must contain columns: `KUNDE`, `PLZ`, `ORT`, `STRASSE`).

### 4.2 IAM & Permissions
The service account running the Cloud Composer workers must have the following IAM roles:
*   `roles/bigquery.jobUser` (To run query jobs).
*   `roles/bigquery.dataViewer` on datasets `DWH_KERN` and `STAMMDATEN`.
*   `roles/storage.objectAdmin` on the Cloud Composer GCS bucket (for writing log protocols).

### 4.3 Connection Strings & Airflow Variables
Configure the following variables in the Airflow UI (**Admin -> Variables**):
*   `GCS_BUCKET`: The name of the GCS bucket where reconciliation logs will be archived (e.g., `my-dwh-reconciliation-logs`).

Configure the following environment variable in your Cloud Composer environment:
*   `GCP_PROJECT`: Your target Google Cloud Project ID.

### 4.4 Scheduling
The DAG is configured to run weekly on Mondays at 06:00 AM (`0 6 * * 1`). If this job must wait for an upstream data load, ensure the upstream DAG is either:
*   Configured to trigger this DAG via `TriggerDagRunOperator`.
*   Monitored using an `ExternalTaskSensor`.

---

## 5. Known Gaps & Unresolved References

### 5.1 Upstream Trigger Mechanism
The legacy system triggered this job via the parent plan `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP.xml`. 
*   **Status**: Unresolved.
*   **Action Required**: Once the parent plan is migrated to Airflow, establish the cross-DAG dependency using a `TriggerDagRunOperator` or Airflow `Datasets`.

### 5.2 Log File Location Divergence
*   **Gap**: The legacy script printed a warning pointing to a local Unix path: `siehe $Protokoll_Datei` (e.g., `/tmp/aktuell/log/...`).
*   **Resolution**: The migrated script writes the protocol to Google Cloud Storage (`gs://<GCS_BUCKET>/logs/...`) and prints this GCS URI in the log. Operations teams must be notified that logs are now stored in GCS instead of a local filesystem.

---

## 6. Validation

### 6.1 How to Run the Tests
1.  **Dry Run (Airflow CLI)**:
    Render the task templates to verify parameter binding:
    ```bash
    airflow tasks render DW_DWH_KUNDE_ABGL_WOECHENTLICH_JS run_reconciliation 2026-01-05
    ```
2.  **Manual Execution**:
    Trigger the DAG manually from the Airflow UI with a specific configuration JSON:
    ```json
    {"ds_nodash": "20260105"}
    ```

### 6.2 What "Passing" Means
The run is successful if:
*   The Airflow task completes with a `SUCCESS` status.
*   The task log contains the verbatim start message:
    `Starte Adressabgleich Kundenstammdaten fuer Stichtag 20260105`
*   The log outputs the discrepancy count:
    `Anzahl gefundener Abweichungen: <count>`
*   If discrepancies exist ($>0$), the log prints the warning containing the GCS path and lists the mismatched records.
*   The final line of the log reads:
    `Adressabgleich Kundenstammdaten ohne erkennbare Fehler beendet`

---

## 7. Rollback Procedure

In the event of a critical failure on the target platform:

1.  **Pause the Airflow DAG**:
    ```bash
    airflow dags pause DW_DWH_KUNDE_ABGL_WOECHENTLICH_JS
    ```
2.  **Re-enable Legacy Scheduling**:
    Resume the active schedule for `DW.DWH_KUNDE_ABGL_WOECHENTLICH_JS` in the UC4 controller.
3.  **Verify Database State**:
    The reconciliation job is read-only and does not modify data in `DWH_KERN` or `STAMMDATEN`. Therefore, no database state rollback or table restoration is required.