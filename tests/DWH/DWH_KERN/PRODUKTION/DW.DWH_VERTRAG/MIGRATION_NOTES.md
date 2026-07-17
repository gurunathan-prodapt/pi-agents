# Migration Notes: Contract and Tariff Synchronization Workflow

This document details the migration of the weekly contract and tariff synchronization job chain (`DW.DWH_VERTRAG_TARIF_SYNC_JP`) from UC4 to Google Cloud Composer (Apache Airflow).

---

## 1. Summary

The legacy UC4 workflow orchestrates a weekly synchronization process of contract and tariff assignments (`TARIF`) between the source system (`Stammdaten`) and the Data Warehouse Core layer (`DWH_KERN`). 

*   **Source Platform:** UC4 (Automic Automation) Job Plan & Script Jobs
*   **Target Platform:** Google Cloud Composer (Apache Airflow)
*   **Migration Strategy:** Direct orchestration translation (`UC4_ONLY` pattern) utilizing Python-based state management to emulate legacy database variable containers.

---

## 2. Generated Artifacts

The migration process generated the following files, preserving the original modular structure of the UC4 includes:

| Target File Path | Role / Purpose |
| :--- | :--- |
| `dags/dwh_dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py` | **Primary DAG File**: Orchestrates the execution flow, defines tasks, and manages state transitions. |
| `dags/dwh_dwh_vertrag/includes/dw_hole_pfad_vtrg.py` | **Environment Helper**: Emulates the legacy `DW.HOLE_PFAD_VTRG` include to resolve system paths. |
| `dags/dwh_dwh_vertrag/includes/dw_lese_log_vtrg.py` | **Logging Helper**: Emulates the legacy `DW.LESE_LOG_VTRG` include, preserving original German log formats. |

---

## 3. Key Design Decisions

### State Management & Locking Emulation
*   **Legacy Approach:** UC4 used an internal database variable container `DW.VARIABLEN_VTRG` with a key `SYNC_STATUS` to act as an application-level lock semaphore.
*   **Airflow Approach:** Emulated using **Airflow Variables** (`dw_variablen_vtrg_sync_status` and `dw_variablen_vtrg_letzter_lauf`). 
*   **Trade-off:** While Airflow Variables are stored in the metadata database and are easy to read/write, rapid back-to-back updates can cause database overhead. Given this is a weekly, low-frequency job, using Airflow Variables is the most transparent and maintainable solution without introducing external caching layers (like Redis).

### Modularization of Includes
*   Instead of flattening the legacy includes (`DW.HOLE_PFAD_VTRG` and `DW.LESE_LOG_VTRG`) directly into the main DAG file, they were partitioned into a dedicated `includes/` subdirectory. This preserves the original code structure, keeps the main DAG clean, and allows other DAGs in the `dwh_dwh_vertrag` namespace to reuse these helpers.

### Branching Logic
*   A `BranchPythonOperator` (`check_and_lock_sync`) evaluates the lock state. If the status is `"GESPERRT"`, it cleanly routes the execution to an `EmptyOperator` (`skip_execution`), preventing downstream tasks from running while avoiding a hard DAG failure.

---

## 4. Manual Steps Before Go-Live

Before deploying and triggering the DAG, the following configuration steps must be completed in the target Airflow environment:

### 1. Airflow Variables Creation
You must initialize the following Airflow Variables in the Airflow UI (**Admin -> Variables**):

| Variable Key | Expected Initial Value | Description |
| :--- | :--- | :--- |
| `dw_variablen_vtrg_sync_status` | `FREI` | Lock semaphore. Allowed values: `FREI`, `LAEUFT`, `GESPERRT`. |
| `dw_variablen_vtrg_letzter_lauf` | `19700101` | Date of the last successful run (format: `YYYYMMDD`). |
| `dw_variablen_dwh_home` | `/opt/dwh` | Path to DWH home directory. |
| `dw_variablen_home` | `/home/dwh` | Path to user home directory. |
| `dw_variablen_pms_home` | `/opt/pms` | Path to PMS home directory. |

### 2. Environment Variables
Ensure the following environment variables are set in your Cloud Composer environment:
*   `GCP_PROJECT`
*   `GCP_REGION`
*   `GCS_BUCKET`

### 3. Scheduling Configuration
The legacy XML files did not contain scheduling rules, resulting in `schedule_interval=None` in the generated DAG. 
*   **Action:** If this job must run weekly (e.g., every Sunday at 03:00 AM), update the DAG definition in `dw_dwh_vertrag_tarif_sync_jp.py` to:
    ```python
    schedule_interval='0 3 * * 0'
    ```

---

## 5. Known Gaps & Unresolved References

### Downstream Workload Placeholder (`B4` Redesign Item)
*   **Gap:** The task `execute_sync_dummy` is currently an `EmptyOperator`. The original UC4 workflow acted as a controller, but the actual data synchronization scripts (e.g., PySpark, SQL scripts, or Dataproc jobs) must be integrated here.
*   **Resolution:** Once the underlying DWH table structures and migration scripts are finalized, replace `execute_sync_dummy` with the appropriate operator (e.g., `DataprocSubmitJobOperator` or `BigQueryOperator`).

---

## 6. Validation

### Local/Development Testing
1. Copy the generated files to your Airflow `dags/` directory, maintaining the folder structure:
   ```bash
   dags/
   ├── dwh_dwh_vertrag/
   │   ├── dw_dwh_vertrag_tarif_sync_jp.py
   │   └── includes/
   │       ├── __init__.py
   │       ├── dw_hole_pfad_vtrg.py
   │       └── dw_lese_log_vtrg.py
   ```
2. Run a syntax and import check:
   ```bash
   python3 dags/dwh_dwh_vertrag/dw_dwh_vertrag_tarif_sync_jp.py
   ```

### Execution Validation Scenarios

#### Scenario A: Successful Run (Lock is Free)
1. Set `dw_variablen_vtrg_sync_status` to `FREI`.
2. Trigger the DAG.
3. **Expected Result:** 
   * `check_and_lock_sync` evaluates to `execute_sync_dummy`.
   * `dw_variablen_vtrg_sync_status` is updated to `LAEUFT` during execution.
   * `execute_sync_dummy` runs successfully.
   * `release_sync_lock` runs, updates `dw_variablen_vtrg_sync_status` back to `FREI`, and updates `dw_variablen_vtrg_letzter_lauf` to the execution date.

#### Scenario B: Skipped Run (Lock is Active)
1. Set `dw_variablen_vtrg_sync_status` to `GESPERRT`.
2. Trigger the DAG.
3. **Expected Result:**
   * `check_and_lock_sync` logs: `Vertrags-/Tarifabgleich fuer <datum> ist gesperrt - Abbruch`.
   * The DAG branches to `skip_execution`.
   * `execute_sync_dummy` and `release_sync_lock` are marked as skipped.

---

## 7. Rollback Procedure

In the event of an execution failure or state corruption:

1. **Reset Lock State:** If the DAG fails mid-run, the lock variable `dw_variablen_vtrg_sync_status` may remain stuck in `LAEUFT`. Manually reset the Airflow Variable `dw_variablen_vtrg_sync_status` to `FREI` via the Airflow UI to allow subsequent runs.
2. **Deactivate DAG:** Pause the DAG in the Airflow UI to prevent scheduled executions while troubleshooting.
3. **Legacy Fallback:** If a fallback to UC4 is required, ensure the Airflow DAG is paused, and manually verify that the legacy UC4 variable container `DW.VARIABLEN_VTRG` is synchronized with the state of the last successful Airflow run.