# Migration Notes: DW.DWH_VVTN_IAR_BGF_GUTSCHR

This document details the migration of the UC4 UNIX job `DW.DWH_VVTN_IAR_BGF_GUTSCHR` to Apache Airflow (Google Cloud Composer).

---

## 1. Summary
The legacy UC4 UNIX job `DW.DWH_VVTN_IAR_BGF_GUTSCHR` has been migrated to an Apache Airflow DAG. 

* **Source Platform:** UC4 / Automic Workload Automation (UNIX Job)
* **Target Platform:** Google Cloud Composer (Apache Airflow)
* **Job Purpose:** Transforms raw "Gutschrift" (credit note) files into a single consolidated CSV file. It initializes environment variables, calculates the previous month's date identifier, and executes a downstream shell script.
* **Target DAG ID:** `dw_dwh_vvtn_iar_bgf_gutschr`

---

## 2. Generated Artifacts
The migration process generated the following file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `DWH_IAR_BGF_GUTSCHRIFT_JOB/dw_dwh_vvtn_iar_bgf_gutschr.py` | Airflow DAG | The Python definition file containing the DAG structure, task configurations, environment variables, and execution logic. |

---

## 3. Key Design Decisions

### Operator Selection
* **Decision:** The legacy `JOBS_UNIX` task was mapped to an Airflow `BashOperator`.
* **Rationale:** The job executes a native shell script (`r_vvtn_iar_bgf_gutschrift`) and sources a local profile (`.dw_init`). Using a `BashOperator` preserves the execution environment and command structure with minimal modification. 
* **Alternative Considered:** An `SSHOperator` could be used if the script must run on a remote legacy VM (e.g., `DWHDWH1P`). The current implementation assumes execution on an Airflow worker with access to the shared filesystem.

### Dynamic Date Calculation
* **Decision:** Replaced the UC4 variable `&LASTMONTH_YYYYMM` with an Airflow Jinja template.
* **Rationale:** To ensure idempotency and support backfilling, the date must be calculated relative to the DAG's logical execution date rather than the current system time.
* **Implementation:** 
  ```python
  {{ (data_interval_end.in_timezone('Europe/Berlin') - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}
  ```

### Literal Output Preservation
* **Decision:** Explicitly added `echo "Lastmonth is $Month_ID"` to the bash command block.
* **Rationale:** This preserves the exact logging behavior of the legacy UC4 `:print Lastmonth is &Month_ID` statement, ensuring log-scraping tools or operators can verify execution parameters.

### Scheduling
* **Decision:** Configured with `schedule=None`.
* **Rationale:** The source UC4 object did not contain an active calendar schedule or parent Job Plan (`JOBP`). It is designed to be triggered on-demand or externally.

---

## 4. Manual Steps Before Go-Live

Before activating this DAG in production, the following infrastructure and configuration steps must be completed:

### 1. Environment Profile Setup
* Ensure that the `$HOME/.dw_init` profile script exists on the target Airflow worker (or target SSH host) and contains all necessary database connection strings, paths, and environment variables.

### 2. Script Deployment
* Deploy the core shell script and its dependencies to the target filesystem:
  * Script: `$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift`
  * Associated AWK scripts (see Section 5).

### 3. Airflow Variables & Connections
* **Airflow Variable:** Create a global Airflow Variable named `GCP_PROJECT` containing your target Google Cloud Project ID.
* **SSH Connection (Optional):** If executing via `SSHOperator` instead of local `BashOperator`, configure an SSH connection in Airflow with the ID corresponding to host `DWHDWH1P` and user `DW.UNIX.ISTNS`.

### 4. IAM & Permissions
* Ensure the service account running the Airflow workers has read/write permissions to the directories where the raw "Gutschrift" files are read and where the final consolidated CSV is written.

---

## 5. Known Gaps & Unresolved References

### Missing Source Scripts
The actual processing scripts referenced by the legacy job were not part of the UC4 XML export bundle and must be migrated manually to the target environment:
* `isdwh/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift`
* `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutsch_foot.awk`
* `isdwh/vorverarbeitung/tn/awk/k_vvtn_iar_bgf_gutschrift.awk`

### Redesign (B4) / Tooling Failures
During the automated parsing phase, the migration tool flagged a `NO_MCP_TOOL` error because the source pattern was unrecognized. This was resolved by manually designing and writing the target Airflow DAG code provided in this bundle. No further automated tool intervention is required for this DAG's structure.

---

## 6. Validation

To validate the migrated workflow, perform the following steps:

### 1. Syntax and DAG Parsing Test
Run the following command within your Airflow environment to ensure there are no Python or DAG import errors:
```bash
python3 dags/dw_dwh_vvtn_iar_bgf_gutschr.py
```

### 2. Execution Test (Dry Run / Backfill)
Test-run the specific task for a historical date using the Airflow CLI:
```bash
airflow tasks test dw_dwh_vvtn_iar_bgf_gutschr dwh_vvtn_iar_bgf_gutschr 2023-10-01
```

### 3. Success Criteria
The validation is considered **passing** if:
1. The task execution log outputs: `Lastmonth is 202309` (verifying correct Jinja date math for an October execution date).
2. The task exits with status code `0`.
3. The consolidated CSV file is successfully generated in the target output directory with the correct data structure and footer.

---

## 7. Rollback Procedure

If issues arise post-go-live, execute the following rollback steps:

1. **Pause the Airflow DAG:**
   ```bash
   airflow dags pause dw_dwh_vvtn_iar_bgf_gutschr
   ```
2. **Re-enable the Legacy UC4 Job:**
   * Log into the UC4 GUI.
   * Locate the job `DW.DWH_VVTN_IAR_BGF_GUTSCHR`.
   * Ensure its active flag is set to `1` (Active).
   * Re-integrate it into any external scheduling or triggering mechanisms.
3. **Data Cleanup:**
   * If the Airflow execution partially wrote or corrupted the target CSV file, delete the incomplete file from the target directory before restarting the UC4 job to prevent duplicate or malformed data.