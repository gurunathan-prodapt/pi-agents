# Migration Notes: DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT

These migration notes document the transition of the UC4 file event monitor `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT` to an Apache Airflow DAG.

---

## 1. Summary

The UC4 Event object `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT` (of type `EVNT_FILE`) has been migrated to an event-driven Apache Airflow DAG. 

* **Source Platform:** UC4 / Automic Workload Automation
* **Target Platform:** Apache Airflow (Google Cloud Composer)
* **Migration Type:** Event-driven File Sensor and Downstream Trigger
* **Functional Description:** This job monitors an external SFTP directory for the arrival of a check file (`DWHK_DWHM_IAR_GUTSCHR_*.chk`). Upon detecting the file, it triggers the downstream processing workflow `DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP`.

---

## 2. Generated Artifacts

The migration process generated the following file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/EINMALIG/DWH_2020/DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP/DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT.py` | Airflow DAG | Python script containing the DAG definition, custom SFTP wildcard sensor, and downstream trigger logic. |

---

## 3. Key Design Decisions

### Custom `SFTPWildcardSensor`
* **Why:** The source UC4 event uses a wildcard pattern (`DWHK_DWHM_IAR_GUTSCHR_*.chk`) to detect files. Standard Airflow SFTP sensors do not natively support wildcard matching in a single task without listing directories. 
* **Implementation:** A custom `SFTPWildcardSensor` was implemented using `fnmatch` to list the directory and match files against the wildcard pattern.
* **Trade-off:** Listing directories can be slow if the target directory contains thousands of files. However, given the specific path structure, this is the most robust way to replicate UC4's native file-sensing behavior.

### Sensor Execution Mode (`reschedule`)
* **Why:** File sensors can run for hours waiting for external data. Using the default `poke` mode blocks an Airflow worker slot for the entire duration.
* **Implementation:** The sensor is configured with `mode="reschedule"`. This releases the worker slot between checks (polling every 5 minutes / 300 seconds), significantly reducing resource consumption.

### Dynamic Downstream Triggering via PythonOperator
* **Why:** The UC4 script contains conditional logic: if the target Jobplan is already active, it logs a message; otherwise, it activates it and logs the start time. A standard `TriggerDagRunOperator` would unconditionally attempt to trigger the DAG, potentially causing duplicate runs or failures depending on DAG configuration.
* **Implementation:** A `PythonOperator` utilizing the `airflow.api.common.trigger_dag` API was used to check for active runs of the target DAG before triggering, matching the legacy UC4 behavior exactly.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling this DAG in production, the following manual setup steps must be completed:

### 1. Connection Configuration
Create an SFTP connection in Airflow to connect to the legacy host `EXT:dwhdwh1p`.
* **Conn ID:** `sftp_default` (or configure a custom name and update the Airflow Variable).
* **Conn Type:** `SFTP`
* **Host:** Hostname or IP of `dwhdwh1p`.
* **Login:** `DW.UNIX.ISTNS` equivalent credentials.
* **Port:** `22` (or custom SFTP port).
* **Extra:** `{"key_file": "/path/to/private/ssh/key"}` or password configuration.

### 2. Airflow Variables
Ensure the following Airflow Variables are defined in the target environment:
* `SFTP_CONN_ID`: The connection ID created in Step 1 (defaults to `sftp_default`).
* `GCP_PROJECT`: The Google Cloud Project ID (if applicable).

### 3. Network & Firewall Rules
Verify that the Cloud Composer / Airflow GKE workers have network egress allowed to port `22` of the target host `dwhdwh1p`.

### 4. Downstream DAG Deployment
The downstream processing DAG `dw_dwh_iar_bgf_gutschrift_import_jp` **must** be deployed and paused/unpaused in the same Airflow environment before this sensor DAG is activated.

---

## 5. Known Gaps & Unresolved References

### Missing Include Script (`DW.HOLE_PFAD`)
* **Gap:** The legacy UC4 script references an include block `: inc DW.HOLE_PFAD` to resolve environment-specific paths. This file was not supplied in the migration bundle.
* **Resolution:** The file path `/app_dwh/sftp_users/istcomis/daten/tcom/iar/work/DWHK_DWHM_IAR_GUTSCHR_*.chk` was extracted from the XML metadata and hardcoded into the DAG configuration. If paths differ across environments (Dev/Test/Prod), this path should be moved to an Airflow Variable or Environment Variable.

### Target Architecture Alignment (SFTP vs. Cloud Storage)
* **Gap:** This DAG currently polls an SFTP server. If the target cloud architecture dictates that files should land directly in Google Cloud Storage (GCS) instead of an SFTP server, this DAG must be refactored.
* **Redesign Action:** Replace `SFTPWildcardSensor` with `GCSObjectsWithPrefixExistSensor` or configure an event-driven Cloud Function to trigger the downstream DAG directly via Airflow's REST API, bypassing the need for a polling sensor entirely.

---

## 6. Validation

To validate the migrated DAG, perform the following tests:

### 1. DAG Parsing Test
Run the following command in your local development or CI/CD environment to ensure there are no syntax or import errors:
```bash
python vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD\ -\ 0001/DWH/DWH_KERN/EINMALIG/DWH_2020/DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP/DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT.py
```

### 2. Mocked Integration Test
1. Set up a mock SFTP server or use a test directory on the SFTP server.
2. Configure the `SFTP_CONN_ID` in Airflow to point to this test environment.
3. Trigger the DAG manually in Airflow.
4. Verify that the `detect_file_event` task remains in a `up_for_reschedule` or `running` state.
5. Upload a dummy file matching the pattern (e.g., `DWHK_DWHM_IAR_GUTSCHR_test.chk`) to the target directory.
6. Verify that the sensor detects the file, completes successfully, and triggers the downstream DAG.

### Successful Run Criteria
* **Sensor Task:** Completes with state `SUCCESS`. Logs show: `Found matching files: ['DWHK_DWHM_IAR_GUTSCHR_xxxx.chk']`.
* **Trigger Task:** Completes with state `SUCCESS`. Logs show: `Starting Jobplan dw_dwh_iar_bgf_gutschrift_import_jp ...` followed by `JP started at YYYYMMDD ...`.

---

## 7. Rollback Procedure

In the event of an issue during go-live, follow these steps to roll back to the legacy UC4 scheduler:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the switch for `dw_dwh_run_iar_bgf_gutschrift_import_jp_evt` to **Off** (Paused).
2. **Clear Active Runs:**
   If there are any active or queued runs in Airflow, select them and set their state to `FAILED` or `SKIPPED` to prevent them from triggering downstream jobs.
3. **Re-enable UC4 Event:**
   Log into the UC4 client, locate the event object `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT`, and set its status to **Active**.
4. **Verify UC4 Monitoring:**
   Confirm in the UC4 Activity Window that the file event is actively polling the file system.