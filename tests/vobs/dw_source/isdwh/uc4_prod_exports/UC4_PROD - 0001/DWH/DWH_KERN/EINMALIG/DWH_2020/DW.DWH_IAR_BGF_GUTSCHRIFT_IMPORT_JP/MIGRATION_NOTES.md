# MIGRATION NOTES: DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT

## 1. Summary
The UC4 File Event (`EVNT_FILE`) `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT` has been migrated to an Apache Airflow DAG on Google Cloud Platform (Cloud Composer). 

In the legacy UC4 system, this object functioned as an autonomous file-polling daemon that checked for the arrival of "BGF Gutschrift Import" (Credit Memo/Refund Import) check files (`DWHK_DWHM_IAR_GUTSCHR_*.chk`) every 30 minutes. Upon detecting a file, it verified if the downstream processing Jobplan (`DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP`) was already active. If not active, it triggered the Jobplan and logged the execution.

The migrated Airflow DAG preserves this polling frequency, state-checking logic, downstream triggering mechanism, and strict logging requirements.

---

## 2. Generated Artifacts
The migration process generated the following file:

* **`vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/EINMALIG/DWH_2020/DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP/DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT.py`**
  * **Role**: Core Airflow DAG definition file. It contains the scheduling parameters, GCS file sensor, custom Python state-checker, downstream DAG trigger operator, and success logging tasks.

---

## 3. Key Design Decisions

### File Sensing Mechanism
* **Decision**: Utilized `GCSObjectsWithPrefixPatternSensor` instead of a standard `GCSObjectExistenceSensor` or `SFTPFileSensor`.
* **Reasoning**: The legacy system monitored a wildcard pattern (`DWHK_DWHM_IAR_GUTSCHR_*.chk`). Since the target architecture lands these files in Google Cloud Storage (GCS), a prefix-pattern sensor is the most efficient cloud-native equivalent to handle wildcard matching.

### Concurrency & State Checking
* **Decision**: Implemented a custom Python task (`check_target_active`) using the Airflow `DagRun` model to check for active runs of the downstream DAG.
* **Reasoning**: In UC4, the script checked `SYS_STATE_ACTIVE(JOBP, &StartJp) = 'Y'` to prevent triggering a new workflow if one was already running. Airflow's `TriggerDagRunOperator` does not natively check if a target DAG is already running before triggering (it will simply queue another run if concurrency limits allow). This custom task ensures strict parity with the legacy safety check.

### Asynchronous Triggering
* **Decision**: Configured `TriggerDagRunOperator` with `wait_for_completion=False`.
* **Reasoning**: This matches the "fire-and-forget" behavior of UC4's `ACTIVATE_UC_OBJECT` call, allowing the event-sensing DAG to complete its run immediately after successfully handing off execution to the ingestion pipeline.

### Output/Print Literal Rule Compliance
* **Decision**: Hardcoded exact string literals within the Python logging tasks.
* **Reasoning**: To ensure compatibility with legacy log parsers and operational runbooks, the exact wording, casing, and spacing of the original UC4 script outputs have been preserved verbatim.

---

## 4. Manual Steps Before Go-Live

### 1. Airflow Variables Configuration
Ensure the following Airflow Variables are defined in the target environment:
* `GCS_BUCKET`: The name of the GCS bucket where incoming credit memo files land (e.g., `prod-dwh-landing-bucket`).
* `SFTP_CONN_ID` (Optional): Defaulted to `sftp_dwh_conn`. Configure this connection in Airflow if direct SFTP polling is required instead of GCS.

### 2. IAM & Permissions
* The Cloud Composer environment's service account must have **Storage Object Viewer** (`roles/storage.objectViewer`) permissions on the GCS bucket defined in `GCS_BUCKET`.
* The service account must have sufficient Airflow RBAC permissions to read DAG states and trigger DAG runs (typically granted by default to the Composer worker service account).

### 3. Connection Strings
* If files are retrieved via SFTP, configure the `sftp_dwh_conn` connection in Airflow with the appropriate host, port, username, and SSH private key corresponding to the legacy `DW.UNIX.ISTNS` login object.

### 4. Downstream DAG Deployment
* Ensure that the downstream ingestion DAG `dw_dwh_iar_bgf_gutschrift_import_jp` is deployed and active in the Airflow environment. The trigger task will fail if this DAG ID is not registered.

---

## 5. Known Gaps & Unresolved References

### Downstream Pipeline Dependency
* **Gap**: The downstream processing pipeline (`dw_dwh_iar_bgf_gutschrift_import_jp`) is a separate migration target. 
* **Action**: This event DAG cannot be fully end-to-end tested until the downstream DAG is deployed.

### Path Resolution (`DW.HOLE_PFAD`)
* **Gap**: The legacy script included `DW.HOLE_PFAD` to resolve environment-specific file paths. 
* **Action**: The migrated DAG assumes a standard GCS prefix of `landing/`. If the actual landing path differs, update the `prefix` parameter in the `sense_import_file` task.

### Standard Alerting Framework (`DW.CALL_STANDARD`)
* **Gap**: The legacy `CallOP` pointed to `DW.CALL_STANDARD` for failure notifications.
* **Action**: The `on_failure_alarm` function in the DAG is currently a stub. It must be integrated with the local Cloud Composer alerting mechanism (e.g., SMTP, Slack, or PagerDuty webhook).

---

## 6. Validation

### Unit & Syntax Testing
Run a local syntax check on the generated Python file:
```bash
python3 vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/EINMALIG/DWH_2020/DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP/DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT.py
```
*Passing criteria*: The command exits with code `0` and no import or syntax errors are displayed.

### DAG Parsing Test
Place the file in the Airflow `dags/` folder and verify:
1. No DAG import errors appear in the Airflow UI.
2. The DAG `dw_dwh_run_iar_bgf_gutschrift_import_jp_evt` is visible.

### Functional Testing (Dry Run)
1. **Scenario A: File Not Present**
   * Ensure no files matching `DWHK_DWHM_IAR_GUTSCHR_*.chk` exist in the target GCS bucket path.
   * Trigger the DAG manually.
   * *Passing criteria*: The `sense_import_file` task remains in a `running` (polling) state and eventually times out or is manually skipped.

2. **Scenario B: File Present & Target Idle**
   * Upload a dummy file named `DWHK_DWHM_IAR_GUTSCHR_TEST_999.chk` to the GCS bucket prefix.
   * Ensure the downstream DAG `dw_dwh_iar_bgf_gutschrift_import_jp` is **not** running.
   * Trigger the DAG.
   * *Passing criteria*: 
     * `sense_import_file` succeeds.
     * `check_target_active` succeeds and logs:
       ```text
       Starting Jobplan DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP ...
       ```
     * `trigger_downstream_workflow` succeeds and triggers the downstream DAG.
     * `log_trigger_success` succeeds and logs:
       ```text
       JP started at <CURRENT_DATE_YYYYMMDD> ...
       ```

3. **Scenario C: File Present & Target Active**
   * Ensure a dummy file exists in GCS.
   * Manually start a run of `dw_dwh_iar_bgf_gutschrift_import_jp` so it is in a `running` state.
   * Trigger the event DAG.
   * *Passing criteria*:
     * `sense_import_file` succeeds.
     * `check_target_active` skips downstream tasks, logging:
       ```text
       Jobplan DW.DWH_IAR_BGF_GUTSCHRIFT_IMPORT_JP is active!
       ```
     * Downstream tasks are marked as `skipped`.

---

## 7. Rollback Procedure

In the event of an issue during deployment or go-live:

1. **Pause the Airflow DAG**:
   Go to the Airflow UI and toggle the active switch for `dw_dwh_run_iar_bgf_gutschrift_import_jp_evt` to **Off** (Paused).
2. **Re-enable UC4 Event**:
   In the UC4 client, locate the File Event `DW.DWH_RUN_IAR_BGF_GUTSCHRIFT_IMPORT_JP_EVT` and set its status to **Active**.
3. **Verification**:
   * Confirm that the UC4 event resumes polling the legacy directory.
   * Confirm that no duplicate executions of the downstream job are triggered.