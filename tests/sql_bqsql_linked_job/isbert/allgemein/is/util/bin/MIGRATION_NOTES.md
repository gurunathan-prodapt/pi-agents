# Migration Notes: Shared Files — sql_bqsql_linked_job/isbert/allgemein/is/util/bin

This document details the migration of the legacy KornShell utility script `h_alis_sqlplus.ksh` to Python.

---

## 1. Summary

* **Source Artifact:** `h_alis_sqlplus.ksh` (KornShell utility script)
* **Target Platform:** Python 3 / Google Cloud Platform (GCP) / Cloud Composer (Apache Airflow)
* **Migration Verdict:** **PYTHON**
* **Purpose:** The legacy script served as a centralized helper module providing the `starteSQLSkript` function. It validated parameters, verified target SQL script readability, executed Oracle SQL*Plus non-interactively, and handled exit codes. This has been migrated to a Python module to support modern orchestration and execution on GCP.

---

## 2. Generated Artifacts

The migration process generated the following file:

### `sql_bqsql_linked_job/isbert/allgemein/is/util/bin/h_alis_sqlplus.py`
* **Role:** A Python module containing the migrated `starte_sql_skript` function and a Command Line Interface (CLI) wrapper.
* **Key Features:**
  * Parameter validation matching legacy logic.
  * File-system checks for script existence and readability using `pathlib`.
  * Non-interactive execution of external processes via `subprocess.run` with `stdin=subprocess.DEVNULL` (mimicking `</dev/null`).
  * Error code propagation matching legacy standards.

---

## 3. Key Design Decisions

* **Python vs. SQL Verdict:** A Python conversion was chosen because the utility performs OS-level operations (file readability checks, dynamic process execution, and positional parameter shifting) that cannot be executed natively within BigQuery SQL.
* **Subprocess Management:** Used `subprocess.run` with `check=False` to mimic the legacy shell's `set +e` and `set -e` behavior. This ensures that SQL*Plus execution failures do not crash the Python wrapper itself, allowing the script to capture and return the exact exit code.
* **Input Redirection:** Configured `stdin=subprocess.DEVNULL` to prevent SQL*Plus from hanging in an interactive terminal state if a script prompts for input.
* **Bug Correction:** The legacy KornShell script defined variables as `ModulName` and `ModulVersion` but referenced them as `Modul_Name` and `Modul_Version` in its error-handling calls. This bug was corrected in the Python implementation to use consistent variable names (`MODUL_NAME` and `MODUL_VERSION`).
* **BigQuery Transition Strategy:** While this script directly translates the execution of `sqlplus`, running raw SQL*Plus commands is obsolete in a pure BigQuery target architecture. This wrapper serves as a bridge. Ultimately, the dynamic SQL scripts executed by this utility must be migrated to BigQuery SQL and executed via the `google-cloud-bigquery` client library.

---

## 4. Manual Steps Before Go-Live

Before deploying this utility to production, complete the following configuration steps:

1. **IAM & Permissions:**
   * Ensure the execution environment (e.g., Cloud Composer workers, GCE instances) has the necessary IAM permissions to access target databases or BigQuery datasets.
   * If still executing legacy Oracle workloads, ensure the Oracle Instant Client and `sqlplus` binary are installed and accessible in the system `PATH`.
2. **Secrets & Connection Strings:**
   * The environment variable `DW_ORAUSER` must be set in the execution environment.
   * *Recommendation:* Transition this credential from a raw environment variable to Google Cloud Secret Manager or Airflow Connections.
3. **Environment Variables:**
   * Configure `GCP_PROJECT` and `BQ_DATASET` in the runtime environment to support downstream BigQuery operations.
4. **Downstream Scheduling:**
   * Because downstream jobs (`DW.BERT_AUSD_V_TA_PERIOD` and `r_ai_start`) are not yet migrated, coordinate with the scheduling team to establish cross-platform dependencies (e.g., using Airflow `TriggerDagRunOperator` or Pub/Sub events).

---

## 5. Known Gaps & Unresolved References

* **Unresolved Component (`DWMSG_MeldeFehler`):**
  * **Gap:** The legacy script relies on an external error-logging utility named `DWMSG_MeldeFehler`. The source code for this utility was not provided.
  * **Mitigation:** A stub function `dwmsg_melde_fehler` has been created in `h_alis_sqlplus.py` which raises a `NotImplementedError`. This stub **must** be implemented by the integration team to map to the target platform's logging framework (e.g., Google Cloud Logging or standard Python `logging`).
* **Unmigrated Downstreams:**
  * **Gap:** Downstream consumers `DW.BERT_AUSD_V_TA_PERIOD` and `r_ai_start` are not yet migrated.
  * **Mitigation:** Orchestration linkages cannot be fully validated until these dependent systems are migrated.
* **SQL*Plus Syntax Compatibility:**
  * **Gap:** The SQL scripts executed by this utility may contain Oracle-specific PL/SQL or SQL*Plus formatting commands (e.g., `SET SERVEROUTPUT ON`, `WHENEVER SQLERROR EXIT`). These commands will fail if executed directly against BigQuery.
  * **Mitigation:** Translate the underlying SQL scripts to BigQuery-compatible SQL and refactor this helper to execute queries via the BigQuery Client API instead of `subprocess`.

---

## 6. Validation

To validate the migrated Python script, execute the following test cases:

### Test Case 1: Missing Parameters
* **Command:**
  ```bash
  python3 h_alis_sqlplus.py "" ""
  ```
* **Expected Result:** The script should fail, attempt to call `dwmsg_melde_fehler` (raising `NotImplementedError` unless implemented), and return exit code `196`.

### Test Case 2: Unreadable/Missing SQL Script
* **Command:**
  ```bash
  python3 h_alis_sqlplus.py "1001" "/nonexistent/path/script.sql"
  ```
* **Expected Result:** The script should fail, attempt to call `dwmsg_melde_fehler` with error code `201`, and return exit code `201`.

### Test Case 3: Successful Execution (Dry Run / Mocked)
* **Command:** Create a dummy SQL file `test.sql` and run:
  ```bash
  export DW_ORAUSER="test_user/test_pass@test_db"
  python3 h_alis_sqlplus.py "1001" "./test.sql" "param1" "param2"
  ```
* **Expected Result:** The script should print the execution details to stdout and attempt to spawn `sqlplus`. (Note: This will fail with a process execution error if `sqlplus` is not installed, which validates that the subprocess spawning logic is functioning).

---

## 7. Rollback Procedure

In the event of a deployment failure or unexpected runtime behavior:

1. **Revert Code Changes:** Roll back the deployed `h_alis_sqlplus.py` file to the previous stable version in your version control system.
2. **Restore Legacy Execution:** If necessary, redirect calling jobs (e.g., in UC4/Automic or parent shell scripts) to source the legacy `h_alis_sqlplus.ksh` script.
3. **Verify Environment:** Ensure that the legacy Oracle environment variables (such as `ORACLE_HOME`, `TNS_ADMIN`, and `DW_ORAUSER`) remain active and correctly configured on the execution host.