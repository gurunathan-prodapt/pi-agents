# Migration Notes: DW.BERT_AUSD_V_TA_P_VERTRAG

## 1. Summary
The legacy UC4 Unix Job `DW.BERT_AUSD_V_TA_P_VERTRAG` and its associated orchestration scripts and Oracle SQL scripts have been migrated to **Google Cloud Platform (GCP)**. 

*   **Source Platform:** UC4/Automic, Unix Shell (KSH), Oracle Database
*   **Target Platform:** Google Cloud Composer (Apache Airflow), Python 3, BigQuery
*   **Workload Purpose:** Reconciles and updates contract information regarding twin-billing (twin-bill) within the BERT application by processing staging data and cleaning up intermediate database tables.

---

## 2. Generated Artifacts
The migration process generated the following files, each playing a specific role in the target architecture:

| Generated File Path | Language / Type | Role |
| :--- | :--- | :--- |
| `dw_bert_ausd_v_ta_p_vertrag.py` | Python (Airflow DAG) | The master orchestrator DAG. Replaces the UC4 Job definition and manages the sequential execution of the wrapper, control, and SQL transformation tasks. |
| `r_ausd_v_ta_p_vertrag.py` | Python 3 | Migrated wrapper script. Validates command-line parameters, initializes tracking IDs, registers the execution session, and outputs the generated sequence ID (`DW_EINTRAGS_NR`) for Airflow XCom consumption. |
| `k_ausd_v_ta_p_vertrag.py` | Python 3 | Migrated control script. Validates job parameters, verifies the execution environment, and handles process-level logging. |
| `d_ausd_v_ta_p_vertrag.sql` | BigQuery SQL | Migrated core transformation script. Executes the twin-bill contract alignment and truncates 22 intermediate staging tables. |

---

## 3. Key Design Decisions

### Decoupling and Flattening (B4 Redesign)
In the legacy environment, `r_ausd_v_ta_p_vertrag.ksh` called `k_ausd_v_ta_p_vertrag.ksh` via a subprocess, which in turn executed the SQL script. To align with modern cloud-native orchestration patterns, this nested execution has been **flattened** into sequential Airflow tasks:
```
[r_ausd_v_ta_p_vertrag] >> [k_ausd_v_ta_p_vertrag] >> [d_ausd_v_ta_p_vertrag_dataform]
```
This decoupling allows Airflow to natively manage retries, capture logs independently for each step, and prevent redundant subprocess invocations on GKE workers.

### State Sharing via Airflow XCom
The wrapper script dynamically generates an execution entry ID (`DW_EINTRAGS_NR`). To pass this ID to the downstream control task, `r_ausd_v_ta_p_vertrag.py` writes `DW_EINTRAGS_NR=<value>` to standard output, which is captured by Airflow and retrieved in the downstream task via XCom:
`"{{ task_instance.xcom_pull(task_ids='r_ausd_v_ta_p_vertrag') }}"`

### ANSI SQL Conversion
Oracle-specific outer join syntax `(+)` was converted to standard ANSI `LEFT OUTER JOIN` syntax to ensure compatibility with the BigQuery SQL engine:
*   **Legacy:** `v.twin_vertrag_id = pv.vertrag_id_carmen (+)`
*   **Target:** `FROM sof$ta_vertrag_tmp v LEFT OUTER JOIN sof$ta_vertrag_tmp pv ON v.twin_vertrag_id = pv.vertrag_id_carmen`

### Direct Truncation over Dynamic PL/SQL
The legacy script used a custom PL/SQL utility package (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`) to dynamically truncate tables. This has been replaced with direct, native BigQuery `TRUNCATE TABLE` statements. This guarantees compile-time schema validation and significantly improves execution performance.

### Performance Optimization
Oracle parallel execution hints (e.g., `/*+ parallel(v,4) */`) were stripped out. BigQuery automatically manages query parallelization and scaling, making manual optimizer hints obsolete.

---

## 4. Manual Steps Before Go-Live

### 1. Schema and Dataset Creation
Ensure the following BigQuery datasets exist in your target GCP project:
*   `isbert_schema`
*   `sof`

Ensure the target table `sof.sof$ta_p_vertrag` and the 22 intermediate staging tables (e.g., `sof.sof$ta_vertrag_tmp`, `sof.sof$ta_discount`, etc.) are created with schemas matching the legacy Oracle definitions.

### 2. IAM & Permissions
The Cloud Composer service account must be granted the following IAM roles:
*   `roles/bigquery.dataEditor` on the `isbert_schema` and `sof` datasets.
*   `roles/bigquery.jobUser` on the target GCP project.
*   `roles/storage.objectViewer` on the GCS bucket housing the scripts.

### 3. Airflow Variables Configuration
Configure the following Airflow Variables in the Composer environment:

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-id` | Target Google Cloud Project ID. |
| `GCP_REGION` | `europe-west3` | Target GCP Region. |
| `GCS_BUCKET` | `my-composer-bucket` | GCS bucket used for temporary storage. |
| `WORKSPACE_ROOT` | `/home/airflow/gcs/dags/dependencies` | Path to the root directory containing the migrated Python scripts. |
| `DWH_JOB_KENNUNG` | `AUSD_V_TA_P_VERTRAG` | Default job identifier. |

### 4. Environment Variables
Ensure the following environment variables are exposed to the Airflow workers (either via Composer environment configuration or local container profiles):
*   `BERT_DIR_ROOT`: Points to the root directory of the BERT application scripts.
*   `DW_DIR_UTL`: Points to the directory designated for temporary files.

### 5. Scheduling Integration
The migrated DAG is configured with `schedule=None`. It must be integrated into the parent workflow DAG (the equivalent of the legacy `DW.BERT_P_VERTRAG_JP` Job Plan) using a `TriggerDagRunOperator`.

---

## 5. Known Gaps & Unresolved References

### Unused Date Variable (`v_datum`)
The SQL script `d_ausd_v_ta_p_vertrag.sql` queries `v_datum` from `isbert_schema.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`. However, this variable is never referenced in any downstream filters or queries within this script. 
*   *Status:* Preserved in the BigQuery SQL script to maintain functional parity with the legacy code.
*   *Recommendation:* Flagged for future cleanup/deprecation if verified as a legacy holdover.

### Shared Utility Libraries
The Python scripts import functions from `f_alis_msgerr` and `h_alis_sqlplus`. These represent shared utility scripts migrated in separate passes (PR #845 and PR #846). 
*   *Status:* Ensure these migrated Python modules are packaged and placed in the Python search path (`PYTHONPATH`) of the Airflow workers. Mock fallbacks are included in the generated scripts to prevent compilation failures, but the actual libraries must be deployed for full operational logging.

### Transactional Integrity
BigQuery executes scripting statements sequentially with auto-commit. If atomic rollback of the 22 table truncations and the main insert is required upon failure, the SQL script must be manually wrapped in a `BEGIN TRANSACTION ... COMMIT TRANSACTION` block.

---

## 6. Validation

To validate the migration, execute the following tests. A "passing" status is achieved only when all criteria are met.

### Test 1: SQL Syntax and Schema Validation (Dry Run)
Run the migrated SQL script in the BigQuery console using dry-run mode:
*   **Verification:** Ensure no syntax errors are returned and all table references resolve correctly.

### Test 2: Local Script Execution
Run the Python scripts locally or in a development container with mock environment variables:
```bash
export BERT_DIR_ROOT="./vobs/dw_source/isrpt/isbert/SQL/aktuell"
export DW_DIR_UTL="./tmp"
python3 r_ausd_v_ta_p_vertrag.py -s test -l test
python3 k_ausd_v_ta_p_vertrag.py -j AUSD_V_TA_P_VERTRAG -f 1001
```
*   **Verification:** Ensure the scripts exit with code `0` and print the expected German log prompts.

### Test 3: End-to-End DAG Run
Trigger the `dw_bert_ausd_v_ta_p_vertrag` DAG manually from the Airflow UI.
*   **Verification:** 
    1.  The DAG run completes with a `SUCCESS` status.
    2.  The table `sof.sof$ta_p_vertrag` is populated with reconciled twin-bill contract data.
    3.  All 22 intermediate staging tables are successfully truncated (0 rows).
    4.  The Airflow task logs contain the exact German output literals:
        *   `"variablendefinitionen"`
        *   `"tabelle von vorherigem lauf leeren"`
        *   `"Verarbeitung fehlerfrei beendet."`

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during deployment:

1.  **Pause the Airflow DAG:** Immediately pause the `dw_bert_ausd_v_ta_p_vertrag` DAG in the Airflow UI to prevent further executions.
2.  **Database Restore:** Restore the target table `sof.sof$ta_p_vertrag` to its pre-execution state using a BigQuery table snapshot or time-travel query:
    ```sql
    CREATE OR REPLACE TABLE `sof.sof$ta_p_vertrag`
    AS SELECT * FROM `sof.sof$ta_p_vertrag` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```
3.  **Revert Code:** Revert the Git repository to the last stable commit prior to merging the migration files.
4.  **Resume Legacy Job:** Re-enable the legacy UC4 job `DW.BERT_AUSD_V_TA_P_VERTRAG` on the legacy scheduler to resume operations.