# Migration Notes: DW.BERT_P_RECH_EMPF

This document details the migration of the legacy UC4 job `DW.BERT_P_RECH_EMPF` (and its associated KornShell wrapper and Oracle SQL scripts) to Google Cloud Platform (GCP). The target architecture utilizes **Google Cloud Composer (Apache Airflow)** for orchestration and **Google BigQuery** as the data warehousing engine.

---

## 1. Summary

The legacy UC4 job `DW.BERT_P_RECH_EMPF` ("Aufbereitung der Rechnungsempfänger") has been migrated from an on-premises Unix environment to **GCP**. 

*   **Source Workflow**: A standalone UC4 `JOBS_UNIX` object executing `r_ausd_rechempf.ksh`, which in turn executed the Oracle SQL script `d_ausd_rechempf.sql`.
*   **Target Platform**: Google Cloud Composer (Airflow) and Google BigQuery.
*   **Core Functionality**: Prepares and compiles billing recipient reporting tables (`sof$ta_p_rech_empf` and `sof$ta_p_d1_vpn`) by extracting, joining, and transforming active payment, bank, and contract data.
*   **Synchronization**: Legacy synchronization against `DW.BERT_STAMMDATEN` and mutual exclusion via `DW.BERT_RECH_SYNC` have been preserved using native Airflow sensors and pools.

---

## 2. Generated Artifacts

The migration process generated three core artifacts, each replacing a specific component of the legacy pipeline:

| Artifact File Path | Target Technology | Role / Description |
| :--- | :--- | :--- |
| `dw_bert_p_rech_empf.py` | Apache Airflow (Python 3) | **Orchestration DAG**: Replaces the UC4 job definition. Manages upstream dependencies (`dw_bert_stammdaten`), enforces mutual exclusion via pools, and triggers the Python execution script. |
| `r_ausd_rechempf.py` | Python 3 (GCP SDK) | **Execution Wrapper**: Replaces `r_ausd_rechempf.ksh`. Parses command-line arguments (reporting date/restart values), initializes the native BigQuery client, executes the SQL script, and handles logging/metrics. |
| `d_ausd_rechempf.sql` | BigQuery Standard SQL | **Data Transformation**: Replaces the Oracle SQL script. Contains the rewritten DDL/DML statements optimized for BigQuery syntax and execution patterns. |

---

## 3. Key Design Decisions

### 3.1. Shell-to-Python Migration (`r_ausd_rechempf.ksh` $\rightarrow$ `r_ausd_rechempf.py`)
*   **Decision**: The legacy KornShell script was migrated to a native Python 3 script rather than a standard `BashOperator` shell script.
*   **Reasoning**: The legacy script contained complex orchestration logic, including `getopts` argument parsing, dynamic date calculations, process-ID-based temporary file handling, and custom error-trapping frameworks. Python's `argparse` and native GCP SDKs provide a robust, maintainable, and platform-independent execution environment.
*   **Trade-off**: Requires Python runtime dependencies on the Airflow worker, which is standard for Cloud Composer environments.

### 3.2. Native BigQuery Client Integration
*   **Decision**: The Python wrapper executes SQL queries natively using the `google-cloud-bigquery` library instead of calling external CLI utilities (like `bq query` or legacy `sqlplus` wrappers).
*   **Reasoning**: This eliminates shell subprocess overhead, enables secure parameterized queries to prevent SQL injection, and allows direct programmatic access to query metadata (such as `num_dml_affected_rows` for row-count logging).

### 3.3. Oracle to BigQuery SQL Translation
*   **Decision**: 
    *   Replaced Oracle-specific outer join syntax `(+)` with standard ANSI `LEFT OUTER JOIN`.
    *   Replaced dynamic PL/SQL blocks executing `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` with native BigQuery `TRUNCATE TABLE` statements.
    *   Replaced Oracle string concatenation `||` with `CONCAT` wrapped in `COALESCE` to prevent `NULL` propagation from nullifying entire strings (matching Oracle's native behavior).
    *   Mapped Oracle `DATE` columns to BigQuery `DATETIME` to preserve both date and time components without timezone offsets.
*   **Reasoning**: Ensures syntax compatibility and leverages BigQuery's highly parallelized columnar execution engine.

### 3.4. Concurrency and Synchronization
*   **Decision**: Implemented an `ExternalTaskSensor` to block execution until `dw_bert_stammdaten` completes, and assigned the execution task to an Airflow pool named `DW_BERT_RECH_SYNC` with a slot capacity of `1`.
*   **Reasoning**: Replicates the legacy UC4 sync requirements and prevents concurrent executions from corrupting shared temporary tables (`sof$ta_*`).

---

## 4. Manual Steps Before Go-Live

The following configuration steps must be completed in the target GCP environment before triggering the workflow:

### 4.1. Schema and Table Creation
Ensure that the target BigQuery datasets and tables exist. The tables must be partitioned or structured to support the following schemas:
*   **Staging/Core Tables**: `bpd$ta_means_of_payment`, `BPD$TA_BANK`, `BPD$TA_BANK_INTERNATIONAL`, `sof$ta_e_regulierer`, `sof$ta_e_reach_re`, `sof$ta_e_business_re`, `dwh$vi_s_ibasisprodukt`, and `isbert_schema.dwtk_meldungen`.
*   **Intermediate/Reporting Tables**: `sof$ta_means_of_pay`, `sof$ta_bank`, `sof$ta_bank_verb`, `sof$ta_bank_zuord`, `sof$ta_p_rech_empf`, and `sof$ta_p_d1_vpn`.

### 4.2. Airflow Variables
Define the following Airflow Variables in the Cloud Composer environment:
*   `GCP_PROJECT`: The GCP Project ID hosting the BigQuery datasets.
*   `GCP_REGION`: The GCP region (e.g., `europe-west3`).
*   `DWH_HOME_PATH`: The absolute path on the Airflow worker or mounted Cloud Storage bucket where the migrated scripts are deployed (e.g., `/home/airflow/gcs/dags/dwh`).

### 4.3. Airflow Pools
Create a new Airflow Pool to manage mutual exclusion:
*   **Pool Name**: `DW_BERT_RECH_SYNC`
*   **Slots**: `1`
*   **Description**: Prevents concurrent execution of billing recipient preparation tasks.

### 4.4. IAM Permissions
The Cloud Composer environment's service account must be granted the following IAM roles:
*   `roles/bigquery.dataEditor` on the target datasets.
*   `roles/bigquery.jobUser` on the GCP project.
*   `roles/storage.objectViewer` on the bucket storing the SQL and Python scripts (if executing from GCS).

### 4.5. Data Ingestion (Replacing `@pcrs1`)
Because BigQuery cannot natively query the legacy Oracle database link `@pcrs1`, an upstream ingestion pipeline (e.g., Cloud Data Fusion, BigQuery DTS, or an independent Python extractor) must be scheduled to replicate the following tables from the source system into BigQuery prior to running this DAG:
*   `BPD$TA_MEANS_OF_PAYMENT`
*   `BPD$TA_BANK`
*   `BPD$TA_BANK_INTERNATIONAL`

---

## 5. Known Gaps & Unresolved References

### 5.1. External Database Link Ingestion (Critical Gap)
*   **Description**: The legacy SQL script queried tables directly from a remote database using the `@pcrs1` synonym. 
*   **Resolution**: This has been flagged for redesign. The migrated SQL script assumes these tables have already been ingested into the local BigQuery dataset. The migration team must ensure that the upstream ingestion jobs are scheduled and completed before `dw_bert_p_rech_empf` runs.

### 5.2. Upstream DAG Dependency
*   **Description**: The `ExternalTaskSensor` in `dw_bert_p_rech_empf` is configured to watch `dw_bert_stammdaten`.
*   **Resolution**: The `dw_bert_stammdaten` DAG must be deployed and active in the same Cloud Composer environment. If the upstream DAG name differs, update the `external_dag_id` parameter in `dw_bert_p_rech_empf.py`.

---

## 6. Validation

To validate the migration, perform the following test steps:

### 6.1. Dry-Run SQL Validation
Validate the BigQuery SQL script syntax without executing DML operations:
```bash
bq query --use_legacy_sql=false --dry_run < d_ausd_rechempf.sql
```
*Verify that the output returns "Query successfully validated" along with an estimate of bytes to be read.*

### 6.2. Local Python Execution Test
Run the Python wrapper script manually from a terminal with access to the target BigQuery environment:
```bash
export BERT_DIR_ROOT="/path/to/your/local/dwh"
export DW_DIR_UTL="/tmp"
export GCP_PROJECT="your-gcp-project-id"
export BQ_LOCATION="EU"
export DW_EINTRAGS_NR="99999"

python r_ausd_rechempf.py -s 20231122 -l 0
```
*   **Passing Criteria**:
    *   The console outputs: `Die Abarbeitung wurde ohne erkennbare Fehler beendet`.
    *   The exit code is `0`.
    *   The temporary file `/tmp/bert_k_ausd_rechempf_<PID>.tmp` is created, written to, and successfully cleaned up.

### 6.3. Airflow DAG Integration Test
Trigger a test run of the Airflow DAG:
```bash
airflow dags test dw_bert_p_rech_empf 2023-01-01
```
*   **Passing Criteria**:
    *   The `wait_for_stammdaten` sensor successfully resolves (or is manually skipped for testing).
    *   The `dw_bert_p_rech_empf_task` task completes with a `SUCCESS` status.
    *   The target tables `sof$ta_p_rech_empf` and `sof$ta_p_d1_vpn` contain populated records matching the expected business logic.

---

## 7. Rollback Procedure

In the event of a critical failure during go-live, execute the following rollback steps:

### 7.1. Cloud Composer Rollback
1. Pause the migrated Airflow DAG:
   ```bash
   airflow dags pause dw_bert_p_rech_empf
   ```
2. If necessary, delete the DAG file from the Composer GCS bucket:
   ```bash
   gsutil rm gs://<composer-bucket>/dags/dw_bert_p_rech_empf.py
   ```

### 7.2. Database State Rollback
Since the SQL script performs `TRUNCATE` operations on intermediate tables, the database state can be restored by running a manual cleanup query:
```sql
TRUNCATE TABLE `sof$ta_means_of_pay`;
TRUNCATE TABLE `sof$ta_bank`;
TRUNCATE TABLE `sof$ta_bank_verb`;
TRUNCATE TABLE `sof$ta_bank_zuord`;
TRUNCATE TABLE `sof$ta_p_rech_empf`;
TRUNCATE TABLE `sof$ta_p_d1_vpn`;
```

### 7.3. Legacy Environment Reactivation
1. Log in to the UC4 / Automic user interface.
2. Locate the job `DW.BERT_P_RECH_EMPF`.
3. Ensure the active flag is set to `1` (Active).
4. Verify that the Unix agent host `DW.UNIX.ISTNS` is online and ready to accept executions.
5. Trigger the legacy job manually or resume its upstream scheduler trigger.