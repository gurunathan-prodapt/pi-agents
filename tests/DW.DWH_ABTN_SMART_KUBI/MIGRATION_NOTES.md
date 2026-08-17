# Migration Notes: DW.DWH_ABTN_SMART_KUBI

These migration notes document the transition of the legacy UC4 job `DW.DWH_ABTN_SMART_KUBI` from an on-premises Unix/Oracle environment to Google Cloud Platform (GCP). The migrated workload utilizes **Google Cloud Composer (Airflow)** for orchestration and **Google BigQuery** for data warehousing.

---

## 1. Summary

The legacy job `DW.DWH_ABTN_SMART_KUBI` was an active Unix-based UC4 job (`JOBS_UNIX`) responsible for populating a temporary aggregation table (`DWH$TA_T_SMART_KUBI`). It performed dynamic date calculations to determine a target reporting month (`MONATSID`) and executed a PL/SQL script (`d_abtn_x_smart_kubi.sql`) via a series of custom KornShell wrapper utilities (`r_sqlscript`, `h_alis_sqlplus.ksh`, `f_alis_msgerr.ksh`, and `.dw_init`).

### Migration Target Architecture
* **Orchestration:** Google Cloud Composer (Apache Airflow 2.x)
* **Data Warehouse:** Google BigQuery
* **Execution Model:** Native BigQuery SQL Scripting executed via the Airflow `BigQueryInsertJobOperator`.
* **Parameterization:** Dynamic runtime parameter injection using Airflow Jinja templates and macros.

---

## 2. Generated Artifacts

The migration process has generated the following artifacts, each serving a specific role in the target architecture:

| File Path | Type | Role / Description |
| :--- | :--- | :--- |
| `dwh_abtn_smart_kubi_dag.py` | Airflow DAG | **Primary Production Orchestrator.** Schedules the monthly execution, dynamically calculates the reporting month (`MONATSID`) using Airflow macros, and triggers the BigQuery SQL script. |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | BigQuery SQL | **Core Business Logic.** Migrated from Oracle PL/SQL to BigQuery SQL Scripting. Handles table truncation, ANSI left outer joins, and set-based aggregation. |
| `d_abtn_x_smart_kubi_wrapper.py` | Python Script | **CLI Execution Wrapper.** A standalone utility to execute the migrated BigQuery SQL script with parameters outside of Airflow (ideal for manual testing and backfills). |
| `local/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi.py` | Airflow DAG | **Structural Legacy Mapping.** A secondary DAG mapping that mirrors the legacy UC4 task structure with placeholders. (Retained for lineage auditing). |
| `local/home/gurunathan_t/kubi/dw_init.py` | Python Module | **Environment Config.** Replaces the legacy `.dw_init` shell script, mapping local directory paths to Google Cloud Storage (GCS) bucket paths. |
| `local/home/gurunathan_t/kubi/f_alis_msgerr.py` | Python Module | **Logging Library.** Replaces the legacy `f_alis_msgerr.ksh` library, mapping Oracle-based logging to Python/Oracle DB calls. |
| `local/home/gurunathan_t/kubi/h_alis_sqlplus.py` | Python Module | **Execution Library.** Replaces the legacy `h_alis_sqlplus.ksh` library, wrapping command-line SQL*Plus executions. |
| `local/home/gurunathan_t/kubi/r_sqlscript.py` | Python Script | **Legacy Runner.** Replaces the legacy `r_sqlscript` launcher, supporting local path resolution and BigQuery execution. |

---

## 3. Key Design Decisions

### 3.1 Native BigQuery SQL Scripting over Python/Pandas
* **Decision:** The core transformation logic was migrated directly to a BigQuery SQL Scripting block (`DECLARE`, `BEGIN...EXCEPTION`, `TRUNCATE`, `INSERT INTO...SELECT`) rather than refactoring into a Python/Pandas or PySpark job.
* **Reasoning:** The aggregation involves heavy joins across large fact and dimension tables (`dwh_ta_f_d1_twvv_tn`, `dwh_ta_c_vertrag`). Executing this natively within BigQuery leverages the engine's massive scale, eliminates data egress costs, and keeps the architecture simple.

### 3.2 ANSI Join Syntax Conversion
* **Decision:** Converted legacy Oracle outer join syntax `(+)` to standard ANSI `LEFT OUTER JOIN` syntax.
* **Reasoning:** BigQuery does not support the legacy Oracle `(+)` operator. Standardizing to ANSI joins ensures compatibility and improves query readability.

### 3.3 Partition Pruning Strategy
* **Decision:** Removed the legacy Oracle partition-extended table syntax `partition(dwh$ta_f_d1_twvv_tn_&1)` and replaced it with a standard `WHERE` clause filter: `WHERE FORMAT_DATETIME('%Y%m', fact.gueltigkeitszeitpunkt) = CAST(l_monats_id AS STRING)`.
* **Reasoning:** BigQuery does not support explicit partition selection in the `FROM` clause. Instead, it relies on automatic partition pruning when a query filters on the partitioned column.

### 3.4 Stateless, Deterministic Scheduling
* **Decision:** Replicated the legacy UC4 date logic (determining if the execution day is before the 15th to target the previous month) using Airflow's native Jinja macros (`macros.dateutil.relativedelta`).
* **Reasoning:** This ensures that the DAG remains stateless and deterministic. If a historical run is triggered, Airflow's `execution_date` (logical date) is used to calculate the correct relative reporting month, preventing errors during backfills.

### 3.5 Wrapper Retirement in Production
* **Decision:** Sourced shell scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`, `r_sqlscript`) were successfully converted to Python modules to preserve legacy compatibility. However, the production DAG (`dwh_abtn_smart_kubi_dag.py`) bypasses these wrappers entirely to use native Airflow operators (`BigQueryInsertJobOperator`).
* **Reasoning:** Bypassing the legacy wrapper layers in production reduces execution overhead, simplifies error tracking, and aligns with cloud-native orchestration patterns.

---

## 4. Manual Steps Before Go-Live

Before enabling the migrated workload in production, the following manual setup steps must be completed:

### 4.1 Schema and Dataset Creation
1. Ensure the target BigQuery dataset (configured via the Airflow Variable `BQ_DATASET`) exists in your GCP project.
2. Create the target table `dwh_ta_t_smart_kubi` with a schema matching the legacy Oracle table:
   * `monats_id` (INT64)
   * `kundennummer` (STRING)
   * `tarif_id` (INT64)
   * `tarif_id_alt` (INT64)
   * `vo_kennung` (STRING)
   * `test_gp` (STRING)
   * `anzahl` (INT64)
   * `kennzahl_id` (STRING)
3. Ensure all source tables and views are fully migrated and populated:
   * `dwh_vi_l_map_fa_tarif`
   * `bl_d_tarif`
   * `dwh_ta_f_d1_twvv_tn`
   * `dwh_ta_c_vertrag`

### 4.2 IAM & Permissions
1. Grant the Cloud Composer service account (e.g., `service-XXX@gcp-sa-composer.iam.gserviceaccount.com`) the following IAM roles on the target BigQuery dataset:
   * **BigQuery Job User** (`roles/bigquery.jobUser`)
   * **BigQuery Data Editor** (`roles/bigquery.dataEditor`)
2. If using GCS to store SQL scripts, grant the service account **Storage Object Viewer** (`roles/storage.objectViewer`) on the environment's GCS bucket.

### 4.3 Airflow Variables & Connections
Configure the following parameters in the Airflow UI (**Admin -> Variables**):

| Variable Name | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-dwh-gcp-1234` | The target GCP Project ID. |
| `BQ_DATASET` | `dwh_analytics` | The target BigQuery dataset name. |
| `GCS_BUCKET` | `us-central1-composer-bucket` | The GCS bucket hosting DAGs and SQL assets. |

Ensure that the Airflow Connection `google_cloud_default` is configured with the correct service account credentials (or uses ambient credentials if running inside Google Cloud).

---

## 5. Known Gaps & Unresolved References

### 5.1 B4 Redesign: Partition Column Verification
* **Risk:** The migrated SQL script queries the base table `dwh_ta_f_d1_twvv_tn` and filters on `gueltigkeitszeitpunkt`.
* **Action Required:** A database administrator must verify that `gueltigkeitszeitpunkt` is configured as the partition column for `dwh_ta_f_d1_twvv_tn` in BigQuery. If the table is partitioned on a different column (or not partitioned), this query will perform a full table scan, resulting in high query costs.

### 5.2 B4 Redesign: Centralized Audit Logging
* **Risk:** The legacy PL/SQL block called `dwpa_meldung.fehler` and `BERT_MELDUNG` procedures to log execution states and errors to an Oracle system table. In the migrated BigQuery SQL script, this is simulated via `SELECT FORMAT(...)` in the `EXCEPTION` block.
* **Action Required:** If your organization requires centralized audit logging in GCP, the `EXCEPTION` block in `d_abtn_x_smart_kubi.sql` must be modified to perform an `INSERT INTO` a centralized BigQuery logging table.

### 5.3 Legacy Logic Bug in `r_sqlscript.py`
* **Risk:** The legacy KornShell script contained a logical bug where it flagged an error (`ErrNr=198`) if the target SQL script *did* exist. The migrated `r_sqlscript.py` replicates this behavior exactly to preserve compatibility.
* **Action Required:** Review this logic with business stakeholders. If confirmed as a legacy bug, remove the existence-error check from `r_sqlscript.py`.

---

## 6. Validation

To validate the migration, perform the following tests in a non-production environment:

### 6.1 Standalone Script Validation
Run the standalone Python wrapper to verify that the BigQuery SQL script executes successfully:

```bash
export GCP_PROJECT="your-test-project"
export BQ_DATASET="your_test_dataset"

python3 d_abtn_x_smart_kubi_wrapper.py \
  --p-eintragsnr 1001 \
  --p-monats-id 202310 \
  --sql-file local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql
```

#### Expected "Passing" Result:
* The target table `dwh_ta_t_smart_kubi` is truncated.
* The query executes without syntax or runtime errors.
* The console outputs a log message indicating the number of rows inserted:
  `QueryResult: {'log_message': 'XXXX rows inserted in DWH$TA_T_SMART_KUBI'}`

### 6.2 Airflow DAG Validation
1. Upload `dwh_abtn_smart_kubi_dag.py` and the SQL script to the Cloud Composer DAGs folder.
2. In the Airflow UI, trigger the DAG manually using **Trigger DAG w/ config**.
3. Verify that the `execute_d_abtn_x_smart_kubi` task completes with a `success` status.
4. Check the task logs to ensure the dynamic `MONATSID` was calculated correctly based on the execution date.

---

## 7. Rollback Procedure

If issues are encountered post-go-live, execute the following steps to roll back to the legacy environment:

1. **Disable the Airflow DAG:**
   In the Airflow UI, toggle the switch for `dwh_abtn_smart_kubi_dag` to **Off** (Paused) to prevent further scheduled executions.
2. **Re-enable the UC4 Job:**
   In the UC4/Automic interface, locate the job `DW.DWH_ABTN_SMART_KUBI` and set its status to **Active (1)**.
3. **Data Cleanup (If Required):**
   If a failed or partial run corrupted the target table in the legacy database, log into the Oracle database and manually truncate the table:
   ```sql
   TRUNCATE TABLE DWH$TA_T_SMART_KUBI;
   ```
4. **Trigger Legacy Execution:**
   Manually trigger the UC4 job `DW.DWH_ABTN_SMART_KUBI` for the target reporting month to restore the data state.