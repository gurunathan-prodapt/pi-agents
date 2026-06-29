# Migration Design — BERT_P_ADRESSEN

## 1. Purpose & Scope
The job `BERT_P_ADRESSEN` is a core data-warehousing orchestration job responsible for the **preparation and processing of master address data** (`Aufbereitung der Adressdaten`) within the `BERT` business domain (specifically part of the `DW.BERT_STAMMDATEN_JP` job plan).

### Business Context & Criticality
* **Core Entity**: Addresses represent a foundational customer/partner master data entity. Downstream systems and reporting rely on validated, cleaned, and standardized addresses for billing, communications, and customer relations.
* **Orchestration Context**: Runs as a UNIX-based background job under the user profile `DW.UNIX.ISBERT` on host `|DWHDWH2P|HOST`.
* **Execution Frequency & SLA**: It is a long-running process with an expected production run time of **6 hours** (and 4 hours in test environments). This suggests complex row-by-row matching, cleansing, deduplication, or high-volume history/SCD Type 2 processing.
* **Recovery**: The job is fully restartable at any execution point ("Restart jederzeit möglich") and utilizes coordination locks (Sync objects) against related business-partner (`DW.BERT_P_GESCHAEFTSP`) and billing-recipient (`DW.BERT_P_RECHEMPF`) jobs.

---

## 2. Source Inventory
The legacy job consists of the following technical artifacts:

| Source File / Component | Source Technology | Complexity Tier | Automation Bucket | Description / Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `DW.BERT_P_ADRESSEN.xml` | UC4 / Automic Job XML | Medium | `semi_auto` (65% rate) | Orchestration definition, environment setup, execution wrappers, and execution locks (Sync elements). |
| `r_ausd_adressen.ksh` | UNIX Korn Shell Script | Medium (Assumed) | `manual_review` | Physical script containing the underlying SQL transformations, file processing, or database utility executions. |
| `DW.HOLE_PFAD` | UC4 Include (`:inc`) | Low | `auto` | Path initialization helper. |
| `DW.BERT_LESE_LOG` | UC4 Include (`:inc`) | Low | `auto` | Log reading and exit-code validation helper. |

---

## 3. Target Architecture
The legacy job will be migrated to a fully native **Google Cloud Platform (GCP)** architecture, maximizing BigQuery’s parallel compute engine to reduce the 6-hour execution window.

```
                  ┌──────────────────────────────────────────────┐
                  │          Google Cloud Composer               │
                  │              (Airflow 2.x)                   │
                  │                                              │
                  │   ┌──────────────────────────────────────┐   │
                  │   │        dw_bert_p_adressen            │   │
                  │   │                 DAG                  │   │
                  │   └──────────────────┬───────────────────┘   │
                  └──────────────────────┼───────────────────────┘
                                         │
                                         ▼
                  ┌──────────────────────────────────────────────┐
                  │             BigQuery Engine                  │
                  │                                              │
                  │   ┌──────────────────────────────────────┐   │
                  │   │     sp_prep_adressen() (SPROC)       │   │
                  │   └──────────────────┬───────────────────┘   │
                  │                      │                       │
                  │                      ▼                       │
                  │   ┌──────────────────────────────────────┐   │
                  │   │           dw_bert.t_adressen         │   │
                  │   │          (Target BQ Table)           │   │
                  │   └──────────────────────────────────────┘   │
                  └──────────────────────────────────────────────┘
```

### Components Layout & Mapping
1. **Orchestration**: Mapped from **UC4** to **Google Cloud Composer** (Apache Airflow 2.x). 
   * A Python DAG file (`dw_bert_p_adressen.py`) will manage execution flow, dependency checks, and alert on failures.
2. **Execution Platform**: Sourcing shell environments and executing on legacy UNIX hosts will be replaced by serverless executions.
   * The core SQL transformations inside `r_ausd_adressen.ksh` will be rewritten as standard **BigQuery SQL** and executed via a **BigQuery Stored Procedure** (`dw_bert.sp_prep_adressen()`). This shift avoids the overhead of managing VM host nodes and scales compute on demand.
3. **Service Accounts**: Run under a secure GCP Service Account:
   * `sa-bert-dwh-prod@<gcp-project>.iam.gserviceaccount.com` mapped with role permissions `roles/bigquery.admin` and `roles/composer.worker`.

---

## 4. Data Flow & Lineage
The data flow encompasses upstream validation, concurrency checking, logic execution, and destination target table logging.

```
   Upstream Tasks Completed
               │
               ▼
┌──────────────────────────────┐
│  guard_active_run (Task)     │ <--- Check Active Run (Else=Skip)
└──────────────┬───────────────┘
               │ Pass
               ▼
┌──────────────────────────────┐
│  dw_bert_p_adressen_run      │ <--- Executes BigQuery Stored Proc
└──────────────┬───────────────┘
               │
               ├──────────────────────────────┐
               ▼                              ▼
┌──────────────────────────────┐┌──────────────────────────────┐
│      dw_bert.t_adressen      ││       Cloud Logging / GCS    │
│      (Target BigQuery)       ││      (Post-Execution Log)    │
└──────────────────────────────┘└──────────────────────────────┘
```

### Execution Order & Execution Locks (Sync Objects)
* **`DW.BERT_ADRESS_SYNC` (Start: BLOCK, End: RELEASE, Else: Skip)**:
  * *Legacy behavior*: If another run of `BERT_P_ADRESSEN` or a related process holds this lock, the execution is skipped.
  * *Airflow mapping*: Handled via a dynamic custom `PythonOperator` (`guard_active_run`) that queries active Airflow DAG runs. If an active run is detected, it raises `AirflowSkipException` to terminate current DAG progress safely.
* **`DW.BERT_GP_SYNC` (Start: BLOCK, End: RELEASE, Else: Wait)**:
  * *Legacy behavior*: Waits if business partner preparation is running.
  * *Airflow mapping*: Enforced using DAG execution sequence dependencies (making the business partner DAG a direct upstream parent) or via an `ExternalTaskSensor` checking for successful execution of `dw_bert_p_geschaeftsp`.
* **`DW.BERT_RECH_SYNC` (Start: BLOCK, End: RELEASE, Else: Wait)**:
  * *Legacy behavior*: Waits if invoice-recipient processing is running.
  * *Airflow mapping*: Enforced via an `ExternalTaskSensor` watching the `dw_bert_p_rechempf` DAG.

---

## 5. Transformation Logic

### Wrapper Translation
* **Legacy (`. $HOME/.dw_init`)**: Sourced global environment definitions and connections.
  * *Target*: Replaced by Airflow Connection parameters (`gcp_conn_id`) and Airflow Variables storing environment config details (`gcp_project_id`, `gcs_bucket_name`).
* **Legacy (`:inc DW.HOLE_PFAD`)**: Defined workspace variables and local SQL scripts location.
  * *Target*: Replaced by a parameterized storage path in Cloud Storage (`gs://<bucket_name>/dags/sql/`).

### Script Body Translation (`r_ausd_adressen.ksh`)
The script is responsible for executing the transformation and load procedures for address master data.
The transformation steps must be converted from legacy Oracle SQL/PLSQL to native **Google BigQuery SQL**:

1. **Staging / Initial Filtering**:
   * Filter inputs from the primary address tables, capturing delta changes since the last run.
   * *BigQuery Equivalent*:
     ```sql
     CREATE OR REPLACE TEMP TABLE temp_address_deltas AS (
       SELECT adr.* 
       FROM `dw_bert_staging.stg_addresses` adr
       WHERE adr.last_modified_timestamp > (
         SELECT COALESCE(MAX(last_run_timestamp), TIMESTAMP('1970-01-01')) 
         FROM `dw_bert.metadata_job_runs` 
         WHERE job_name = 'BERT_P_ADRESSEN' AND status = 'SUCCESS'
       )
     );
     ```
2. **Standardization / Cleansing**:
   * Standardize street addresses, formatting phone numbers, casing, and correcting zip codes using BigQuery SQL functions (`REGEXP_REPLACE`, `SUBSTR`, `TRIM`, `UPPER`).
3. **Historization / SCD Type 2 Update**:
   * Address datasets typically require tracking changes over time (SCD Type 2).
   * *BigQuery Equivalent (Merge Syntax)*:
     ```sql
     MERGE `dw_bert.t_adressen` T
     USING temp_address_deltas S
     ON T.address_id = S.address_id AND T.is_current = TRUE
     WHEN MATCHED AND (T.street_name != S.street_name OR T.postal_code != S.postal_code OR T.city != S.city) THEN
       UPDATE SET T.valid_to = CURRENT_TIMESTAMP(), T.is_current = FALSE;
       
     -- Followed by inserting the new current records:
     INSERT INTO `dw_bert.t_adressen` (address_id, street_name, postal_code, city, valid_from, valid_to, is_current)
     SELECT address_id, street_name, postal_code, city, CURRENT_TIMESTAMP(), TIMESTAMP('9999-12-31'), TRUE
     FROM temp_address_deltas;
     ```

### Logging Wrapper Translation (`:inc DW.BERT_LESE_LOG`)
* **Legacy**: Handled the extraction of rows processed and caught execution warnings.
* **Target**: Airflow natively logs execution metadata. However, explicitly writing execution states to a centralized audit schema is best practice:
  ```sql
  INSERT INTO `dw_bert.metadata_job_runs` (job_name, start_time, end_time, status, rows_affected)
  VALUES ('BERT_P_ADRESSEN', start_ts, CURRENT_TIMESTAMP(), 'SUCCESS', row_count);
  ```

---

## 6. External Dependencies

| Legacy Dependency | Target Architecture Replacement | Details |
| :--- | :--- | :--- |
| **UNIX Host (`|DWHDWH2P|HOST`)** | Serverless BigQuery Compute Engine | Eliminates the need to maintain, patch, and manage physical VMs. |
| **Unix User Login `DW.UNIX.ISBERT`** | GCP Service Account IAM | Permissions managed strictly via a service account with minimal privileges (`roles/bigquery.jobUser`, `roles/storage.objectViewer`). |
| **Oracle DB Connections** | BigQuery Connection | Native Airflow `BigQueryHook` using GCP client libraries. |
| **UC4 Sync Objects** | Airflow DAG Controls & Custom Guard Tasks | Replaces centralized sync locks with flexible DAG dependency constraints and programmatic Airflow sensors. |

---

## 7. Unresolved / Risks

### Risks & Mitigations
1. **Missing Source Shell Script Details (B4 Redesign Item)**:
   * *Risk*: The physical content of `r_ausd_adressen.ksh` was not included in the pre-collected context, which might contain complex post-processing commands, export scripts, or local file triggers.
   * *Mitigation*: This shell script must be fetched from disk on the source UNIX environment (`$HOME/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh`) and mapped directly. If it uses external utility tools (like address validation software), they must be replaced with APIs or Python modules running on Cloud Run.
2. **Synchronization Overlap (Race Conditions)**:
   * *Risk*: High frequency or manual parallel starts could bypass the `Else=Skip` logic if not implemented with strict transaction guarantees.
   * *Mitigation*: Ensure the custom `guard_active_run` task executes first as an absolute gatekeeper and utilizes Composer's SQL-backed state database dynamically to query for any active run IDs.
3. **Data Volumes and Performance**:
   * *Risk*: Processing master address history for the entire enterprise could require significant compute.
   * *Mitigation*: Leverage BigQuery partitioned tables on the target side (e.g., partitioning by `valid_from` or hashing key ranges) and cluster tables by `address_id` to speed up join operations.

---

## 8. Build Plan
The following ordered build plan details the implementation steps:

```
  ┌─────────────────────────────────────────────────────────────┐
  │ 1. Deploy Staging & Target Tables in BigQuery               │
  │    (Language: SQL - DDL)                                    │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ 2. Create BigQuery Stored Procedure `sp_prep_adressen`      │
  │    (Language: BigQuery SQL)                                 │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ 3. Build & Package Common Log/Utility Modules               │
  │    (Language: Python)                                       │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │ 4. Generate Cloud Composer Orchestration DAG                │
  │    `dw_bert_p_adressen.py` (Language: Python / Airflow API) │
  └─────────────────────────────────────────────────────────────┘
```

1. **Step 1: BigQuery Target DDL Creation**
   * *Language*: SQL
   * *Output*: Generate the target schema and tables for `dw_bert.t_adressen` (including SCD2 parameters) and standard metadata log tables.
2. **Step 2: BigQuery Stored Procedure Development**
   * *Language*: BigQuery SQL / SQL Scripting
   * *Output*: Translate the full extraction, cleaning, and historicized merge logic of the legacy shell code `r_ausd_adressen.ksh` into `dw_bert.sp_prep_adressen()`.
3. **Step 3: Common Log & Utility Classes**
   * *Language*: Python
   * *Output*: Deploy the cloud-native logging helper modules (replacing `DW.BERT_LESE_LOG`) to standard GCP Cloud Logging.
4. **Step 4: Orchestration DAG Code (`dw_bert_p_adressen.py`)**
   * *Language*: Python (Airflow SDK)
   * *Output*: Orchestrates the execution:
     * Task 1: `guard_active_run` (Custom Python guard operator simulating the `Else=Skip` sync check).
     * Task 2: `sensor_gp` and `sensor_rech` (Airflow `ExternalTaskSensor` tasks simulating `DW.BERT_GP_SYNC` and `DW.BERT_RECH_SYNC` wait semantics).
     * Task 3: `dw_bert_p_adressen_run` (`BigQueryExecuteQueryOperator` executing the stored procedure developed in Step 2).