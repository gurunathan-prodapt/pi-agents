# Migration Notes: DW.CCM_WRITE_CONTRACTMAPLOOKUP

This document details the migration of the UC4 UNIX job `DW.CCM_WRITE_CONTRACTMAPLOOKUP` and its underlying Ab Initio graph `BHB_CCM_PROC_WriteContractMapLookup.mp` to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy standalone UC4 UNIX job `DW.CCM_WRITE_CONTRACTMAPLOOKUP` has been migrated to a modern cloud-native architecture. 

* **Source Platform:** UC4 (Orchestration) + Ab Initio GDE/Co>Operating System (ETL) running on an on-premise Unix host (`dwhdwh2p`).
* **Target Platform:** Google Cloud Platform (GCP) utilizing **Cloud Composer (Apache Airflow)** for orchestration and **Dataproc Serverless (PySpark)** for data processing.
* **Data Warehouse Target:** **BigQuery** (replacing Oracle).
* **Storage Target:** **Google Cloud Storage (GCS)** (replacing local Unix filesystems).

The core business logic extracts contract mapping records from a database, sorts them by `vertrags_id`, materializes them into a delimited lookup file, and registers the load execution window using a metadata stored procedure.

---

## 2. Generated Artifacts

The migration process generated the following key artifacts:

| Artifact Path | Language / Type | Role |
| :--- | :--- | :--- |
| `dags/vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/CCM_PROC/PRODUKTION/DW.CCM_PROC_JP/dw_ccm_write_contractmaplookup.py` | Python / Airflow DAG | Orchestrates the execution of the migrated job. It defines a single-task DAG that submits the PySpark job to Dataproc Serverless. |
| `vobs/dw_source/isdwh/abinitio/ccm_proc/run/BHB_CCM_PROC_WriteContractMapLookup.py` | Python / PySpark | Replaces both the legacy KornShell wrapper (`.ksh`) and the Ab Initio graph (`.mp`). It extracts data from BigQuery, sorts it, writes the delimited file to GCS, and calls the BigQuery metadata stored procedure. |

---

## 3. Key Design Decisions

### 3.1. Compute Engine: Dataproc Serverless (PySpark)
* **Decision:** Convert the Ab Initio graph (`.mp`) directly into a PySpark application running on Dataproc Serverless.
* **Trade-off / Reason:** Dataproc Serverless eliminates the need to manage VM clusters, scales dynamically, and integrates natively with BigQuery and GCS. PySpark natively handles the sorting and formatting operations previously performed by Ab Initio's `Sort` and `Reformat` components.

### 3.2. Orchestration: Airflow DAG with `schedule=None`
* **Decision:** The migrated Airflow DAG is configured with `schedule=None`.
* **Trade-off / Reason:** The original UC4 job was a sub-module of the parent Job Plan `DW.CCM_PROC_JP` (which is not yet migrated). Keeping the schedule as `None` prevents accidental standalone runs and allows it to be triggered dynamically or embedded as a `TriggerDagRunOperator` / Task Group inside the parent DAG once migrated.

### 3.3. Storage: GCS Delimited Files
* **Decision:** The output lookup file is written directly to a GCS bucket using PySpark's CSV writer with the `\x01` (SOH) delimiter, matching the legacy Ab Initio DML specification.
* **Trade-off / Reason:** GCS provides highly durable, shared cloud storage accessible by downstream cloud processes, replacing the local Unix filesystem.

### 3.4. Metadata Update: BigQuery Stored Procedure
* **Decision:** The legacy Oracle PL/SQL call `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` is replaced by a BigQuery SQL Stored Procedure call using the Google Cloud BigQuery Python client.
* **Trade-off / Reason:** This maintains transactional metadata tracking within the target data warehouse environment without requiring an external database connection.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed in the target GCP environment before executing the migrated workflow:

### 4.1. Schema and Dataset Creation
1. Ensure the source table `DWH_TA_L_MAP_VT_CARM_DWH` is migrated and populated in the target BigQuery dataset (e.g., `DWH`).
2. Deploy the migrated BigQuery Stored Procedure `SetzeLadedatumAbInitio` within the target BigQuery dataset.

### 4.2. IAM & Permissions
Ensure the Service Account executing the Cloud Composer worker and Dataproc Serverless workloads has the following IAM roles:
* **BigQuery:** `roles/bigquery.dataViewer` (on the source table) and `roles/bigquery.jobUser` (to execute the stored procedure).
* **Cloud Storage:** `roles/storage.objectAdmin` on the target GCS bucket.
* **Dataproc:** `roles/dataproc.worker` and `roles/dataproc.editor`.

### 4.3. Airflow Variables & Connections
Configure the following Airflow Variables in the Cloud Composer environment:

| Variable Name | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-id` | The target GCP Project ID. |
| `GCS_BUCKET` | `my-dwh-data-bucket` | The GCS bucket where the output lookup file will be written. |
| `BQ_DATASET` | `DWH` | The BigQuery dataset containing the source table and stored procedure. |

### 4.4. Environment Variables
The PySpark script expects the following environment variables (which can be passed via the Airflow Dataproc operator or set globally):
* `BHB_CCM_PROC_TargetObjectName` (Default: `ContractMapLookup.txt`)
* `BHB_CCM_PROC_FirstDay` (Default: `20050217`)
* `BHB_CCM_PROC_LastDayPlus1` (Default: `20050218`)

---

## 5. Known Gaps & Unresolved References

* **Downstream Orchestrator (`DW.CCM_PROC_JP`):** The parent UC4 Job Plan has not yet been migrated. The migrated DAG `dw_ccm_write_contractmaplookup` must be integrated into the parent workflow DAG once that migration pass is completed.
* **Stored Procedure Logic:** The internal PL/SQL logic of `DWH$PA_ALIS_OBJEKT.SetzeLadedatumAbInitio` was not part of this bundle. It must be manually translated to BigQuery SQL and deployed to the target dataset before go-live.
* **Template Sourcing:** The legacy script sourced `.dw_init` and `.dw_global` for environment setup. The migrated Python script attempts to import these from the migrated `istools/seu/template` directory. Ensure these template files are present in the Python path or Composer environment.

---

## 6. Validation

To validate the migration, perform the following steps in a lower environment (e.g., UAT):

### 6.1. Execution
1. Upload the PySpark script `BHB_CCM_PROC_WriteContractMapLookup.py` to your environment's GCS code bucket.
2. Trigger the Airflow DAG manually via the Airflow UI or CLI:
   ```bash
   airflow dags trigger dw_ccm_write_contractmaplookup
   ```

### 6.2. Definition of "Passing"
The validation is successful if:
1. The Airflow DAG run completes with a status of `SUCCESS`.
2. A folder/file is created in GCS at `gs://{GCS_BUCKET}/ccm_proc/ContractMapLookup.txt/` containing the extracted records.
3. The output file is verified to be:
   * Sorted by `vertrags_id` in ascending order.
   * Delimited by the `\x01` character.
4. The BigQuery job history logs a successful execution of the stored procedure:
   ```sql
   CALL `DWH.SetzeLadedatumAbInitio`('ContractMapLookup.txt', '20050217', '20050218')
   ```

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or go-live, use the following steps to roll back to the legacy on-premise system:

1. **Pause Airflow DAG:** Pause the migrated DAG in the Cloud Composer UI to prevent further cloud executions:
   ```bash
   airflow dags pause dw_ccm_write_contractmaplookup
   ```
2. **Re-enable UC4 Job:** In the UC4 client, locate the job `DW.CCM_WRITE_CONTRACTMAPLOOKUP` and set its status back to Active (`active=1`).
3. **Verify On-Premise Environment:** Ensure that the legacy Oracle database and the on-premise Unix filesystem paths are intact and synchronized.
4. **Data Reconciliation:** If the cloud job executed partially, verify if any downstream systems consumed the GCS file. If necessary, manually copy the legacy-generated `ContractMapLookup.txt` from the on-premise server to GCS to ensure downstream cloud processes do not fail.