# Migration Notes: DW.BERT_P_GESCHAEFTSP

This document details the migration of the `DW.BERT_P_GESCHAEFTSP` pipeline from its legacy Oracle-based environment to Google Cloud Platform (GCP).

---

## 1. Summary

The `DW.BERT_P_GESCHAEFTSP` pipeline extracts, transforms, and loads business partner master data (*Geschäftspartnerstammdaten*) and customer value-segments (*Kundenwert*) into the BERT Data Warehouse environment.

*   **Source Platform**: Oracle Database (PL/SQL), UC4 (Automic) Scheduler, KornShell (KSH) wrappers.
*   **Target Platform**: Google Cloud Platform (GCP).
    *   **Orchestration**: Cloud Composer (Apache Airflow).
    *   **Data Transformation**: Google Cloud Dataform (SQLX models compiling to BigQuery SQL).
    *   **Storage & Query Engine**: Google BigQuery.

---

## 2. Generated Artifacts

The following files have been generated to replace the legacy shell scripts, UC4 configurations, and PL/SQL scripts:

| File Path | Type | Role |
| :--- | :--- | :--- |
| `dags/dag_bert_p_geschaeftsp.py` | Python (Airflow DAG) | Orchestrates the execution flow. Replaces UC4 scheduling and KornShell wrappers (`r_ausd_geschaeftspartner.ksh`, `k_ausd_geschaeftspartner.ksh`). |
| `dataform/definitions/staging/sof$ta_segm_prem.sqlx` | Dataform (SQLX) | Extracts and stages premium segment associations from the CRM source. |
| `dataform/definitions/staging/sof$ta_bpr_dn_evn_his.sqlx` | Dataform (SQLX) | Filters and stages historical business partner relation instances based on validity dates. |
| `dataform/definitions/staging/sof$ta_bpr_dn_evn.sqlx` | Dataform (SQLX) | Resolves the latest active business partner relation instances from the staged history. |
| `dataform/definitions/core/sof$ta_p_gesch_part.sqlx` | Dataform (SQLX) | Transforms and loads the core Business Partner Master Data target table. |
| `dataform/definitions/core/sof$ta_p_dn_nutzer.sqlx` | Dataform (SQLX) | Transforms and loads the core Service Users target table. |
| `dataform/definitions/core/sof$ta_p_evn_empf.sqlx` | Dataform (SQLX) | Transforms and loads the core Itemized Bill (EVN) Recipients target table. |

---

## 3. Key Design Decisions

### 3.1. Decoupled Staging and Core Layers
Instead of running a single, massive PL/SQL script that performs multiple operations sequentially, the logic has been modularized into declarative Dataform models. This leverages BigQuery's columnar execution engine and provides clear lineage tracking within the Dataform UI.

### 3.2. Date and Parameter Handling
*   **Legacy Approach**: Shell scripts calculated dates using helper scripts (e.g., `h_alis_date.ksh`) and passed them as SQL*Plus substitution variables (`&v_datum`).
*   **Modernized Approach**: The Airflow DAG captures the execution date using the native Jinja template parameter `{{ ds_nodash }}` and passes it to Dataform as a compilation variable (`v_datum`). Inside the SQLX models, date parsing is standardized using BigQuery's `PARSE_DATE('%Y%m%d', ...)` and `FORMAT_DATE()`.

### 3.3. Elimination of Procedural Control Logic
Oracle-specific procedural commands, session settings, and transaction controls (`COMMIT`, `TRUNCATE`, `SPOOL`) were removed. Dataform natively manages table creation, replacement, and dependency ordering declaratively via `config` blocks and `${ref()}` statements.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed in the target GCP environment before triggering the pipeline:

### 4.1. BigQuery Dataset Creation
Ensure the target datasets exist in your designated region (e.g., `europe-west3`):
*   `bert_staging_sof`
*   `bert_staging_bpd`
*   `bert_staging_pds`
*   `bert_core`
*   `isbert_schema`

### 4.2. Ingestion of Upstream Tables
Ensure that upstream tables (previously accessed via Oracle DB links or local schemas) are replicated into BigQuery:
*   `isbert_schema.dwtk_meldungen`
*   `bert_staging_bpd.bpd$ta_bp_valueseg_assoc`
*   `bert_staging_pds.pds$ta_bpri_com`
*   `bert_staging_sof.sof$ta_e_reach_gp`
*   `bert_staging_sof.sof$ta_e_business_gp`
*   `bert_staging_sof.sof$ta_e_reach_dn`
*   `bert_staging_sof.sof$ta_e_business_dn`
*   `bert_staging_sof.sof$ta_e_reach_ev`
*   `bert_staging_sof.sof$ta_e_business_ev`

### 4.3. IAM & Permissions
*   The Cloud Composer Service Account must have the **Dataform Editor** (`roles/dataform.editor`) and **BigQuery Data Editor** (`roles/bigquery.dataEditor`) roles.
*   The Dataform Service Account must have **BigQuery Job User** (`roles/bigquery.jobUser`) and **BigQuery Data Editor** roles on the target datasets.

### 4.4. Airflow Variables
Define the following Airflow variables in the Composer environment:
*   `gcp_project_id`: The target GCP Project ID.
*   `dataform_repository`: The name of the Dataform repository.
*   `gcp_location`: The GCP region (e.g., `europe-west3`).

---

## 5. Known Gaps & Unresolved References

### 5.1. Unresolved Legacy Components
The following legacy components were referenced in the UC4/KSH scripts but had no corresponding source files in the codebase:
*   **`DW.BERT_LESE_LOG`**: Used for logging management.
    *   *Mitigation*: Replaced by native Airflow task execution logs and Google Cloud Logging (Stackdriver).
*   **`DW.HOLE_PFAD`**: Used for path initialization.
    *   *Mitigation*: Replaced by Airflow variables and environment-specific configuration parameters.

### 5.2. Redesign Items (B4)
*   **Incremental vs. Full Refresh**: The target tables (`sof$ta_p_gesch_part`, `sof$ta_p_dn_nutzer`, `sof$ta_p_evn_empf`) are currently configured as `{ type: "table" }` (full refresh). If the volume of business partner data grows significantly, these should be redesigned as `{ type: "incremental" }` models with appropriate merge keys.

---

## 6. Validation

To validate the migrated pipeline, execute the following tests:

### 6.1. Compilation Test
Run the Dataform compilation in the development workspace to ensure there are no syntax or dependency errors:
```bash
dataform compile
```

### 6.2. Dry-Run Execution
Execute a dry-run of the compilation result in BigQuery to verify query validity and estimate costs:
```bash
dataform run --dry-run
```

### 6.3. Data Reconciliation (Dual-Run)
1.  Run the legacy Oracle pipeline and the migrated GCP pipeline using the same logical date (`v_datum`).
2.  Compare row counts and column checksums between the Oracle target tables and the BigQuery target tables:
    *   `sof$ta_p_gesch_part`
    *   `sof$ta_p_dn_nutzer`
    *   `sof$ta_p_evn_empf`

---

## 7. Rollback Procedure

If a critical issue is discovered post-go-live, perform the following steps to roll back:

1.  **Pause the Airflow DAG**:
    Go to the Airflow UI and toggle the switch for `dag_bert_p_geschaeftsp` to **Off**.
2.  **Re-enable Legacy Scheduler**:
    Resume the `DW.BERT_P_GESCHAEFTSP` job in the UC4 (Automic) scheduler.
3.  **Data Cleanup (Optional)**:
    If target tables in BigQuery were partially or incorrectly populated, they can be truncated or restored to a previous state using BigQuery Time Travel:
    ```sql
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    ```