# Migration Notes: `ausd_bp_ta_apn_vertrag`

This document provides comprehensive migration notes for the job `ausd_bp_ta_apn_vertrag`, which has been transitioned from a legacy Oracle/UC4 environment to Google Cloud Platform (BigQuery and Cloud Composer/Airflow).

---

## 1. Summary
The job **`ausd_bp_ta_apn_vertrag`** is a key component of the `BERT Stammdaten` (product master data) business domain. Its primary function is to aggregate and pivot Access Point Names (APNs) and contract references per contract ID. This aggregated cache is utilized downstream for credit scoring and fraud analysis (BERT/FOS).

*   **Source Platform:** Oracle Database, KornShell (ksh) scripts, and UC4 (Automic) Scheduler.
*   **Target Platform:** Google Cloud BigQuery and Google Cloud Composer (Apache Airflow).
*   **Migration Scope:** 
    *   Replaced Oracle PL/SQL cursor loops with a native BigQuery SQL query utilizing a JavaScript User-Defined Function (UDF).
    *   Replaced KornShell framework and control scripts (`r_...` and `k_...`) with an Airflow DAG.
    *   Replaced UC4 orchestration with native Airflow scheduling.
    *   Migrated target table `sof$ta_apn_vertrag` to BigQuery table `sof_ta_apn_vertrag`.

---

## 2. Generated Artifacts

The migration process generated the following artifacts, located in the deployment repository:

| Artifact Path | Type | Role / Description |
| :--- | :--- | :--- |
| `src/ddl/sof_ta_apn_vertrag.sql` | DDL SQL | Creates the target BigQuery table `sof_ta_apn_vertrag` with appropriate column descriptions and schema definitions. |
| `src/sql/d_ausd_bp_ta_apn_vertrag.sql` | DML SQL | Contains the core transformation logic. Uses a temporary JavaScript UDF to aggregate APNs and contract references while strictly enforcing the legacy 100-character limit without truncating words. |
| `src/dags/dw_bert_ausd_bp_ta_apn_vertrag.py` | Python (Airflow) | Orchestrates the execution of the BigQuery transformation. Defines the DAG, sets up environment variables, and executes the SQL logic. |

---

## 3. Key Design Decisions

### JavaScript UDF for Strict Functional Parity (Option B)
*   **Decision:** Implement a temporary JavaScript UDF (`aggregate_limited`) in BigQuery to handle string concatenation.
*   **Reasoning:** The legacy Oracle PL/SQL script iterated through records and appended values up to a strict limit of 100 characters. If a value caused the string to exceed 100 characters, it was skipped entirely, but subsequent shorter values could still be appended if they fit. A standard SQL `STRING_AGG` combined with `SUBSTR` (Option A) would truncate strings mid-word, violating functional parity. The JS UDF perfectly mirrors the legacy cursor's element-fitting logic.
*   **Trade-off:** JavaScript UDFs introduce a minor performance overhead compared to native BigQuery SQL functions. However, given the relatively small dataset size of the staging table (`sof_ta_bpr_apn`), this overhead is negligible and justified by the requirement for 100% functional parity.

### Retirement of Shell Wrappers and UC4 XML
*   **Decision:** Completely retire `r_ausd_bp_ta_apn_vertrag.ksh`, `k_ausd_bp_ta_apn_vertrag.ksh`, and the UC4 XML definition.
*   **Reasoning:** Airflow natively handles logging, error trapping, retries, and parameter passing. Retaining shell wrappers would introduce unnecessary complexity and run counter to cloud-native best practices.

### Elimination of Dead Code
*   **Decision:** Removed legacy references to the audit table `isbert_schema.dwtk_meldungen` and the database link `@pcrs1`.
*   **Reasoning:** These elements were remnants of deprecated staging and partitioning strategies that are obsolete in BigQuery's serverless architecture.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated workflow in production, complete the following administrative and configuration steps:

### A. Schema & Dataset Creation
Ensure the target BigQuery dataset exists in your project. If not, create it using the `bq` CLI or Google Cloud Console:
```bash
bq mk --location=EU your_dataset
```
Execute the DDL script to create the target table:
```bash
bq query --use_legacy_sql=false < src/ddl/sof_ta_apn_vertrag.sql
```

### B. IAM & Permissions
The Cloud Composer environment's service account must have the following IAM roles assigned:
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target dataset.
*   **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level.

### C. Airflow Variables Configuration
The DAG relies on Airflow Variables for environment-specific configurations. Define the following variables in the Airflow UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `gcp_project` | `your-gcp-project-id` | The target Google Cloud Project ID. |
| `gcp_dataset` | `your_dataset_name` | The target BigQuery dataset name. |
| `gcp_location` | `EU` | The geographic location of the BigQuery dataset. |

### D. Scheduling & Upstream Dependencies
The DAG is currently configured with `schedule_interval=None`. 
*   **Action Required:** Coordinate with the orchestration team to trigger this DAG automatically upon the successful completion of the upstream staging load DAG for `sof_ta_bpr_apn`. This can be achieved using an `ExternalTaskSensor` or by updating the schedule to use Airflow Dataset-based scheduling.

---

## 5. Known Gaps & Unresolved References

### Legacy 100-Character Limit (Flagged for B4 Redesign)
*   **Issue:** The 100-character limit on `apn` and `cntrct_ref` columns is a legacy constraint from 2001, designed to save relational database storage space. In BigQuery, string fields can natively hold up to 10MB of data without performance degradation.
*   **Risk:** Downstream credit scoring models might miss critical fraud signals if APN records are omitted because they exceeded the 100-character threshold.
*   **Recommendation (Follow-up):** Align with business stakeholders and downstream consumers to lift this limit. If approved, the JS UDF can be retired in favor of a standard, high-performance `STRING_AGG` query without length constraints.

---

## 6. Validation

To validate the migration, execute the following testing procedures:

### A. DAG Parsing Test
Verify that the Airflow DAG is syntactically correct and can be loaded by the scheduler:
```bash
python3 src/dags/dw_bert_ausd_bp_ta_apn_vertrag.py
```
*Passing criteria:* The command exits with code `0` without throwing any import or syntax errors.

### B. Functional Parity Test (Dry Run)
1. Populate the staging table `sof_ta_bpr_apn` with a controlled set of test records, including:
    *   A contract with no APNs.
    *   A contract with multiple APNs whose combined length is under 100 characters.
    *   A contract with multiple APNs whose combined length exceeds 100 characters (to test the JS UDF fitting logic).
2. Execute the query in `src/sql/d_ausd_bp_ta_apn_vertrag.sql`.
3. Compare the output in `sof_ta_apn_vertrag` against the legacy Oracle execution output.

*Passing criteria:* 
*   Row counts match exactly.
*   For contracts exceeding 100 characters, the aggregated string contains only complete elements that fit within the limit, matching the legacy Oracle output character-for-character.

---

## 7. Rollback Procedure

In the event of a critical failure or data mismatch post-go-live, execute the following steps to roll back to the legacy environment:

1.  **Pause the Airflow DAG:**
    Go to the Airflow UI and toggle the switch for `dw_bert_ausd_bp_ta_apn_vertrag` to **Off**.
2.  **Re-enable Legacy UC4 Job:**
    Un-pause/re-enable the legacy UC4 job definition `DW.BERT_AUSD_BP_TA_APN_VERTRAG`.
3.  **Redirect Downstream Consumers:**
    If downstream processes were already pointed to BigQuery, redirect them back to the legacy Oracle table `sof$ta_apn_vertrag`.
4.  **Clean Up Target Table (Optional):**
    If required to prevent accidental reads of stale data, truncate the BigQuery target table:
    ```sql
    TRUNCATE TABLE `your_project.your_dataset.sof_ta_apn_vertrag`;
    ```