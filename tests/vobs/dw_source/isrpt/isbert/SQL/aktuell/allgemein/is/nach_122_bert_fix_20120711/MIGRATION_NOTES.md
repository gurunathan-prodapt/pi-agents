# MIGRATION_NOTES.md

**Migration Target:** Google Cloud Platform (GCP) — BigQuery Standard SQL  
**Source Job:** Shared Files — `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711`  
**Migration Date:** October 2023  

---

## 1. Summary

The legacy Oracle SQL*Plus script `d_ausd_v_ta_vertrag_tmp.sql` has been migrated to **Google Cloud BigQuery Standard SQL**. 

The primary purpose of this script is to populate a temporary contracts table (`sof$ta_vertrag_tmp`) with consolidated contract configurations, status indicators, and upgrade eligibility flags. The migration translates Oracle-specific SQL*Plus variables, PL/SQL dynamic utility calls, and complex date arithmetic into a native, high-performance BigQuery Standard SQL Scripting block.

---

## 2. Generated Artifacts

The migration process has generated the following target file:

*   **File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/nach_122_bert_fix_20120711/d_ausd_v_ta_vertrag_tmp.sql`
*   **Role:** This is a unified BigQuery Standard SQL Scripting file. It encapsulates the entire execution lifecycle:
    1.  **Variable Declaration & Assignment:** Dynamically retrieves the reporting stichtag (`v_datum`) from the metadata table.
    2.  **Target Truncation:** Performs a native `TRUNCATE TABLE` operation on the temporary target.
    3.  **Data Consolidation & Insertion:** Executes a high-performance `INSERT INTO ... SELECT ... UNION ALL SELECT ...` query to populate the target table.

---

## 3. Key Design Decisions

### 3.1 Native BigQuery Scripting Block (`BEGIN ... END`)
*   **Approach:** The legacy SQL*Plus orchestration (using `DEFINE`, `COLUMN...NEW_VALUE`, and PL/SQL wrappers) was consolidated into a single BigQuery scripting block.
*   **Reasoning:** This avoids the need for external Python or bash wrappers to pass variables. BigQuery natively supports `DECLARE` and `SET` statements, allowing the script to remain self-contained and easily callable within Cloud Composer (Airflow).

### 3.2 Emulation of `MONTHS_BETWEEN`
*   **Approach:** Replaced Oracle's `MONTHS_BETWEEN(date1, date2)` with:
    ```sql
    DATE_DIFF(date1, date2, DAY) / 30.436875
    ```
*   **Reasoning:** BigQuery's native `DATE_DIFF(..., MONTH)` returns a truncated integer. Because the business logic evaluates fractional thresholds (e.g., checking if a contract has been active for strictly more than `9` or `23` months), integer truncation would lead to incorrect upgrade eligibility flags. Emulating the calculation using the average month length in days (`30.436875`) preserves floating-point precision.

### 3.3 Removal of Optimizer Hints and Client Settings
*   **Approach:** All Oracle parallel hints (e.g., `/*+ parallel(c,4) ... */`) and SQL*Plus environment settings (`WHENEVER SQLERROR`, `SPOOL`, `SET TIMING`) were stripped.
*   **Reasoning:** BigQuery automatically manages query parallelization and execution scaling. Client-side execution controls are handled natively by the GCP orchestrator.

### 3.4 Standardizing Conditional Logic
*   **Approach:** Converted all Oracle `DECODE` functions to ANSI-compliant `CASE WHEN` statements.
*   **Reasoning:** Improves code readability, maintainability, and ensures native compatibility with BigQuery's SQL engine.

---

## 4. Manual Steps Before Go-Live

Before deploying this script to production, the following setup steps must be completed in the target GCP environment:

### 4.1 Schema and Dataset Creation
Ensure that the target dataset (e.g., `isbert_schema`) and all referenced tables/views exist in BigQuery with compatible schemas:
*   `isbert_schema.dwtk_meldungen`
*   `sof$ta_vertrag_tmp` (Target Table)
*   All source tables/views: `sof$ta_cntrct_crs3`, `sof$ta_bp_ref`, `sof$ta_inv_acc`, `dwh$vi_s_rd_segment`, `sof$ta_notice`, `sof$ta_barrier_zusgf`, `sof$ta_cntrct_templ`, `sof$ta_cntrct_valid`, `sof$ta_period`, `sof$ta_vvl_upgrade`, `sof$ta_apn_ve`, `sof$ta_action_assoc`, and `sof$vi_c_bfc`.

### 4.2 IAM & Permissions
The service account executing this script (e.g., the Cloud Composer worker identity) requires:
*   `roles/bigquery.jobUser` on the GCP project.
*   `roles/bigquery.dataEditor` on the target dataset containing `sof$ta_vertrag_tmp`.
*   `roles/bigquery.dataViewer` on datasets containing the source tables.

### 4.3 Connection Strings & Secrets
The legacy Oracle DB Link (`@pcrs1`) has been retired. It is assumed that the Carmen source tables are replicated into the local BigQuery environment. Ensure that the data ingestion pipeline (e.g., via Fivetran, Airbyte, or BigQuery Omni) is fully operational and up-to-date.

### 4.4 Scheduling & DAG Integration
This script must be integrated into the parent Airflow DAG in Cloud Composer:
*   **Upstream Dependency:** Must run *after* the Carmen source tables and the metadata table `dwtk_meldungen` have finished their daily loads.
*   **Downstream Dependency:** Must run *before* downstream jobs `DW.BERT_AUSD_V_TA_P_VERTRAG` and `DW.BERT_AUSD_V_TA_VERTRAG_TMP` are triggered.

---

## 5. Known Gaps & Unresolved References

### 5.1 Downstream Job Status
*   **Gap:** The downstream consumers `DW.BERT_AUSD_V_TA_P_VERTRAG` and `DW.BERT_AUSD_V_TA_VERTRAG_TMP` are marked as **not yet migrated**.
*   **Mitigation:** End-to-end integration testing of the entire pipeline is blocked until these downstream components are migrated to GCP.

### 5.2 Boundary Date Discrepancies
*   **Gap:** The fractional month emulation (`DATE_DIFF / 30.436875`) is mathematically robust but may differ by a fraction of a day from Oracle's calendar-aware `MONTHS_BETWEEN` on leap years or specific month-end boundaries.
*   **Mitigation:** Flagged for human review during User Acceptance Testing (UAT). Compare the `upgradeberechtigt` flag output between Oracle and BigQuery for historical edge cases.

---

## 6. Validation

To validate the migrated script, execute the following testing protocol:

### 6.1 Execution Test
Run the compiled SQL script directly in the BigQuery console or via the `bq` CLI:
```bash
bq query --use_legacy_sql=false < d_ausd_v_ta_vertrag_tmp.sql
```
*   **Passing Criteria:** The script completes successfully with no syntax or runtime errors, and the query execution details show that rows were successfully inserted into `sof$ta_vertrag_tmp`.

### 6.2 Data Parity Test
1. Run the legacy Oracle script on a specific snapshot date and export the resulting `sof$ta_vertrag_tmp` table.
2. Run the migrated BigQuery script using the same snapshot data.
3. Compare the outputs using a checksum or row-by-row diff tool.
*   **Passing Criteria:** 100% parity in row counts and column values, specifically verifying that the `upgradeberechtigt` flag ('J'/'N') matches across both environments.

---

## 7. Rollback Procedure

In the event of a production failure or critical data mismatch:

1.  **Revert Orchestration:** Update the Cloud Composer/Airflow DAG to point back to the legacy Oracle database execution path.
2.  **Clean Target Environment:** If necessary, clear any partially written data in the BigQuery target table to prevent duplicate or dirty reads:
    ```sql
    TRUNCATE TABLE `sof$ta_vertrag_tmp`;
    ```
3.  **Investigate Logs:** Analyze the BigQuery Job History logs and Airflow task logs to identify the root cause of the failure.