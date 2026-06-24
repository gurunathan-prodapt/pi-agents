# Migration Design — DW.BERT_AUSD_BP_TA_APN_VERTRAG

## 1. Purpose & Scope
This job, `DW.BERT_AUSD_BP_TA_APN_VERTRAG`, is an Oracle PL/SQL script designed to process and aggregate Access Point Name (APN) and contract reference data. It reads from the `sof$ta_bpr_apn` source table, consolidates APN values and contract IDs based on a contract identifier, and then inserts the aggregated results into the `sof$ta_apn_vertrag` target table. The core logic involves iterating through records using a cursor and dynamically building concatenated strings for APNs and contract references for each unique contract.

## 2. Source Inventory
The job is composed of a single Oracle PL/SQL script:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql`
    *   **Technology:** Oracle PL/SQL
    *   **Category:** SQL
    *   **Tier:** Unknown (Complexity analysis data was not available)
    *   **Automation Bucket:** B3 (manual)
    *   **Summary:** This Oracle PL/SQL script processes APN (Access Point Name) and contract reference data from a source table, aggregates them, and inserts the results into a target table. It uses a cursor-based loop for row-by-row processing.

## 3. Target Architecture
The migration target is Google Cloud's BigQuery platform. The Oracle PL/SQL script will be translated into a BigQuery-compatible SQL script.

*   **Target Tables:**
    *   `sof$ta_bpr_apn` (Oracle) will be migrated to a corresponding BigQuery table, e.g., `your_project.your_dataset.SOFTA_BPR_APN`.
    *   `sof$ta_apn_vertrag` (Oracle) will be migrated to a corresponding BigQuery table, e.g., `your_project.your_dataset.SOFTA_APN_VERTRAG`.
*   **Transformation Logic:** The procedural PL/SQL logic, particularly the cursor-based `FOR` loop and string concatenation, will be re-engineered into a set-based BigQuery SQL query. This will typically involve using BigQuery's advanced aggregation functions (e.g., `STRING_AGG`) combined with `GROUP BY` clauses.
*   **Data Handling:** `TRUNCATE TABLE` operations will be replaced with BigQuery SQL `TRUNCATE TABLE` statements or managed as `WRITE_TRUNCATE` disposition in BigQuery load jobs. `COMMIT` statements are not directly applicable in BigQuery's atomic DML context.

## 4. Data Flow & Lineage
The original script performs a direct read from `sof$ta_bpr_apn` and a write to `sof$ta_apn_vertrag`. No explicit lineage edges beyond these direct table interactions were identified during the analysis, suggesting a standalone processing step within its original environment.

**Original Data Flow:**
1.  **Input:** `sof$ta_bpr_apn` (Oracle table)
2.  **Processing:** `d_ausd_bp_ta_apn_vertrag.sql` (Oracle PL/SQL script)
    *   Reads `cntrct_id_ref`, `bpr_id`, `cntrct_id`, and `access_point_name` columns.
    *   Aggregates `access_point_name` and `cntrct_id_ref` values per `cntrct_id`.
    *   Truncates `sof$ta_apn_vertrag`.
    *   Inserts the aggregated data into `sof$ta_apn_vertrag`.
3.  **Output:** `sof$ta_apn_vertrag` (Oracle table)

**Target BigQuery Data Flow:**
1.  **Input:** `your_project.your_dataset.SOFTA_BPR_APN` (BigQuery table)
2.  **Processing:** Migrated BigQuery SQL script
    *   Reads necessary columns from `SOFTA_BPR_APN`.
    *   Performs set-based aggregation of APN and contract reference data using `STRING_AGG` and `GROUP BY`.
    *   Truncates `SOFTA_APN_VERTRAG` (if implemented as a separate step) or overwrites it.
    *   Inserts the transformed data into `SOFTA_APN_VERTRAG`.
3.  **Output:** `your_project.your_dataset.SOFTA_APN_VERTRAG` (BigQuery table)

## 5. Transformation Logic

**Original Oracle PL/SQL Core Logic:**
```sql
DECLARE
  v_cntrct       varchar2(10)  := null;
  v_cntrct_ref   varchar2(100) := null;
  v_apn          varchar2(100) := null;
  -- ... (other declarations and variable assignments) ...
BEGIN
  isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_apn_vertrag');

  FOR rec_cn IN (SELECT cntrct_id_ref, bpr_id, cntrct_id, access_point_name FROM sof$ta_bpr_apn ORDER BY cntrct_id)
  LOOP
    IF v_cntrct     != rec_cn.cntrct_id THEN
      -- Insert accumulated data for previous contract
      INSERT INTO sof$ta_apn_vertrag VALUES (v_cntrct, substr(rtrim(v_apn,', '),1,100),substr(rtrim(v_cntrct_ref,', '),1,100));
      -- Reset for new contract
      v_cntrct      := rec_cn.cntrct_id;
      v_cntrct_ref  := null;
      v_apn         := null;
    END IF;

    IF v_cntrct IS NULL THEN -- Handle first record
      v_cntrct := rec_cn.cntrct_id;
    END IF;

    -- Concatenate APNs and contract references
    IF LENGTH(v_apn||rec_cn.access_point_name||', ') <= 100 THEN
      v_apn := v_apn||rec_cn.access_point_name||', ';
    END IF;

    IF LENGTH(v_cntrct_ref||rec_cn.cntrct_id_ref||', ') <= 100 THEN
      v_cntrct_ref := v_cntrct_ref||rec_cn.cntrct_id_ref||', ';
    END IF;
  END LOOP;
  -- Final insert for the last contract
  INSERT INTO sof$ta_apn_vertrag VALUES (v_cntrct, substr(rtrim(v_apn,', '),1,100),substr(rtrim(v_cntrct_ref,', '),1,100));
  COMMIT;
END;
/
```

**Proposed BigQuery SQL Transformation:**

The procedural loop will be replaced by a single `INSERT` statement with `GROUP BY` and `STRING_AGG` functions, which are highly optimized for this type of aggregation in BigQuery.

```sql
-- Step 1: Truncate the target table in BigQuery.
-- This can be a standalone DDL statement or part of a data loading job configuration (WRITE_TRUNCATE).
TRUNCATE TABLE `your_project.your_dataset.SOFTA_APN_VERTRAG`;

-- Step 2: Insert aggregated data into the target BigQuery table.
INSERT INTO `your_project.your_dataset.SOFTA_APN_VERTRAG` (
    cntrct_id,
    aggregated_apn,
    aggregated_cntrct_ref
)
SELECT
    cntrct_id,
    -- Concatenate APNs, ordering is important for consistent output if not guaranteed by source.
    -- SUBSTR and RTRIM mimic the original logic for length and trailing comma.
    SUBSTR(RTRIM(STRING_AGG(access_point_name, ', ' ORDER BY access_point_name), ', '), 1, 100) AS aggregated_apn,
    -- Concatenate contract references; casting to STRING is necessary for STRING_AGG if not already STRING.
    SUBSTR(RTRIM(STRING_AGG(CAST(cntrct_id_ref AS STRING), ', ' ORDER BY cntrct_id_ref), ', '), 1, 100) AS aggregated_cntrct_ref
FROM
    `your_project.your_dataset.SOFTA_BPR_APN`
GROUP BY
    cntrct_id;
```

## 6. External Dependencies
*   **Oracle Database:** The original script is deeply integrated with an Oracle database environment, relying on Oracle-specific SQL and PL/SQL syntax, functions, and table structures. This dependency will be completely removed by migrating all referenced data to BigQuery.
*   **`isbert_schema.DWPA_UTIL_SKRIPT`:** This appears to be a custom Oracle stored procedure or package used to execute dynamic SQL (e.g., `TRUNCATE TABLE`). In BigQuery, this functionality will be replaced by native BigQuery DDL statements (`TRUNCATE TABLE`) or by configuring the BigQuery load job disposition. The exact scope of this utility should be confirmed; if it performs actions beyond simple DDL, those actions will need specific BigQuery equivalents or custom re-implementation.
*   **`isbert_schema.dwtk_meldungen`:** This table is referenced in a `SELECT` statement to derive a `v_datum` variable. While its direct use in the active part of the provided script is minimal (primarily for commented-out table naming), if this table holds critical metadata or control parameters, it should be migrated to BigQuery (e.g., `your_project.your_dataset.DWTK_MELDUNGEN`) and its relevant data utilized.
*   **`v_carmen` variable (`@pcrs1`):** The variable `v_carmen` is assigned `@pcrs1`, which typically indicates a database link or connection string in Oracle. Since no external systems were found in the job's metadata, it's likely an internal Oracle reference or a remnant. Its purpose needs to be fully clarified. If it points to external data, that data source will need to be connected to BigQuery (e.g., via federated queries, data transfer services, or batch ingestion).

## 7. Unresolved / Risks
*   **`file_complexity` data:** The absence of detailed complexity analysis (tier, migration flags) from `file_complexity` means that the estimated effort and specific challenges are based solely on manual code review and general knowledge of PL/SQL to BigQuery migration. This may lead to underestimation of effort.
*   **Full `DWPA_UTIL_SKRIPT` functionality:** The current analysis assumes `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` is primarily for `TRUNCATE TABLE`. If it performs more complex or critical operations, these will need to be thoroughly understood and re-implemented in BigQuery SQL, Python, or another appropriate language.
*   **Data Type and Length Handling:** While `SUBSTR` is used in BigQuery, explicit length constraints (e.g., `VARCHAR2(100)`) from Oracle need to be carefully mapped to BigQuery's flexible `STRING` type. Potential data truncation or unexpected behavior due to character sets/encodings should be reviewed.
*   **Error Handling and Logging:** The `EXCEPTION WHEN OTHERS` block in the Oracle script is very generic. The BigQuery solution should incorporate robust error handling and logging mechanisms, potentially using BigQuery audit logs, Cloud Logging, or custom logging tables.
*   **Performance of `STRING_AGG`:** While generally efficient, large numbers of groups or very long aggregated strings could impact performance. Careful testing with representative data volumes is necessary.
*   **Ordering of Concatenated Strings:** The `ORDER BY access_point_name` within `STRING_AGG` ensures deterministic output, mimicking the implicit order a cursor might process. This explicit ordering is crucial for data consistency if the original logic relied on a specific sort order for concatenation.
*   **Historical Data:** If the `sof$ta_apn_vertrag` table is historized or used for auditing, the simple `TRUNCATE TABLE` and `INSERT` approach needs to be reviewed against historization requirements in BigQuery.

## 8. Build Plan
1.  **BigQuery Data Model Creation:**
    *   Create `your_project.your_dataset.SOFTA_BPR_APN` table.
    *   Create `your_project.your_dataset.SOFTA_APN_VERTRAG` table.
    *   *Language:* BigQuery DDL
2.  **Initial Data Migration:**
    *   Migrate historical and current data from the Oracle `sof$ta_bpr_apn` table to `your_project.your_dataset.SOFTA_BPR_APN`.
    *   If `sof$ta_apn_vertrag` contains historical data that needs to be preserved, migrate it to a BigQuery equivalent.
    *   *Language:* Google Cloud Data Transfer Service, BigQuery Data Load jobs, or custom Python scripts.
3.  **BigQuery Transformation Script Development:**
    *   Develop the BigQuery SQL script (`d_ausd_bp_ta_apn_vertrag_bq.sql`) for aggregation and insertion as outlined in Section 5.
    *   Include `TRUNCATE TABLE` as the initial step in the script or manage it via job configuration.
    *   *Language:* BigQuery SQL
4.  **Utility Functionality Replacement:**
    *   Review `isbert_schema.DWPA_UTIL_SKRIPT` for any functionality beyond truncation. If critical, re-implement in BigQuery SQL or Python.
    *   *Language:* BigQuery SQL / Python
5.  **Orchestration and Scheduling:**
    *   Integrate the `d_ausd_bp_ta_apn_vertrag_bq.sql` script into a BigQuery orchestration framework (e.g., Cloud Composer/Apache Airflow, native BigQuery scheduled queries).
    *   Define the job schedule and dependencies.
    *   *Language:* Airflow DAG (Python) or BigQuery Scheduled Queries (SQL).
6.  **Testing and Validation:**
    *   Perform unit, integration, and user acceptance testing with comparable data volumes to ensure data integrity and performance.
    *   *Language:* Testing Frameworks, SQL for data validation.