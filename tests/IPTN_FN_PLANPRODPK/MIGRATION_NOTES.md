# Migration Notes: IPTN_FN_PLANPRODPK

## 1. Summary
The legacy Oracle PL/SQL anonymous block (`d_iptn_l_fn_vs_planprod_pk.sql`) for job **IPTN_FN_PLANPRODPK** has been migrated to Google Cloud BigQuery using Dataform (SQLX). 

The primary purpose of this job is to perform a delta data load mapping "Perlenprodukte" (Pearl Products) to plan products from an incoming CSV file (represented as external tables) into the target data warehouse table `DWH$TA_L_FN_VS_PLANPROD_PK`. The migration translates procedural, row-by-row cursor loops into highly optimized, set-based BigQuery DML operations.

---

## 2. Generated Artifacts
The migration process generated the following target artifact:

*   **`jp_11/d_iptn_l_fn_vs_planprod_pk.sqlx`**
    *   **Role**: Dataform operations file containing the refactored BigQuery SQL scripting block. It orchestrates the delta load using transactional blocks (`BEGIN TRANSACTION` / `COMMIT TRANSACTION`), executing set-based `MERGE` and `UPDATE` statements to achieve equivalent logic without performance-intensive row-by-row loops.

---

## 3. Key Design Decisions

### Set-Based Refactoring (Performance & Cost Optimization)
*   **The Challenge**: The source Oracle script used two explicit cursors (`cur1` and `cur2`) to loop through records row-by-row, calling database package procedures (`dwh$bs_l_fn_vs_planprod_pk.db_insert` and `db_delete`) for each record.
*   **The Solution**: Row-by-row processing is highly inefficient and expensive in BigQuery. 
    *   `cur1` (Upsert logic) was refactored into a single set-based **`MERGE`** statement.
    *   `cur2` (Deactivation/Soft-delete logic) was refactored into a single set-based **`UPDATE`** statement.

### ANSI Join Compliance
*   **The Challenge**: The source script used implicit Cartesian joins (`FROM <DATEN_EXTTAB> a, <ER_EXTTAB> b`).
*   **The Solution**: Converted to explicit **`CROSS JOIN`** syntax to ensure type safety, readability, and strict ANSI compliance.

### Set Operators
*   **The Challenge**: The source script used the Oracle `MINUS` operator to identify records missing from the new delivery.
*   **The Solution**: Replaced with BigQuery's native **`EXCEPT DISTINCT`** operator, which has identical semantics.

### Transaction and Exception Management
*   **The Challenge**: The source script relied on Oracle PL/SQL transaction control (`COMMIT`/`ROLLBACK`) and custom exception blocks (`WHEN OTHERS`).
*   **The Solution**: Implemented BigQuery Scripting's native **`BEGIN TRANSACTION`**, **`COMMIT TRANSACTION`**, and **`ROLLBACK TRANSACTION`** inside a `BEGIN...EXCEPTION WHEN ERROR THEN...END` block. This guarantees atomic execution (all-or-nothing).

### Logging System Alignment
*   **The Challenge**: The legacy script called `dwpa_meldung.fehler` to log execution errors.
*   **The Solution**: Replaced with a standardized `INSERT` statement into a centralized logging table (`dw_logs.dwpa_meldung_errors`), capturing the error message (`@@error.message`) and the failing statement (`@@error.statement_text`).

---

## 4. Manual Steps Before Go-Live

### 1. Schema & Dataset Creation
Ensure that the target datasets and tables exist in the target BigQuery environment:
*   Target Table: `DWH$TA_L_FN_VS_PLANPROD_PK`
*   Dimension View: `DWH$VI_L_FN_VS_PLANPROD_PK`
*   Logging Table: `dw_logs.dwpa_meldung_errors` (Create if it does not exist using the schema below):
    ```sql
    CREATE TABLE IF NOT EXISTS dw_logs.dwpa_meldung_errors (
      severity STRING,
      entry_nr INT64,
      error_nr INT64,
      error_msg STRING,
      statement STRING,
      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    ```

### 2. External Table Configuration
The script references two external tables representing the incoming CSV files. These must be configured in BigQuery pointing to their respective Cloud Storage (GCS) URIs:
*   `${ref("DATEN_EXTTAB")}`: Staging table containing the CSV payload data.
*   `${ref("ER_EXTTAB")}`: Staging table containing execution run metadata (specifically the `stichtag` date).

### 3. IAM & Permissions
Ensure the service account executing the Dataform pipeline has the following permissions:
*   `BigQuery Data Editor` on the target datasets.
*   `BigQuery Admin` or `BigQuery Connection User` (if using external connections to GCS).
*   `Storage Object Viewer` on the GCS buckets hosting the CSV source files.

### 4. Scheduling & Parameterization
*   The script expects a parameterized job execution identifier `@p_eintragsnr` (corresponding to the legacy `<JOBNR>`).
*   Configure this parameter in your orchestration tool (e.g., Cloud Composer / Apache Airflow) to pass the unique run ID dynamically during execution.

---

## 5. Known Gaps & Unresolved References

### 1. Business Logic of Legacy Packages (Advisory)
The source code for the legacy Oracle package `dwh$bs_l_fn_vs_planprod_pk` was not provided. The migration assumes standard Slowly Changing Dimension (SCD) Type-1/Type-2 behavior based on the comments:
*   `db_insert` maps to an upsert matching on `td_perlenprodukt_id` and `referenz_jahr`.
*   `db_delete` maps to an update setting `gueltig_bis` to the execution day's `stichtag`.

*If these procedures implement additional custom business rules (e.g., surrogate key generation, archiving, or validation), they must be manually added to the `MERGE` and `UPDATE` statements.*

### 2. Cardinality of `<ER_EXTTAB>`
The script uses a `CROSS JOIN` with `${ref("ER_EXTTAB")}`. This design assumes that the execution metadata table **always contains exactly one row**. If this table contains multiple rows, the cross join will cause data multiplication. 

---

## 6. Validation
To validate the migrated script, execute the following test cases in a lower environment (UAT/QA):

### Run the Tests
1.  **Dry Run**: Execute the Dataform action in dry-run mode to verify syntax and compilation.
2.  **Initial Load Test**:
    *   Populate `DATEN_EXTTAB` with sample records.
    *   Populate `ER_EXTTAB` with a single row containing a `stichtag` (e.g., `2023-10-01`).
    *   Execute the Dataform action.
3.  **Delta Load Test (Upsert & Deactivation)**:
    *   Modify an existing record's `td_plan_produkt_id` in `DATEN_EXTTAB`.
    *   Remove one record from `DATEN_EXTTAB` (to test deactivation).
    *   Add a new record to `DATEN_EXTTAB`.
    *   Update `ER_EXTTAB` with a new `stichtag` (e.g., `2023-10-02`).
    *   Execute the Dataform action.

### What "Passing" Means
*   **Upsert Success**: Modified records are updated, and new records are inserted into `DWH$TA_L_FN_VS_PLANPROD_PK`.
*   **Deactivation Success**: The record removed from the source file now has its `gueltig_bis` column set to `2023-10-02` in `DWH$TA_L_FN_VS_PLANPROD_PK`.
*   **No Duplication**: The total row count matches expectations; no Cartesian product duplication occurred.
*   **Error Handling**: Intentionally introduce a database error (e.g., type mismatch). Verify that the transaction rolls back completely and a row is written to `dw_logs.dwpa_meldung_errors`.

---

## 7. Rollback Procedure
Because this script performs in-place updates and soft-deletes, rolling back a failed or corrupted run requires restoring the target table to its state prior to execution.

### Option A: BigQuery Time Travel (Recommended)
If the corruption is detected within the BigQuery time travel window (default 7 days), restore the table using the following SQL:

```sql
-- 1. Drop the corrupted table
DROP TABLE `your_project.your_dataset.DWH$TA_L_FN_VS_PLANPROD_PK`;

-- 2. Restore from a specific timestamp before the job execution
CREATE TABLE `your_project.your_dataset.DWH$TA_L_FN_VS_PLANPROD_PK`
AS SELECT *
FROM `your_project.your_dataset.DWH$TA_L_FN_VS_PLANPROD_PK`
FOR SYSTEM_TIME AS OF TIMESTAMP('2023-10-02 05:00:00 UTC');
```

### Option B: Snapshot Restore
If a table snapshot was taken prior to the execution of the job, restore the table from the snapshot:

```sql
-- 1. Drop the corrupted table
DROP TABLE `your_project.your_dataset.DWH$TA_L_FN_VS_PLANPROD_PK`;

-- 2. Restore from snapshot
CREATE TABLE `your_project.your_dataset.DWH$TA_L_FN_VS_PLANPROD_PK`
CLONE `your_project.your_dataset.DWH$TA_L_FN_VS_PLANPROD_PK_PRE_JOB_SNAPSHOT`;
```