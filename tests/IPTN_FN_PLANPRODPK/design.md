=== FILE: local/data/source/jp_11/d_iptn_l_fn_vs_planprod_pk.sql ===
------------------------------------------------------------------------------------------------------
-- Import mapping data for Perlenprodukte to plan products from ECG-I CSV file to table DWH$TA_L_FN_VS_PLANPROD_PK.
-- Erstellt : Sven Strehlau
-- Datum : 30.03.2021
-- Language : PL/SQL
-- Job: IPTN_FN_PLANPRODPK
-- Version : 1.0
-- History :	30.03.2021 - 1.0 Sven Strehlau - Initial version
--				initiative: BDI-2722 TDG Salesreporting - Plandaten
--				epic: BDI-10502 TDG Salesreporting Integration Plandaten FN für Plan/Ist-Vergleich
--				increment: 2021 PI5
------------------------------------------------------------------------------------------------------




START $DW_DIR_ROOT/allgemein/is/util/sql/d_alis_init.sql

SET TIMING ON;
SET ECHO OFF;

DECLARE
   l_table VARCHAR2(30) := 'DWH$TA_L_FN_VS_PLANPROD_PK';
   v_anzahl_ds NUMBER := 0;
   EintragsNr NUMBER := <JOBNR>;
   l_ret_value NUMBER;

-----------------------------------------------------------------
-- Cursor for INSERT new rows or update existing rows          --
-----------------------------------------------------------------

CURSOR cur1 IS
WITH
    ext_tab AS
    (
      SELECT
           a.*,
           b.stichtag er_stichtag
      FROM <DATEN_EXTTAB> a, <ER_EXTTAB> b
    )
   SELECT
		td_perlenprodukt_id,
		td_plan_produkt_id,
		referenz_jahr,
		er_stichtag gueltig_von
   FROM ext_tab;

-----------------------------------------------------------------
-- Cursor for ending validity of existing rows                 --
-----------------------------------------------------------------
CURSOR cur2 IS
   SELECT
		a.td_perlenprodukt_id,
		a.referenz_jahr,
        b.stichtag
   FROM DWH$VI_L_FN_VS_PLANPROD_PK a,
        <ER_EXTTAB> b
MINUS
   SELECT
		a.td_perlenprodukt_id,
		a.referenz_jahr,
        b.stichtag
   FROM <DATEN_EXTTAB> a,
        <ER_EXTTAB> b
		;

BEGIN
   --------------------------------------------------------------
   -- The db_insert performs an insert or update               --
   -- If one key from the new delivery is existing in DB table --
   -- then UPDATE else INSERT the row in DB table.             --
   --------------------------------------------------------------
   v_anzahl_ds := 0;
  
   FOR c1 IN cur1
   LOOP
      l_ret_value := dwh$bs_l_fn_vs_planprod_pk.db_insert
      (
		i_td_perlenprodukt_id => c1.td_perlenprodukt_id
		, i_td_plan_produkt_id => c1.td_plan_produkt_id
		, i_referenz_jahr => c1.referenz_jahr
		, i_gueltig_von => c1.gueltig_von
      );
  
     IF l_ret_value = 0 THEN 
        v_anzahl_ds := v_anzahl_ds + 1; 
     END IF; 
  
   END LOOP; 
  
   dbms_output.put_line(TO_CHAR(v_anzahl_ds) || ' rows successfully inserted in the table ' || l_table || ' (db_insert).');
  
   COMMIT;

   ------------------------------------------------------------------
   -- The db_delete performs an update on the gueltig_bis column   --
   -- so that keys with a row in DB table but no row with that key --
   -- in the new delivery were no longer valid.                    --
   ------------------------------------------------------------------
	 v_anzahl_ds := 0;

   FOR c2 IN cur2
   LOOP
   
	  l_ret_value := dwh$bs_l_fn_vs_planprod_pk.db_delete
      (
		  i_td_perlenprodukt_id => c2.td_perlenprodukt_id
		, i_referenz_jahr => c2.referenz_jahr
        , i_gueltig_bis => c2.stichtag
      );

      IF l_ret_value = 0 THEN 
        v_anzahl_ds := v_anzahl_ds + 1; 
      END IF; 

   END LOOP;
 
   dbms_output.put_line(TO_CHAR(v_anzahl_ds) || ' rows successfully deleted in the table ' || l_table || ' (db_delete).');

   COMMIT;

EXCEPTION
WHEN OTHERS THEN
    -- unbekannte bzw. nicht erwartete Exceptions koennen auch
    -- behandelt werden. Die Fehlernummer ist immer die gleiche, nur
    -- der Zusatzfehlertext kann vorher ermittelt werden.

  ROLLBACK;

  DECLARE
    ErrText VARCHAR2(512);
    ErrC NUMBER;
    FehlerNr NUMBER := dwpa_globals.k_alis_err_unknown;
  BEGIN
    ErrText := SQLERRM;
    ErrC := SQLCODE;
    dwpa_meldung.fehler ('F', EintragsNr, FehlerNr, ErrText, TO_CHAR(ErrC));
    RAISE_APPLICATION_ERROR(FEHLERNR, ERRTEXT);
  END;
END;
/


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Oracle SQL Object Type:
    - PL/SQL Anonymous Block (Multi-statement data integration script)

1.2 Business Logic & Purpose:
    - This script automates a delta data load mapping Perlenprodukte (Pearl Products) to plan products from an incoming CSV format (represented as external tables) into a data warehouse target table (`DWH$TA_L_FN_VS_PLANPROD_PK`).
    - **Step 1 (Upsert/Load)**: It iterates through `cur1` (all records from the new staging/external delivery tables `<DATEN_EXTTAB>` cross-joined with execution run metadata from `<ER_EXTTAB>`). It passes each record to the business procedure `dwh$bs_l_fn_vs_planprod_pk.db_insert` to handle insert/update logic.
    - **Step 2 (Deactivation/End-dating)**: It identifies existing records in the target dimension view (`DWH$VI_L_FN_VS_PLANPROD_PK`) that are missing in the current load (`<DATEN_EXTTAB>`). For these expired keys, it runs `dwh$bs_l_fn_vs_planprod_pk.db_delete` to soft-delete or set an expiry date (`gueltig_bis`) using the run's key date (`stichtag`).
    - **Step 3 (Error Handling)**: Implements custom PL/SQL exception blocks that log run errors to logging objects (`dwpa_meldung.fehler`) and raise a fatal run error.

1.3 Entities Referenced:
    - `DWH$TA_L_FN_VS_PLANPROD_PK` (Target Table)
    - `DWH$VI_L_FN_VS_PLANPROD_PK` (Dimension View tracking current active relationships)
    - `<DATEN_EXTTAB>` (Staging/External Data Input Table - parameterized)
    - `<ER_EXTTAB>` (Staging/External Execution Metadata Table - parameterized)
    - `dwh$bs_l_fn_vs_planprod_pk` (Business Logic DB Package)
    - `dwpa_globals`, `dwpa_meldung` (Infrastructure logging/global utilities)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - `VARCHAR2(30)` / `VARCHAR2(512)` → `STRING`
    - `NUMBER` (for counts / return values) → `INT64`
    - `DATE` (specifically `stichtag` / `gueltig_von`) → `DATE` (treated as calendar dates for dimension validity)

2.2 Implicit and Explicit Type Casting:
    - `TO_CHAR(v_anzahl_ds)` → `CAST(v_anzahl_ds AS STRING)` or formatting functions.

2.3 NULL Handling and Conditional Functions:
    - None used directly in cursors; business package handles values.

2.4 String Functions:
    - `TO_CHAR(v_anzahl_ds)` → `CAST(v_anzahl_ds AS STRING)`.

2.5 Date and Timestamp Functions:
    - No direct date truncations or arithmetic are used. The fields `gueltig_von`, `gueltig_bis`, and `stichtag` represent system processing and validity dates.

2.6 Numeric and Aggregate Functions:
    - None used.

2.7 Analytical and Window Functions:
    - None used.

2.8 Set and Join Operations:
    - `MINUS` inside `cur2` → Convert to `EXCEPT DISTINCT` in BigQuery.
    - Implicit Cartesian cross join: `FROM <DATEN_EXTTAB> a, <ER_EXTTAB> b` → Convert to explicit `CROSS JOIN` to avoid legacy SQL syntax and ensure type safety.

2.9 Row Limiting and Sampling:
    - None used.

2.10 Sequences:
    - None used.

2.11 MERGE Statements:
    - The PL/SQL procedural insert/update loop (`cur1`) behaves like an upsert operations pattern. To optimize for BigQuery's set-based nature, this row-by-row cursor execution should be converted into a single optimized `MERGE` statement.

2.12 INSERT / UPDATE / DELETE:
    - The PL/SQL procedural deactivation loop (`cur2`) is converted to an optimized set-based `UPDATE` statement in BigQuery.

2.13 DDL Constructs:
    - None present.

2.14 PL/SQL Scripting Constructs:
    - Local variable declarations, exception block, and transaction tracking map directly to BigQuery Scripting (`DECLARE`, `BEGIN...EXCEPTION...END`).
    - Cursors are refactored to set-based DML operations to remove low-performance loops.
    - Transaction control (`COMMIT`/`ROLLBACK`) is handled implicitly by BigQuery's transaction blocks (`BEGIN TRANSACTION` ... `COMMIT TRANSACTION`).

2.15 Unresolvable or Advisory Items:
    - `dwh$bs_l_fn_vs_planprod_pk` package logic is missing from this file scope. Its core logic is modeled based on documented comments (UPSERT/Merge logic for insert, and soft-delete/date validation update for delete).
    - Custom logger `dwpa_meldung.fehler` cannot execute directly inside BigQuery. It must be logged to a BigQuery audit/log table or handled via a procedural tracking block.

2.16 MIGRATION DECISION MATRIX

| Statement / Oracle Construct | Selected Target | Rejected Alternatives | Evidence / Reason |
| :--- | :--- | :--- | :--- |
| **Cursor Loops (`cur1` & `cur2`)** | Set-based BigQuery DML (`MERGE` & `UPDATE`) | BQ Scripting `FOR` loops | Executing row-by-row updates via loops in BigQuery is highly inefficient, expensive, and scales poorly. Set-based logic is the industry standard for analytical warehouses. |
| **`MINUS` Set Operator** | `EXCEPT DISTINCT` | Subqueries with `NOT EXISTS` | `EXCEPT DISTINCT` is the direct ANSI/BigQuery equivalent to Oracle's `MINUS` with identical semantics. |
| **Implicit Joins (`FROM A, B`)** | Explicit `CROSS JOIN` | Comma-separated joins | ANSI-compliance, safety, and performance optimization in BigQuery. |
| **Exception Handling (`WHEN OTHERS`)** | `BEGIN...EXCEPTION` Block | Application-level checks | BigQuery Scripting supports robust `EXCEPTION` handling natively to capture errors and roll back active transactions. |

2.17 REQUIRED ARTIFACTS
- **BigQuery SQL Script**: A unified standard BigQuery scripting block executing transactional, set-based updates with exception management. No external UDFs or Python wrappers are required.

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Type | BigQuery Type | Conversion Rule | Warnings / Implications |
| :--- | :--- | :--- | :--- |
| `VARCHAR2(30)` | `STRING` | Direct Mapping | None. |
| `VARCHAR2(512)` | `STRING` | Direct Mapping | None. |
| `NUMBER` | `INT64` | Cast to integer for identifiers, counters, and job numbers. | None. |
| `DATE` | `DATE` | Maps to `DATE` if time components are omitted; otherwise `DATETIME`. | Target values `stichtag`, `gueltig_von`, and `gueltig_bis` represent day-level validity boundaries. |

2.19 DESIGN REVIEW SUMMARY
- **Patterns/Objects Found**: Row-level cursor logic wrappers executing DML via external packages, set difference operations, transaction rollbacks.
- **Unsupported Functions**: Oracle PL/SQL package business logic (`dwh$bs_l_fn_vs_planprod_pk`).
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: `<DATEN_EXTTAB>`, `<ER_EXTTAB>`, `DWH$VI_L_FN_VS_PLANPROD_PK`, `DWH$TA_L_FN_VS_PLANPROD_PK`.
- **Assumptions**: 
  - `dwh$bs_l_fn_vs_planprod_pk.db_insert` performs an upsert on `DWH$TA_L_FN_VS_PLANPROD_PK` matching on `td_perlenprodukt_id` and `referenz_jahr`.
  - `dwh$bs_l_fn_vs_planprod_pk.db_delete` updates `gueltig_bis` to the execution day's `stichtag` for missing entries.
  - Standard transaction behaviors apply (the operations must commit atomically or all roll back).

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.20 PACKAGE ANALYSIS
*(No package bodies are declared in this source file. The procedure calls are treated as functional dependencies).*

| Package.Member | Classification | Migration Target | Package-level State | Calls to other members |
| :--- | :--- | :--- | :--- | :--- |
| `dwh$bs_l_fn_vs_planprod_pk.db_insert` | `UNKNOWN_NO_SOURCE` | Refactored to set-based `MERGE` | None verified | None verified |
| `dwh$bs_l_fn_vs_planprod_pk.db_delete` | `UNKNOWN_NO_SOURCE` | Refactored to set-based `UPDATE` | None verified | None verified |
| `dwpa_meldung.fehler` | `UNKNOWN_NO_SOURCE` | Scripted logging table insert | None verified | None verified |

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `MINUS` | Direct-with-rewrite | `EXCEPT DISTINCT` |
| `TO_CHAR` | Direct-with-rewrite | `CAST(value AS STRING)` |
| `dbms_output.put_line` | Direct-with-rewrite | BQ script output or logging table insert |
| `RAISE_APPLICATION_ERROR` | Direct-with-rewrite | `ERROR(message)` |
| `SQLERRM` | Direct-with-rewrite | `@@error.message` |
| `SQLCODE` | Direct-with-rewrite | `@@error.statement_text` or customized tracking |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- Dynamic parameters or scripting variables to replace environment placeholders
DECLARE var_table STRING DEFAULT 'DWH$TA_L_FN_VS_PLANPROD_PK';
DECLARE v_anzahl_ds INT64 DEFAULT 0;
DECLARE EintragsNr INT64 DEFAULT <JOBNR>; -- placeholder parameter
DECLARE v_inserted_rows INT64 DEFAULT 0;
DECLARE v_updated_rows INT64 DEFAULT 0;
DECLARE v_deleted_rows INT64 DEFAULT 0;

BEGIN
  -- Start Transactional block
  BEGIN TRANSACTION;

  --------------------------------------------------------------
  -- REFACED LOOP 1: Set-based MERGE operation representing     --
  -- dwh$bs_l_fn_vs_planprod_pk.db_insert processing          --
  --------------------------------------------------------------
  
  -- Compute Source data equivalent to Cursor 1
  -- Implicit cartesian join converted to explicit CROSS JOIN
  MERGE DWH$TA_L_FN_VS_PLANPROD_PK AS target
  USING (
    SELECT
      a.td_perlenprodukt_id,
      a.td_plan_produkt_id,
      a.referenz_jahr,
      b.stichtag AS gueltig_von
    FROM `<DATEN_EXTTAB>` AS a
    CROSS JOIN `<ER_EXTTAB>` AS b
  ) AS source
  ON target.td_perlenprodukt_id = source.td_perlenprodukt_id
    AND target.referenz_jahr = source.referenz_jahr
  WHEN MATCHED THEN
    UPDATE SET 
      target.td_plan_produkt_id = source.td_plan_produkt_id,
      target.gueltig_von = source.gueltig_von
  WHEN NOT MATCHED THEN
    INSERT (td_perlenprodukt_id, td_plan_produkt_id, referenz_jahr, gueltig_von)
    VALUES (source.td_perlenprodukt_id, source.td_plan_produkt_id, source.referenz_jahr, source.gueltig_von);

  -- Track affected row counts
  SET v_inserted_rows = @@row_count;

  -- Log action output
  SELECT FORMAT('%d rows successfully merged into table %s.', v_inserted_rows, var_table) AS execution_log;


  --------------------------------------------------------------
  -- REFACED LOOP 2: Set-based UPDATE operation representing     --
  -- dwh$bs_l_fn_vs_planprod_pk.db_delete processing          --
  --------------------------------------------------------------

  -- Identifies active keys no longer present in the input file 
  -- Updates target table's gueltig_bis column to signify expiration
  UPDATE DWH$TA_L_FN_VS_PLANPROD_PK AS target
  SET target.gueltig_bis = expiry_source.stichtag
  FROM (
    SELECT
      a.td_perlenprodukt_id,
      a.referenz_jahr,
      b.stichtag
    FROM DWH$VI_L_FN_VS_PLANPROD_PK AS a
    CROSS JOIN `<ER_EXTTAB>` AS b
    EXCEPT DISTINCT  -- converted from MINUS
    SELECT
      a.td_perlenprodukt_id,
      a.referenz_jahr,
      b.stichtag
    FROM `<DATEN_EXTTAB>` AS a
    CROSS JOIN `<ER_EXTTAB>` AS b
  ) AS expiry_source
  WHERE target.td_perlenprodukt_id = expiry_source.td_perlenprodukt_id
    AND target.referenz_jahr = expiry_source.referenz_jahr;

  SET v_deleted_rows = @@row_count;

  -- Log delete output
  SELECT FORMAT('%d rows successfully deactivated (deleted) in table %s.', v_deleted_rows, var_table) AS execution_log;

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;

  -- Nested block to log error details to logging catalog
  BEGIN
    DECLARE ErrText STRING;
    DECLARE ErrC STRING;
    DECLARE FehlerNr INT64 DEFAULT -1; -- mapping from dwpa_globals.k_alis_err_unknown;

    SET ErrText = @@error.message;
    SET ErrC = @@error.statement_text;

    -- Standardized target logging write replacing dwpa_meldung.fehler call
    INSERT INTO `dw_logs.dwpa_meldung_errors` (severity, entry_nr, error_nr, error_msg, statement)
    VALUES ('F', EintragsNr, FehlerNr, ErrText, ErrC);

    -- Bubble up actual execution failure details
    ERROR(FORMAT('Execution Failed. Code: %s, Message: %s', ErrC, ErrText));
  END;
END;
```

### FLAGGED ITEMS FOR HUMAN REVIEW
1. **Business Logic of `db_insert`/`db_delete` Procedures**: The code was refactored assuming standard Type-2 Dimension SCD logic (inserting/updating valid states and end-dating missing records based on keys `td_perlenprodukt_id` and `referenz_jahr`). If the underlying packages perform additional structural actions (e.g., surrogate key generation, archiving, or auditing), this logic must be added back to the set-based `MERGE` statement.
2. **Execution Metadata Source `<ER_EXTTAB>`**: Verify if this external metadata table always returns a single row. The `CROSS JOIN` is highly performance-sensitive depending on the cardinality of `<ER_EXTTAB>`.
3. **Target Logging Catalog**: The PL/SQL routine `dwpa_meldung.fehler` has been replaced with a descriptive insert into a mock logging table (`dw_logs.dwpa_meldung_errors`). Adjust target dataset/table namespace based on organizational logging design.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/data/source/jp_11/d_iptn_l_fn_vs_planprod_pk.sql` | `jp_11/d_iptn_l_fn_vs_planprod_pk.sqlx` | Migrate the PL/SQL anonymous block to a Dataform SQLX scripting block, refactoring procedural row-by-row cursors into optimized set-based BigQuery `MERGE` and `UPDATE` operations. |

---

### Lineage

Based on the lineage edges in the source context:
* **Upstream Data Sources**: 
  * `<DATEN_EXTTAB>` (External table containing the imported ECG-I CSV mapping data)
  * `<ER_EXTTAB>` (External table containing stichtag execution run metadata)
  * `DWH$VI_L_FN_VS_PLANPROD_PK` (Database view tracking active valid relationships)
* **Downstream Target Table**:
  * `DWH$TA_L_FN_VS_PLANPROD_PK` (Target table receiving upserted or end-dated product-mapping records)
* **Underlying Logic Dependencies**:
  * `PACKAGE:BS_L_FN_VS_PLANPROD_PK` (Legacy Oracle package `dwh$bs_l_fn_vs_planprod_pk` containing `db_insert` and `db_delete` routines, now refactored to set-based logic)
  * `PACKAGE:DWPA_MELDUNG` (Oracle logging package containing error logging functions, mapped to a standardized BigQuery logging catalog)

*Note on Parser Artifacts*: Lineage references to `TABLE:ECG`, `TABLE:EXT_TAB`, `TABLE:THE`, `TABLE:EXISTING`, `TABLE:ELSE`, and `TABLE:ON` are technical artifacts generated by the parser interpreting SQL comments and variable names. They do not represent real database entities.

---

### External System Replacements

* **Oracle External Tables**: The source script reads from external files via `<DATEN_EXTTAB>` and `<ER_EXTTAB>`. In the BigQuery architecture, these files must be hosted on Cloud Storage (GCS) and exposed as BigQuery External Tables or loaded into staging tables using Cloud Composer or Dataform integrations prior to script execution.
* **Oracle Package Calls**: 
  * `dwh$bs_l_fn_vs_planprod_pk.db_insert` is replaced by a BigQuery `MERGE` statement.
  * `dwh$bs_l_fn_vs_planprod_pk.db_delete` is replaced by a BigQuery `UPDATE` statement.
  * `dwpa_meldung.fehler` is replaced by inserting error details into a centralized logging table in the `dw_logs` dataset.

---

### Cross-File Dependencies

* **Shared Tables and Views**:
  * `DWH$TA_L_FN_VS_PLANPROD_PK` is the primary target table shared with downstream reporting processes.
  * `DWH$VI_L_FN_VS_PLANPROD_PK` is a shared view queryable inside the same dataset to track current mapping relationships.
* **Initialization Script**: The legacy initialization script (`d_alis_init.sql`) must be replaced by standard environment configuration settings in the Cloud Composer/Dataform workflow.

---

### Target File Plan

* **Target File**: `jp_11/d_iptn_l_fn_vs_planprod_pk.sqlx`
  * **Language**: Dataform SQLX
  * **Source File**: `local/data/source/jp_11/d_iptn_l_fn_vs_planprod_pk.sql`
  * **Purpose**: Orchestrates the delta load using transactional scripting blocks (`BEGIN TRANSACTION` / `COMMIT TRANSACTION`), performing set-based `MERGE` and `UPDATE` statements to achieve equivalent logic without performance-intensive row-by-row loops.

---

### Environment-Specific Values

The environment values and variables within this script must be resolved via standard Airflow/Composer variables and Dataform configurations rather than embedded literal placeholders.

#### 1. GLOBAL (Environment-Wide Variables)
* **`GCP_PROJECT`**: The target Google Cloud Project ID.
* **`GCS_BUCKET`**: The target Cloud Storage Bucket path replacing the legacy `$DW_DIR_ROOT` pathing.
* **`BQ_DATASET`**: The target dataset name (where `DWH$TA_L_FN_VS_PLANPROD_PK` and `DWH$VI_L_FN_VS_PLANPROD_PK` reside).

*Resolution Mechanism*: Integrated via Dataform's `projectConfig` or resolved at runtime using Airflow standard variables: `Variable.get("GCP_PROJECT")` and `Variable.get("GCS_BUCKET")`.

#### 2. JOB-SPECIFIC (Job-Specific Variables)
* **`EintragsNr` (corresponds to `<JOBNR>`)**: The unique execution run identifier.
* **`DATEN_EXTTAB`**: The actual name of the external staging table containing CSV payload data.
* **`ER_EXTTAB`**: The actual name of the external table containing run execution metadata.

*Resolution Mechanism*: Configured inside the Airflow task execution context or passed into the Dataform execution via dynamic run parameters.

---

### Risks and Manual Steps

1. **Missing Business Logic from Packages**: The legacy PL/SQL package `dwh$bs_l_fn_vs_planprod_pk` was not supplied. We assume standard Type-2 / Slowly Changing Dimension (SCD) behaviour. If these procedures implement custom business actions (e.g., surrogate key generation, archiving, or auditing), the logic must be incorporated manually into the target SQLX script.
2. **Cardinality of `<ER_EXTTAB>`**: The script converts implicit joins using `<ER_EXTTAB>` into explicit `CROSS JOIN` statements. This assumes `<ER_EXTTAB>` always returns exactly one row containing execution metadata. If this table can contain multiple rows, the cross join will cause data multiplication and must be rewritten with an explicit window partition or filtering condition.
3. **Logging System Alignment**: The legacy logging utility `dwpa_meldung` has been mapped to a descriptive `INSERT` into a mock logging table (`dw_logs.dwpa_meldung_errors`). The exact target logging dataset and table schema must be verified and adjusted against the target enterprise logging design.