# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

## 1. Purpose & Scope
This document designs the migration of the legacy KornShell (KSH) wrapper script `k_ausd_bp_ta_bpr_apn.ksh` to Google Cloud BigQuery. 

In the legacy system, this shell script serves as a control and orchestration wrapper. It validates input parameters, confirms date formatting, derives dynamic parameters (yesterday and today's dates), and launches an Oracle SQL\*Plus script (`d_ausd_bp_ta_bpr_apn.sql`) to process data for the `PoolBasisprodukt` table. It also handles basic job control and logs execution metrics.

The goal of this migration is to retire the KSH file-based execution and replace it with a native, robust, and serverless **BigQuery Stored Procedure** (`sp_k_ausd_bp_ta_bpr_apn`) that preserves the validation, date-derivation, and execution sequence.

---

## 2. Source Inventory
| Source File | Tech / Language | Complexity Tier | Automation Bucket | Est. Effort | Role / Purpose |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `k_ausd_bp_ta_bpr_apn.ksh` | KornShell (KSH) | Medium | Semi_auto (0.65 rate) | Medium | Command-line parameter parser, date validator, dynamic date calculator, and Oracle SQL\*Plus wrapper. |
| `d_ausd_bp_ta_bpr_apn.sql` *(referenced)* | Oracle SQL / PL-SQL | Medium | Semi_auto | Medium | Executes core transformation/extraction logic for `PoolBasisprodukt`. |

---

## 3. Target Architecture
To align with Google Cloud best practices and eliminate the need for virtual machine execution (Compute Engine/GKE) for lightweight shell wrappers, the orchestration logic is migrated directly into BigQuery procedural SQL.

```
       [ Cloud Composer / Airflow ] (Triggers Stored Procedure)
                     │
                     ▼
  ┌─────────────────────────────────────────────────────────────┐
  │         BigQuery Stored Procedure:                          │
  │         sp_k_ausd_bp_ta_bpr_apn                             │
  │  ┌───────────────────────────────────────────────────────┐  │
  │  │ 1. Parameter Validation                               │  │
  │  │ 2. Date Check (DDMMYYYY)                              │  │
  │  │ 3. Current & Yesterday Date Derivation                │  │
  │  └──────────────────────────┬────────────────────────────┘  │
  │                             │                               │
  │                             ▼                               │
  │  ┌───────────────────────────────────────────────────────┐  │
  │  │ Call Transformation Procedure:                          │  │
  │  │ sp_d_ausd_bp_ta_bpr_apn                               │  │
  │  │ (Migrated Oracle ETL Script logic)                    │  │
  │  └──────────────────────────┬────────────────────────────┘  │
  │                             │                               │
  │                             ▼                               │
  │  ┌───────────────────────────────────────────────────────┐  │
  │  │ 4. Audit Log & Record Count Tracking                  │  │
  │  └───────────────────────────────────────────────────────┘  │
  └─────────────────────────────┬───────────────────────────────┘
                                │
         ┌──────────────────────┴──────────────────────┐
         ▼                                             ▼
┌─────────────────────────────────┐         ┌─────────────────────────────────┐
│     Table: job_error_log        │         │     Table: job_audit_log        │
│  (Tracks execution failures)    │         │ (Tracks processed record counts)│
└─────────────────────────────────┘         └─────────────────────────────────┘
```

### Component Mapping:
1. **Orchestration**: Replaced by **Cloud Composer (Airflow)** or **BigQuery Stored Procedures**. An Airflow DAG can invoke the main wrapper stored procedure daily.
2. **Stored Procedures**:
   - `project.dataset.sp_k_ausd_bp_ta_bpr_apn`: Wraps execution, validation, and metadata tracking (replacing the `.ksh` script).
   - `project.dataset.sp_d_ausd_bp_ta_bpr_apn`: Wraps the core SQL transformations (replacing the `.sql` script).
3. **Audit & Log Tables**:
   - `project.dataset.job_error_log`: Replaces the legacy `f_alis_msgerr.ksh` / `DWMSG_MeldeFehler` error logging mechanism.
   - `project.dataset.job_audit_log`: Replaces the legacy `FOSJobErzeugeEintrag` / temporary file record count metrics logging.

---

## 4. Data Flow & Lineage
### Legacy Execution Flow:
1. **Invocation**: The scheduler triggers `k_ausd_bp_ta_bpr_apn.ksh` with parameters `-j <JobKennung>`, `-f <EintragsNr>`, `-s <Stichtag>`, and `-l <wiederanlaufWert>`.
2. **Parameters & Dates**: 
   - Parses arguments using shell built-in `getopts`.
   - Validates format of `$p_Stichtag` using helper script `h_alis_date.ksh` (must be `DDMMYYYY`).
   - Runs `gestern.ksh` to retrieve today's and yesterday's dates as strings.
3. **DB execution**: Executes `d_ausd_bp_ta_bpr_apn.sql` via Oracle SQL\*Plus (`starteSQLSkript` helper).
4. **Post-processing** *(Commented Out)*: Historical files (`cibasis_data24.dat`, etc.) were manipulated via Unix utility tools (`sed`, `sort`, `join`) to construct a `.csv`. Since this code is commented out in the source, it is marked as inactive and is omitted from the active pipeline.
5. **Auditing**: Writes final counts to a temp file `$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_apn.tmp`, reads it back to `$v_records`, and records the execution in the FOS job-tracking table.

### Target BigQuery Execution Flow:
1. **Invocation**: Airflow calls `CALL project.dataset.sp_k_ausd_bp_ta_bpr_apn(p_JobKennung, p_EintragsNr, p_Stichtag, p_wiederanlaufWert)`.
2. **Parameter Validation**: The stored procedure natively checks that `p_JobKennung`, `p_EintragsNr`, and `p_Stichtag` are non-null and non-empty.
3. **Date Validation**: Validates date using BigQuery standard function `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)`.
4. **Date Derivation**: Calculates today's and yesterday's values using dynamic standard SQL:
   - Today: `CURRENT_DATE()`
   - Yesterday: `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`
5. **SQL Execution**: Invokes child stored procedure `CALL project.dataset.sp_d_ausd_bp_ta_bpr_apn(...)`.
6. **Auditing**: Calculates count of processed rows, writes results directly to `job_audit_log`, and avoids any file-based storage.

---

## 5. Transformation Logic
Below is the direct translation of the KornShell logic into standard procedural BigQuery SQL.

### Legacy to BigQuery Command Mapping:
- **`getopts` Parameter Parsing** $\rightarrow$ Migrated to Stored Procedure input parameters (`IN`).
- **`DWDate_Datum_Check`** $\rightarrow$ Migrated to `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)`.
- **`gestern.ksh`** $\rightarrow$ Migrated to BigQuery `DATE_SUB` expressions.
- **`DWMSG_MeldeFehler`** $\rightarrow$ Migrated to `INSERT INTO project.dataset.job_error_log` and standard SQL `RAISE USING MESSAGE`.
- **`cat $tmpFile` (Record Count)** $\rightarrow$ Migrated to `SELECT COUNT(*)` or `@@row_count` directly into a BigQuery variable.

### Target Stored Procedure Implementation (Target SQL):
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_bp_ta_bpr_apn`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart_value STRING DEFAULT '0';

  -- Initialize fallback for restart/wiederanlaufWert
  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET v_restart_value = '0';
  ELSE
    SET v_restart_value = p_wiederanlaufWert;
  END IF;

  -- 1. Parameter presence validations
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'Jobkennung is missing';
  END IF;

  IF v_err_nr = 0 AND (p_Stichtag IS NULL OR p_Stichtag = '') THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'Stichtag is missing';
  END IF;

  IF v_err_nr = 0 AND (p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'EintragsNr is missing';
  END IF;

  -- Exit and log if parameters are invalid
  IF v_err_nr != 0 THEN
    INSERT INTO `project.dataset.job_error_log`
    (tab_name, error_code, error_arg, created_at)
    VALUES (v_TabName, v_err_nr, v_err_arg, CURRENT_TIMESTAMP());

    RAISE USING MESSAGE = CONCAT('FEHLER: 0 E ', CAST(v_err_nr AS STRING), ' - ', v_err_arg);
  END IF;

  -- 2. Validate date format (expected DDMMYYYY, e.g., 07052001)
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);

  IF v_stichtag_date IS NULL THEN
    SET v_err_nr = 192;
    SET v_err_arg = CONCAT('Invalid date format for Stichtag: ', p_Stichtag);
    
    INSERT INTO `project.dataset.job_error_log`
    (tab_name, error_code, error_arg, created_at)
    VALUES (v_TabName, v_err_nr, v_err_arg, CURRENT_TIMESTAMP());

    RAISE USING MESSAGE = CONCAT('FEHLER: 0 E ', CAST(v_err_nr AS STRING), ' - ', v_err_arg);
  END IF;

  -- 3. Execute Core Migrated SQL from d_ausd_bp_ta_bpr_apn.sql
  -- This child SP encapsulates the original Oracle transform logic converted to BigQuery SQL
  CALL `project.dataset.sp_d_ausd_bp_ta_bpr_apn`(
    p_JobKennung,
    p_EintragsNr,
    v_stichtag_date,
    v_restart_value,
    v_datum_heute,
    v_datum_gestern
  );

  -- 4. Calculate records generated / processed
  -- Replaces legacy temp file verification and logs to target log table
  SET v_records = (
    SELECT COUNT(*)
    FROM `project.dataset.PoolBasisprodukt`
    -- Filter on the business date corresponding to the execution Stichtag
    WHERE stichtag = v_stichtag_date
  );

  -- 5. Create audit log entry
  INSERT INTO `project.dataset.job_audit_log`
  (
    tab_name,
    job_kennung,
    eintrags_nr,
    stichtag,
    records_loaded,
    status,
    created_at
  )
  VALUES
  (
    v_TabName,
    p_JobKennung,
    p_EintragsNr,
    p_Stichtag,
    v_records,
    'SUCCESS',
    CURRENT_TIMESTAMP()
  );

END;
```

---

## 6. External Dependencies
- **Oracle SQL\*Plus**: This legacy database connection client is retired. All target queries execute in native BigQuery.
- **Unix Shell Environment & Helpers** (`.dw_init`, `h_alis_*.ksh` utilities): Fully retired. BigQuery's built-in stored procedure capabilities handle parameter checking, error generation, and date manipulation.
- **Temp Unix Storage** (`bert_k_ausd_bp_ta_bpr_apn.tmp`): Fully retired. We capture the metrics in-memory via BigQuery SQL variables (`v_records`) and store them directly in the logging database table, eliminating file write/read disk latency.
- **`gestern.ksh` Script**: Replaced completely with standard BigQuery date manipulation functions.

---

## 7. Unresolved / Risks
1. **Orchestrator Argument Resolution**:
   - *Risk*: The Airflow/Composer layer must be configured to pass the correct arguments to `sp_k_ausd_bp_ta_bpr_apn` on execution.
   - *Mitigation*: The Airflow DAG should utilize the `BigQueryInsertJobOperator` to call the procedure and inject execution parameters (such as logical execution date formatted as `DDMMYYYY`) dynamically using Airflow macros (e.g., `{{ ds_format(ds, '%Y-%m-%d', '%d%m%Y') }}`).
2. **Commented-out Post-processing Logic (`sed`, `sort`, `join`)**:
   - *Risk*: The legacy script contains commented-out code indicating that previously, data was outputted to files, sorted, joined, and saved as a CSV (`cibasisprodukt.csv`).
   - *Mitigation*: Verify with business stakeholders if this file output is still required. If yes, this represents a functionality gap; BigQuery should not export raw files during ETL. Instead, the joined and sorted records should be written directly to a BigQuery analytics table.
3. **`d_ausd_bp_ta_bpr_apn.sql` Transformation Logic**:
   - *Risk*: This script only maps the orchestration/control wrapper. The inner transformation logic inside the SQL script must also be converted to BigQuery syntax.
   - *Mitigation*: Ensure that the migration of `d_ausd_bp_ta_bpr_apn.sql` into the procedure `sp_d_ausd_bp_ta_bpr_apn` is prioritized alongside this orchestrator wrapper.

---

## 8. Build Plan
The following artifacts must be developed and executed in the target Google Cloud project in chronological order:

| Step | Artifact Name | Target Language / Technology | Purpose |
| :---: | :--- | :--- | :--- |
| **1** | `job_error_log` DDL | BigQuery SQL | Creates the log table to capture parameter and validation failures. |
| **2** | `job_audit_log` DDL | BigQuery SQL | Creates the operational table to track successful job completions and record metrics. |
| **3** | `sp_d_ausd_bp_ta_bpr_apn` (Placeholder/Actual) | BigQuery SQL | Deploys the main conversion logic of the SQL transformation. |
| **4** | `sp_k_ausd_bp_ta_bpr_apn` | BigQuery SQL | Deploys the main orchestration wrapper procedure designed in this document. |
| **5** | Cloud Composer DAG | Python (Apache Airflow) | Deploys the automated schedule that triggers the execution of the main BigQuery stored procedure. |