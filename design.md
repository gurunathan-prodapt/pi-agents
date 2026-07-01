# MIGRATION DESIGN DOCUMENT: ausd_bp_ta_apn_carmen

---

## 1. Executive Summary & Migration Strategy

### 1.1 Overview
This document specifies the migration plan for the legacy job **`ausd_bp_ta_apn_carmen`** from an on-premise Oracle / UC4 / KornShell (ksh) environment to **Google Cloud Platform (BigQuery and Cloud Composer / Apache Airflow)**. 

The original job extracts active, production-relevant GPRS/UMTS PDP contexts and Access Point Name (APN) associations for contracts from a remote database link (`@pcrs1` - Carmen source system), filters them by a historical dynamic cutoff date, and provisions a target table `sof$ta_apn_carmen` (translated to `sof.ta_apn_carmen` in BigQuery).

### 1.2 Target Architecture Strategy
- **Orchestration**: The UC4 job XML (`DW.BERT_AUSD_BP_TA_APN_CARMEN.xml`) and its two wrapper scripts (`r_ausd_bp_ta_apn_carmen.ksh`, `k_ausd_bp_ta_apn_carmen.ksh`) will be consolidated into a native **Cloud Composer (Apache Airflow) DAG**. 
- **Processing Engine**: The core Oracle SQL logic (`d_ausd_bp_ta_apn_carmen.sql`) will be migrated into a native **BigQuery Stored Procedure**. This stored procedure handles parameter extraction, date validation, target table truncation, and the transactional bulk insert of the mapped contract-to-APN records.
- **Data Integration**: Remote tables referenced over the Oracle DB Link (`@pcrs1`) must be ingested into a replica dataset in BigQuery (`carmen_replica`) prior to executing this workflow.

---

## 2. Lineage, Execution Flow & Dependencies

### 2.1 Upstream Producers & Inputs
1. **`isbert_schema.dwtk_meldungen`**: Contains the job execution events. The stored procedure reads the latest `timecreated` timestamp for the key `BERT_DROP_TEMP_TABLE` to determine the dynamic cutoff date (`v_datum`).
2. **`carmen_replica.pds_ta_pdp_context_assoc`** (originally `pds$ta_pdp_context_assoc@pcrs1`): Replicated table containing contract-to-PDP context mappings.
3. **`carmen_replica.pds_ta_pdp_context`** (originally `pds$ta_pdp_context@pcrs1`): Replicated table containing PDP context definitions.
4. **`carmen_replica.pds_ta_access_point`** (originally `pds$ta_access_point@pcrs1`): Replicated table containing APN definitions.

### 2.2 Downstream Consumers & Outputs
1. **`sof.ta_apn_carmen`** (originally `sof$ta_apn_carmen`): Target BigQuery table populated with mapped, active columns `CNTRCT_ID` and `ACCESS_POINT_NAME`.
2. **`isbert_schema.job_log`**: Audit and runtime monitoring log table.

### 2.3 Legacy to Target Call Chain
```
[Legacy Flow]
UC4 (DW.BERT_AUSD_BP_TA_APN_CARMEN) 
  --> r_ausd_bp_ta_apn_carmen.ksh (Wrapper & Log Init)
        --> k_ausd_bp_ta_apn_carmen.ksh (Validation & Sourcing yesterday/today)
              --> d_ausd_bp_ta_apn_carmen.sql (Truncate & Insert via SQL*Plus)

[Target Flow]
Cloud Composer (Airflow DAG: ausd_bp_ta_apn_carmen_dag)
  --> BigQuery Stored Procedure: `sof.proc_d_ausd_bp_ta_apn_carmen` (Truncate, Log, & ETL Insert)
```

---

## 3. Cross-File Dependencies & Shared Entities

- **Dynamic Cutoff Date (`v_datum`)**: The execution is tightly coupled to the timestamp produced by an upstream job named `BERT_DROP_TEMP_TABLE`. If this upstream job has not updated `isbert_schema.dwtk_meldungen`, the workflow defaults to `19000101`.
- **Schema Mapping**:
  - Oracle `sof$` schema prefix is normalized to BigQuery dataset `sof`.
  - Oracle `isbert_schema` is normalized to BigQuery dataset `isbert_schema`.
  - Oracle DB Link `@pcrs1` is replaced with target dataset `carmen_replica`.
- **Temporal Logic**: Slices are determined using inclusive temporal validity rules:
  - `insert_at <= cutoff AND (modified_at IS NULL OR modified_at > cutoff)`
  - `valid_from <= cutoff AND (valid_to IS NULL OR valid_to > cutoff)`

---

## 4. Target File Plan

| Relative Target Path | Language | Source Components | Purpose |
| :--- | :--- | :--- | :--- |
| `dags/ausd_bp_ta_apn_carmen_dag.py` | Python / Airflow | `DW.BERT_AUSD_BP_TA_APN_CARMEN.xml`, `r_*.ksh`, `k_*.ksh` | Orchestrates daily run, implements error triggers, and executes the BigQuery SQL procedure. |
| `sql/proc_d_ausd_bp_ta_apn_carmen.sql` | BigQuery SQL | `d_ausd_bp_ta_apn_carmen.sql` | Stored procedure encapsulating logic for date derivation, target table truncation, mapping, and population. |
| `sql/ddl_sof_ta_apn_carmen.sql` | BigQuery SQL | Implicit Schema | DDL to create the target BigQuery table `sof.ta_apn_carmen`. |

---

## 5. Environment-Specific Configuration (Build Parameters)

```json
{
  "gcp_project_id": "gcp-project-id",
  "airflow_connection_id": "bigquery_default",
  "datasets": {
    "target_dataset": "sof",
    "metadata_dataset": "isbert_schema",
    "source_dataset": "carmen_replica"
  },
  "tables": {
    "target_table": "ta_apn_carmen",
    "log_table": "job_log",
    "events_table": "dwtk_meldungen",
    "pdp_assoc": "pds_ta_pdp_context_assoc",
    "pdp_context": "pds_ta_pdp_context",
    "access_point": "pds_ta_access_point"
  }
}
```

---

## 6. Verification, Risks & Manual Steps

1. **Replication of `@pcrs1` Source Tables**: The pipeline expects tables `pds_ta_pdp_context_assoc`, `pds_ta_pdp_context`, and `pds_ta_access_point` to be continuously or batched-replicated to the BigQuery target project. Any lag in replication will lead to stale calculations of active APNs.
2. **Format of Dynamic Cutoff Date**: The dynamic cutoff date is retrieved in format `YYYYMMDD`. If format mismatches occur in the source `timecreated` column (e.g., if there are timezone shifts or unparsed timestamp offsets), it may shift the filtered records. The stored procedure explicitly uses `SAFE.PARSE_DATE('%Y%m%d', ...)` to handle unexpected formats gracefully.
3. **Transaction Blocks**: BigQuery scripting executes DML implicitly. There is no explicit need for a `COMMIT` statement in BigQuery unless the stored procedure is explicitly run within a multi-statement transaction (`BEGIN TRANSACTION ... COMMIT TRANSACTION`).

---

## 7. VERBATIM MCP TOOL MIGRATION OUTPUTS

The following segments are returned by the automated migration code patterns. Do not modify the structure or pseudocode blocks below, as they are consumed directly by downstream build engines.

### 7.1 Result for `DW.BERT_AUSD_BP_TA_APN_CARMEN.xml`
```markdown
Document: Shell Script Analysis

1. Purpose
- UC4/Automic UNIX job `DW.BERT_AUSD_BP_TA_APN_CARMEN`
- Title: `BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte`
- Primary purpose: initialize runtime context, execute a shell/KornShell processing script, then read/log results.
- The actual business logic is not embedded in this XML; it is delegated to:
  - `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh`
  - included log reader `DW.BERT_LESE_LOG`

2. Input Parameters
- No explicit UC4 dynamic variables are defined in this object.
- Implicit runtime inputs:
  - `&DWH_JOB_KENNUNG='AUSD_BP_TA_APN_CARMEN'`
  - environment from `. $HOME/.dw_init`
  - shell environment variables from `DW.HOLE_PFAD` include
  - `HOME` path and any variables sourced by `.dw_init`
- External script path depends on `HOME`

3. Outputs
- No direct file output configured in UC4 object.
- Expected outputs are likely:
  - database changes and/or log entries produced by the called `.ksh` script
  - log consumption/reading via `DW.BERT_LESE_LOG`
- UC4 output settings:
  - `OutputDb=1`
  - `OutputFile=0`

4. Dependencies
- UC4 include objects:
  - `DW.HOLE_PFAD`
  - `DW.BERT_LESE_LOG`
- External shell initialization:
  - `$HOME/.dw_init`
- External executable/script:
  - `$HOME/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh`
- Execution context:
  - UNIX host `|DWHDWH2P|HOST`
  - login `DW.UNIX.ISBERT`

5. Logic Flow
- Include path setup via `DW.HOLE_PFAD`
- Set job identifier variable
- Source environment initialization file
- Execute the main KornShell script
- Read log via included UC4 object

6. External File / Tool Usage
- Shell environment file: `$HOME/.dw_init`
- KornShell script: `r_ausd_bp_ta_apn_carmen.ksh`
- UC4 include objects for path and log handling
- No APIs are referenced in the XML itself

7. Replicability in BigQuery
- The UC4 wrapper logic can be replicated with BigQuery scripting:
  - procedure parameters replace environment variables
  - logging can be implemented with audit tables
  - the external `.ksh` script logic must be translated separately into SQL if it performs data transformations
- Since the actual `.ksh` content is not provided, only the wrapper orchestration can be directly mapped.
- BigQuery stored procedures can replicate:
  - variable assignment
  - conditional execution
  - logging inserts
  - calling subordinate procedures
- If the `.ksh` script contains filesystem operations, those must be replaced with:
  - BigQuery tables
  - Cloud Storage staging
  - external orchestration if needed
- Restartability note:
  - docu says `Restart jederzeit möglich`
  - in BigQuery this should be implemented with idempotent writes and run-control tables

Break Down of the Script

1. Data Extraction and Ingestion
- Not visible in the XML wrapper
- Likely performed inside `r_ausd_bp_ta_apn_carmen.ksh`

2. Transformations and Aggregations
- Not visible in the XML wrapper
- Likely performed inside the external KornShell script

3. Error Handling and Logging
- UC4 runtime handles return code `MaxRetCode=0`
- Log reading is explicitly invoked by `DW.BERT_LESE_LOG`
- Restartability is documented

4. Output or Result Storage
- No file output configured
- Likely database output or log output from the external script

Assumptions and Additional Notes
- The provided artifact is an Automic/UC4 job definition, not the actual shell script body.
- Therefore, only orchestration-level logic can be translated with certainty.
- The external `.ksh` script must be separately analyzed for full SQL migration.
- BigQuery translation below models:
  - environment setup
  - job identifier assignment
  - execution of a subordinate processing routine
  - log capture
- Any file-based or shell-specific behavior is represented as placeholders or external procedure calls.
- No renaming of business identifiers is introduced.

BigQuery SQL Pseudocode

```sql
-- BigQuery Script / Stored Procedure Pseudocode
-- Job: DW.BERT_AUSD_BP_TA_APN_CARMEN

CREATE OR REPLACE PROCEDURE `project.dataset.DW_BERT_AUSD_BP_TA_APN_CARMEN`(
  IN p_job_kennung STRING,
  IN p_home STRING
)
BEGIN
  DECLARE v_job_kennung STRING DEFAULT 'AUSD_BP_TA_APN_CARMEN';
  DECLARE v_home STRING DEFAULT p_home;
  DECLARE v_status STRING DEFAULT 'STARTED';
  DECLARE v_error_message STRING DEFAULT NULL;
  DECLARE v_run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  -- Equivalent to: :inc DW.HOLE_PFAD
  -- Assumption: path/bootstrap values are provided via procedure parameters or session variables.
  -- Example placeholder for path bootstrap:
  DECLARE v_sql_path STRING DEFAULT CONCAT(v_home, '/SQL/aktuell/aufbereitung/bin/');
  DECLARE v_script_name STRING DEFAULT 'r_ausd_bp_ta_apn_carmen.ksh';

  -- Equivalent to: :set &DWH_JOB_KENNUNG='AUSD_BP_TA_APN_CARMEN'
  SET v_job_kennung = 'AUSD_BP_TA_APN_CARMEN';

  -- Equivalent to: . $HOME/.dw_init
  -- Assumption: environment initialization is represented by configuration tables or parameters.
  -- Example: load runtime config from a control table if needed.
  -- SELECT config_value INTO ... FROM `project.dataset.dw_init_config` WHERE ...

  BEGIN
    -- Equivalent to executing the external KornShell script
    -- If the shell script contains SQL logic, migrate that logic into this block.
    CALL `project.dataset.r_ausd_bp_ta_apn_carmen_sql`(v_job_kennung);

    SET v_status = 'SUCCESS';

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'ERROR';
    SET v_error_message = @@error.message;

    -- Persist error log
    INSERT INTO `project.dataset.dw_job_log`
    (
      job_kennung,
      run_ts,
      status,
      error_message,
      source_object
    )
    VALUES
    (
      v_job_kennung,
      v_run_ts,
      v_status,
      v_error_message,
      v_script_name
    );

    -- Re-raise error to preserve failure semantics
    RAISE USING MESSAGE = v_error_message;
  END;

  -- Persist success log
  INSERT INTO `project.dataset.dw_job_log`
  (
    job_kennung,
    run_ts,
    status,
    error_message,
    source_object
  )
  VALUES
  (
    v_job_kennung,
    v_run_ts,
    v_status,
    NULL,
    v_script_name
  );

  -- Equivalent to: :inc DW.BERT_LESE_LOG
  -- Assumption: log reading is implemented as a query against the log table.
  SELECT
    job_kennung,
    run_ts,
    status,
    error_message,
    source_object
  FROM `project.dataset.dw_job_log`
  WHERE job_kennung = v_job_kennung
    AND run_ts = v_run_ts;
END;
```

Low-Level Pseudocode

```text
PROCEDURE DW_BERT_AUSD_BP_TA_APN_CARMEN(p_job_kennung, p_home):

  DECLARE v_job_kennung = 'AUSD_BP_TA_APN_CARMEN'
  DECLARE v_home = p_home
  DECLARE v_run_ts = current timestamp
  DECLARE v_status = 'STARTED'
  DECLARE v_error_message = NULL

  SET v_job_kennung = 'AUSD_BP_TA_APN_CARMEN'

  LOAD runtime configuration equivalent to sourcing $HOME/.dw_init
  BUILD script path from v_home + '/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh'

  TRY:
      CALL subordinate BigQuery procedure that contains migrated logic
      SET v_status = 'SUCCESS'
  CATCH error:
      SET v_status = 'ERROR'
      SET v_error_message = error message
      INSERT error row into job log table
      STOP with error

  INSERT success row into job log table

  QUERY job log table for current job/run and return results
END PROCEDURE
```

Python Pseudocode (if applicable)

```python
# Only needed if the external .ksh contains unsupported filesystem or parsing logic.
# This is a placeholder for isolated migration of incompatible operations.

def r_ausd_bp_ta_apn_carmen_python(job_kennung: str, home: str):
    """
    Isolated Python function for logic that cannot be expressed in BigQuery SQL.
    """
    script_path = f"{home}/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh"

    # Load external configuration equivalent to .dw_init
    # Read files / parse text / perform OS-level operations if required
    # Convert results into tabular records for BigQuery ingestion

    result_rows = []
    # ... unsupported logic here ...

    return result_rows
```

Configuration Files Required for BigQuery Execution
- BigQuery stored procedure definition file
- Optional runtime configuration table or JSON config source to replace `.dw_init`
- Optional job control / audit table DDL
- Optional orchestration config for:
  - Cloud Composer
  - Workflows
  - Scheduled Query
- Optional external Python deployment package only if unsupported shell logic must be isolated
```

---

### 7.2 Result for `r_ausd_bp_ta_apn_carmen.ksh`
```markdown
Document: Shell Script Analysis

1. Purpose
- Job name: `Bereitstellung Basisprodukte BERT`
- Version: `V2.0.0`
- Main purpose:
  - Prepare and provide selected base products for BERT/FOS.
  - Create a cutoff-date extraction of contract cache data from DWH.
  - Deliver records to scoring/FOS processing.
  - Support restart/resume behavior via a restart value.
- Core business rule:
  - Select records where:
    - `Gueltig_von <= Stichtag`
    - `Stichtag < Gueltig_bis`
    - `LADEDATUM < Stichtag`
  - If no cutoff date is provided, use the minimum of current system date and maximum load date from source; in this script, the fallback is effectively the current system date.

2. Input Parameters
- `-h`
  - Displays usage/help text.
- `-s <DDMMYYYY>`
  - Cutoff date (`Stichtag`).
- `-l <restart_value>`
  - Restart value (`Wiederanlaufwert`).
  - If set, only contracts with `DWH_VERTRAG_ID > restart_value` are processed.
  - Existing entries with values `>= restart_value` are deleted in downstream logic.

3. Outputs
- Log file created via logging framework.
- Status entry written to job log/status system.
- Delegation to core script:
  - `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh`
- Final success message:
  - `Die Abarbeitung wurde ohne erkennbare Fehler beendet`

4. Dependencies
- Environment initialization:
  - `$HOME/.dw_init`
- Error/logging framework:
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
- Parameter helper:
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
- Date helper:
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
- Core processing script:
  - `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh`
- External functions/macros used:
  - `DWDate_Gib_Zeitraum`
  - `pruefeParameterGesetzt`
  - `DWMSG_ErmittleNr`
  - `DWMSG_Logdateiname`
  - `DWMSG_ErzeugeEintrag`
  - `DWMSG_SetzeStichtagInfo`
  - `DWMSG_Fehlerbehandlung`
  - `DWMSG_SetzeStatusOK`
  - `DWMSG_MeldeFehler`
- Shell/system features:
  - `getopts`
  - `trap`
  - `set -e`
  - `print`
  - `tee`

5. Logic Flow
- Load environment and helper libraries.
- Parse command-line options.
- Initialize restart value to `0` if missing.
- Determine system date in `DDMMYYYY`.
- If cutoff date is missing, default it to system date.
- Validate required parameters.
- If validation fails:
  - Log error
  - Print usage
  - Exit with error code
- Initialize job logging metadata.
- Register traps for interrupt/error handling.
- Print job header information.
- Call the core script with:
  - job identifier
  - cutoff date
  - job entry number
  - restart value
- On success:
  - Append success message to log
  - Mark job status OK
- Clear traps and exit `0`.

6. External File / Tool Usage
- Reads shell initialization and helper scripts from filesystem.
- Writes to log file.
- Invokes another shell script as the main processing engine.
- No direct database access in this wrapper script; database logic is assumed to be inside the core script.

7. BigQuery Replicability Assessment
- Fully replicable in BigQuery:
  - Parameter handling via stored procedure parameters.
  - Defaulting logic via procedural `IF`.
  - Validation via `ASSERT`-like checks or `IF ... THEN SIGNAL`.
  - Logging via audit tables.
  - Job metadata generation via sequence/table-based counters.
  - Core data filtering and restart logic via SQL.
- Not directly replicable in pure BigQuery SQL:
  - Shell traps
  - File-based logging
  - Sourcing environment files
  - Calling another `.ksh` script
- Recommended BigQuery replacement:
  - Stored procedure as orchestration layer.
  - Audit/log tables for job tracking.
  - Optional Cloud Workflows / Cloud Composer for external orchestration if needed.

8. Functionality Gaps and Alternatives
- Gap: `DWMSG_*` logging framework
  - Alternative: insert rows into `job_log` / `job_status` tables.
- Gap: `DWDate_Gib_Zeitraum`
  - Alternative: `CURRENT_DATE()` and `FORMAT_DATE()` / `PARSE_DATE()`.
- Gap: `pruefeParameterGesetzt`
  - Alternative: procedural validation in BigQuery.
- Gap: core script execution
  - Alternative: migrate core logic into BigQuery stored procedure or SQL script.
- Gap: trap-based error handling
  - Alternative: `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;`
- Gap: restart deletion behavior
  - Alternative: `DELETE` statements in BigQuery before insert/merge.

Break Down the Script

1. Data Extraction and Ingestion
- No direct extraction in wrapper.
- Delegates extraction/ingestion to core script.
- In BigQuery migration, this becomes:
  - source table scan
  - filtered insert into target table
  - optional delete for restart handling

2. Transformations and Aggregations
- Wrapper itself performs no transformations.
- Only parameter normalization:
  - default restart value to `0`
  - default cutoff date to system date

3. Error Handling and Logging
- Uses `set -e`
- Uses `trap` for `INT`, `STOP`, `CONT`, `ERR`
- Logs job metadata and status
- On parameter error:
  - emits error
  - prints usage
  - exits with code
- On success:
  - writes success message
  - sets status OK

4. Output or Result Storage
- Log file output
- Job status tracking
- Downstream target table population handled by core script

Assumptions and Additional Notes
- The wrapper script is an orchestration layer; the actual business SQL is in the core script.
- BigQuery migration assumes the core script’s logic will be implemented as SQL DML/DDL inside a stored procedure.
- Job numbering/logging must be replaced by a metadata table or generated identifier.
- If the source max load date logic is required, it must be implemented in SQL against the source table.
- If the downstream process depends on filesystem logs, those must be replaced by BigQuery audit tables or external orchestration logs.

Pseudocode: BQ SQL Pseudocode

```sql
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_apn_carmen`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_apn_carmen';
  DECLARE v_eintragsnr INT64;
  DECLARE v_log_message STRING;

  -- Job metadata / logging replacement
  INSERT INTO `project.dataset.job_log`
    (job_name, event_type, event_ts, message)
  VALUES
    (v_jobkennung, 'START', CURRENT_TIMESTAMP(), 'Job started');

  -- Initialize restart value
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- Determine system date in DDMMYYYY
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Determine cutoff date
  IF p_stichtag IS NULL OR p_stichtag = '' THEN
    SET v_stichtag = v_sysdate;
  ELSE
    SET v_stichtag = p_stichtag;
  END IF;

  -- Validate required parameter
  IF v_stichtag IS NULL OR v_stichtag = '' THEN
    INSERT INTO `project.dataset.job_log`
      (job_name, event_type, event_ts, message)
    VALUES
      (v_jobkennung, 'ERROR', CURRENT_TIMESTAMP(), 'Missing Stichtag');
    RAISE USING MESSAGE = 'Stichtag is required';
  END IF;

  -- Optional: parse DDMMYYYY to DATE for comparisons
  DECLARE v_stichtag_date DATE;
  SET v_stichtag_date = PARSE_DATE('%d%m%Y', v_stichtag);

  -- Optional: restart cleanup
  IF v_wiederanlaufWert > 0 THEN
    DELETE FROM `project.dataset.target_table`
    WHERE DWH_VERTRAG_ID >= v_wiederanlaufWert;
  END IF;

  -- Core extraction and load
  INSERT INTO `project.dataset.target_table`
  SELECT
    src.*
  FROM `project.dataset.source_contract_cache` AS src
  WHERE DATE(src.Gueltig_von) <= v_stichtag_date
    AND v_stichtag_date < DATE(src.Gueltig_bis)
    AND DATE(src.LADEDATUM) < v_stichtag_date
    AND (v_wiederanlaufWert = 0 OR src.DWH_VERTRAG_ID > v_wiederanlaufWert);

  -- Success logging
  INSERT INTO `project.dataset.job_log`
    (job_name, event_type, event_ts, message)
  VALUES
    (v_jobkennung, 'SUCCESS', CURRENT_TIMESTAMP(), 'Die Abarbeitung wurde ohne erkennbare Fehler beendet');

EXCEPTION WHEN ERROR THEN
  INSERT INTO `project.dataset.job_log`
    (job_name, event_type, event_ts, message)
  VALUES
    (v_jobkennung, 'FAILURE', CURRENT_TIMESTAMP(), 'AppError: Abbruch');
  RAISE;
END;
```

Low-Level Pseudocode

```text
PROCEDURE ausd_bp_ta_apn_carmen(p_stichtag, p_wiederanlaufWert)

  SET job name = 'ausd_bp_ta_apn_carmen'
  SET restart value = p_wiederanlaufWert if provided else 0
  SET sysdate = current date formatted as DDMMYYYY

  IF p_stichtag is missing
    SET stichtag = sysdate
  ELSE
    SET stichtag = p_stichtag
  END IF

  IF stichtag is missing
    WRITE error log
    STOP with error
  END IF

  WRITE start log entry

  IF restart value > 0
    DELETE rows from target where DWH_VERTRAG_ID >= restart value
  END IF

  SELECT rows from source where
    Gueltig_von <= stichtag
    AND stichtag < Gueltig_bis
    AND LADEDATUM < stichtag
    AND (restart value = 0 OR DWH_VERTRAG_ID > restart value)

  INSERT selected rows into target

  WRITE success log entry
  RETURN success

EXCEPTION
  WRITE failure log entry
  RETURN error
END PROCEDURE
```

Python Pseudocode (if applicable)

```python
def validate_and_prepare(p_stichtag, p_wiederanlaufWert):
    v_wiederanlaufWert = p_wiederanlaufWert if p_wiederanlaufWert is not None else 0
    v_sysdate = current_date_ddmmyyyy()

    if not p_stichtag:
        v_stichtag = v_sysdate
    else:
        v_stichtag = p_stichtag

    if not v_stichtag:
        raise ValueError("Stichtag is required")

    return v_stichtag, v_wiederanlaufWert
```

Configuration Files Required for BigQuery Execution
- BigQuery stored procedure deployment script
- Dataset/table DDL for:
  - source table
  - target table
  - job log table
- Optional configuration for orchestration:
  - Cloud Composer DAG or Cloud Workflow definition
  - Cloud Scheduler trigger configuration
- Optional parameter/config file:
  - default dataset/project identifiers
  - restart handling policy
  - logging table schema
```

---

### 7.3 Result for `k_ausd_bp_ta_apn_carmen.ksh`
```markdown
Document: Shell Script Analysis

1. Purpose
- Control script for `r_ausd_bp_ta_apn_carmen.ksh`.
- Performs parameter validation, date validation, SQL script execution, and post-processing of record count.
- Intended to orchestrate a database extraction/load workflow for table `PoolBasisprodukt`.

2. Input Parameters
- `-j p_JobKennung`: Job identifier.
- `-f p_EintragsNr`: Entry number.
- `-s p_Stichtag`: Key date / as-of date in `DDMMYYYY`.
- `-l p_wiederanlaufWert`: Restart/recovery value, optional.
- `-h`: Help message and exit.

3. Outputs
- Executes SQL script: `d_ausd_bp_ta_apn_carmen.sql`.
- Writes record count to temporary file: `$DW_DIR_UTL/bert_k_ausd_bp_ta_apn_carmen.tmp`.
- Prints status messages.
- Potentially logs errors via `DWMSG_MeldeFehler`.
- Final record count is read from temp file into `v_records`.

4. Dependencies
- Environment file: `$HOME/.dw_init`
- Utility scripts:
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`
  - `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`
  - `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`
- SQL script:
  - `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_apn_carmen.sql`
- Temporary file:
  - `$DW_DIR_UTL/bert_k_ausd_bp_ta_apn_carmen.tmp`

5. Logic Flow
- Source environment and helper scripts.
- Parse command-line parameters using `getopts`.
- Validate required parameters:
  - Jobkennung
  - Stichtag
  - EintragsNr
- If validation fails:
  - Emit error message
  - Exit with error code
- Validate date format `DDMMYYYY`.
- Source SQL execution helper.
- Initialize SQL script path and temp file path.
- Default restart value to `0` if not provided.
- Compute yesterday and today via `gestern.ksh`.
- Execute SQL script with parameters.
- Print end-of-processing message.
- Read record count from temp file.
- Job-table insertion is commented out.

6. External File Dependencies / APIs / Tools
- Korn shell (`/bin/ksh`)
- `getopts`
- External shell helper functions
- SQL execution wrapper around SQL*Plus-like behavior
- File system temp file read/write
- Commented-out Unix text tools:
  - `sed`
  - `sort`
  - `join`
  - `cat`

7. Replicability in BigQuery SQL / Stored Procedures
- Parameter parsing maps to stored procedure parameters.
- Required parameter validation maps to procedural `IF` checks.
- Date validation can be implemented with `SAFE.PARSE_DATE` or regex checks.
- SQL script execution logic can be migrated into BigQuery scripting and stored procedures.
- Record count can be computed directly in SQL instead of reading a temp file.
- Yesterday/today derivation can be done with `DATE_SUB` and `CURRENT_DATE`.
- Commented file post-processing can be replicated with SQL joins, deduplication, and string cleanup.
- Job logging can be implemented using audit tables.

8. Functionality Gaps / Alternatives
- Shell-specific error framework (`DWMSG_MeldeFehler`) requires replacement with:
  - BigQuery logging table
  - Stored procedure `ASSERT`/`RAISE`-style handling via `SELECT ERROR(...)` pattern if supported in orchestration layer
- File-based temp output must be replaced by:
  - BigQuery tables
  - Temporary tables
  - Script variables
- `gestern.ksh` must be replaced by SQL date functions.
- Any SQL*Plus-specific behavior must be rewritten in BigQuery SQL.
- If downstream file generation is required, use:
  - BigQuery export to Cloud Storage
  - External orchestration workflow
- Commented `sed/sort/join` pipeline is fully SQL-migratable.

9. Script Segmentation
- Data extraction and ingestion
  - SQL script execution via `starteSQLSkript`
- Transformations and aggregations
  - Commented post-processing pipeline:
    - remove blanks
    - sort unique
    - join datasets
- Error handling and logging
  - Parameter validation
  - Date validation
  - Error message emission
- Output or result storage
  - Temp file record count
  - Potential job table entry

Assumptions and Additional Notes
- The actual SQL logic resides in `d_ausd_bp_ta_apn_carmen.sql` and is not present here; only orchestration logic is analyzed.
- Commented-out sections are treated as intended but inactive functionality.
- BigQuery implementation assumes the SQL script’s business logic can be embedded into a stored procedure or script.
- Any file-based intermediate artifacts should be replaced by BigQuery tables or temporary tables.
- If exact shell error semantics are required, an orchestration layer may be needed around BigQuery.

Pseudocode: BQ SQL Pseudocode

```sql
-- BigQuery Stored Procedure / Script Pseudocode

CREATE OR REPLACE PROCEDURE dataset.r_ausd_bp_ta_apn_carmen(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING,
  p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_err STRING DEFAULT '';
  DECLARE v_tmp_records INT64 DEFAULT 0;

  -- Required parameter checks
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_err = 'Jobkennung fehlt';
    SELECT ERROR(CONCAT('FEHLER: ', v_err));
  END IF;

  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    SET v_err = 'Stichtag fehlt';
    SELECT ERROR(CONCAT('FEHLER: ', v_err));
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    SET v_err = 'EintragsNr fehlt';
    SELECT ERROR(CONCAT('FEHLER: ', v_err));
  END IF;

  -- Date validation DDMMYYYY
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    SELECT ERROR('FEHLER: Stichtag hat ungueltiges Format DDMMYYYY');
  END IF;

  -- Default restart value
  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = '' THEN
    SET p_wiederanlaufWert = '0';
  END IF;

  -- Main SQL logic migrated from d_ausd_bp_ta_apn_carmen.sql
  -- Replace SQL*Plus execution with native BigQuery SQL statements
  -- Example placeholder:
  -- INSERT INTO dataset.target_table (...)
  -- SELECT ...
  -- FROM source_tables
  -- WHERE ...

  -- Commented shell post-processing migrated to SQL if needed:
  -- 1) remove blanks
  -- 2) deduplicate
  -- 3) join datasets
  -- 4) store final output

  -- Example of record count calculation
  SET v_records = (
    SELECT COUNT(*)
    FROM dataset.target_table
    WHERE stichtag = v_stichtag_date
  );

  -- Optional job logging table insert
  INSERT INTO dataset.job_log_table (
    tab_name,
    status_code,
    process_type,
    stichtag_from,
    stichtag_to,
    job_type,
    restart_flag,
    record_count,
    description
  )
  VALUES (
    v_TabName,
    'A',
    'I',
    v_stichtag_date,
    v_stichtag_date,
    'J',
    'N',
    v_records,
    'Initialbefuellung'
  );

  -- Final status output
  SELECT ' ---------- ENDE Datenverarbeitung ----------' AS message;
END;
```

Python Pseudocode (if applicable)

```python
# Only for unsupported orchestration or file-based behavior
# Keep isolated and minimal; migrate core data logic to BigQuery SQL.

def validate_stichtag_ddmmyyyy(p_stichtag: str) -> bool:
    from datetime import datetime
    try:
        datetime.strptime(p_stichtag, "%d%m%Y")
        return True
    except ValueError:
        return False

def compute_dates():
    from datetime import date, timedelta
    heute = date.today()
    gestern = heute - timedelta(days=1)
    return heute, gestern

def default_restart_value(p_wiederanlaufWert):
    return p_wiederanlaufWert if p_wiederanlaufWert not in (None, "") else "0"
```

Configuration Files Required for BigQuery Execution
- BigQuery dataset and schema configuration
- Stored procedure deployment script
- Optional orchestration config:
  - Cloud Composer DAG
  - Cloud Workflows definition
  - Cloud Scheduler trigger
- Optional logging table DDL
- Optional target/source table DDL
- Optional service account / IAM configuration for execution
```

---

### 7.4 Result for `d_ausd_bp_ta_apn_carmen.sql`
```markdown
Document: Shell Script Analysis

1. Purpose
- The script builds a local temporary result table `sof$ta_apn_carmen` containing contract-to-access-point mappings.
- It extracts active, production-relevant APN associations from versioned source tables filtered by a runtime cutoff date.
- It truncates the target table first to support reruns on the same day.
- It logs execution trace and exits successfully when complete.

2. Input Parameters / Runtime Variables
- `v_carmen = "@pcrs1"`
  - Used as a table suffix/version selector for source tables:
    - `pds$ta_pdp_context_assoc &v_carmen`
    - `pds$ta_pdp_context &v_carmen`
    - `pds$ta_access_point &v_carmen`
- `v_datum`
  - Derived dynamically from `isbert_schema.dwtk_meldungen`
  - Computed as `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`
  - Defaults to `19000101` if no matching row exists

3. Outputs
- Populates `sof$ta_apn_carmen` with:
  - `CNTRCT_ID`
  - `ACCESS_POINT_NAME`
- Produces trace output in:
  - `./tmp/trace_d_ausd_apn_carmen.trc`

4. Dependencies
- Database objects:
  - `isbert_schema.dwtk_meldungen`
  - `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`
  - `sof$ta_apn_carmen`
  - `pds$ta_pdp_context_assoc@pcrs1`
  - `pds$ta_pdp_context@pcrs1`
  - `pds$ta_access_point@pcrs1`
- SQL*Plus / Oracle scripting features:
  - `DEFINE`
  - `COLUMN ... NEW_VALUE`
  - `PROMPT`
  - `START`
  - `SPOOL`
  - `WHENEVER SQLERROR`
  - `COMMIT`
  - `EXIT`
- Oracle date functions:
  - `NVL`
  - `TO_CHAR`
  - `TO_DATE`
  - `MAX`

5. Logic Flow
- Step00:
  - Define version/table suffix variable.
  - Derive cutoff date from message table.
  - Start trace and enable timing/error handling.
- Step01:
  - Truncate target table to clear prior run data.
- Step10:
  - Insert filtered join results into target table.
  - Apply temporal validity checks against cutoff date.
  - Restrict to production contexts.
  - Exclude null contract IDs.
- Final:
  - Commit transaction.
  - Stop trace.
  - Exit success.

6. External File / Tool Usage
- External trace configuration:
  - `../trace.sql.cfg`
- Trace spool file:
  - `./tmp/trace_d_ausd_apn_carmen.trc`
- Oracle stored procedure call:
  - `DWPA_UTIL_SKRIPT.runstatement`

7. Replicability in BigQuery
- Fully replicable in BigQuery SQL and stored procedures.
- Oracle SQL*Plus variable substitution maps to:
  - Stored procedure parameters
  - BigQuery scripting variables
- `TRUNCATE TABLE` is supported in BigQuery.
- `INSERT INTO ... SELECT` is supported in BigQuery.
- Date filtering and null handling are directly supported.
- Trace/spool behavior is not native to BigQuery and must be replaced by:
  - Logging tables
  - Cloud Logging
  - Orchestration-layer logging

8. Functionality Gaps / Alternatives
- SQL*Plus trace/spool:
  - Replace with audit/log table inserts or external orchestration logs.
- `runstatement` wrapper:
  - Replace with direct `TRUNCATE TABLE` in BigQuery script.
- Dynamic table suffix `@pcrs1`:
  - Replace with explicit dataset/table parameterization or static table references.
- If source tables are partitioned or versioned externally:
  - Use BigQuery views, table decorators, or dataset parameters.
- No unsupported business logic requiring Python is present.

Break Down of the Script

1. Data Extraction and Ingestion
- Reads cutoff date from message table.
- Selects rows from three source tables.
- Inserts qualifying rows into target table.

2. Transformations and Aggregations
- No aggregation in final insert.
- Only one scalar aggregation:
  - `MAX(timecreated)` to derive cutoff date.
- Temporal filtering:
  - `insert_at <= cutoff`
  - `modified_at is null OR modified_at > cutoff`
  - `valid_from <= cutoff`
  - `valid_to is null OR valid_to > cutoff`
- Production filter:
  - `pc.is_production = 1`

3. Error Handling and Logging
- SQL error handling configured to continue during truncate step.
- SQL error handling configured to exit failure afterward.
- Trace file generation via SQL*Plus spool.
- No explicit exception block beyond SQL*Plus directives.

4. Output or Result Storage
- Final data stored in `sof$ta_apn_carmen`.
- No intermediate file output besides trace.

Mapping Bash/SQL*Plus Constructs to BigQuery SQL

1. Environment Variables
- `DEFINE v_carmen = "@pcrs1"`
  - Map to procedure parameter, e.g. `p_carmen_suffix STRING`
  - Or hardcode dataset/table names in BigQuery script

2. Loops
- None present.

3. Conditionals
- No explicit `IF/ELSE` in script.
- Null/date logic maps directly to SQL predicates.

4. Functions and Commands
- `NVL` -> `IFNULL`
- `TO_CHAR(MAX(...),'YYYYMMDD')` -> `FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated)))` or `FORMAT_TIMESTAMP`
- `TO_DATE('&v_datum','YYYYMMDD')` -> `PARSE_DATE('%Y%m%d', v_datum)`
- `TRUNCATE TABLE` -> supported directly
- `COMMIT` -> optional in BigQuery scripting; transactions are implicit unless explicit transaction block used

5. File Manipulation
- `spool` and `start` are not replicated in SQL.
- Replace with:
  - logging table writes
  - orchestration logs
  - external workflow artifacts

Assumptions and Additional Notes
- `timecreated`, `insert_at`, `modified_at`, `valid_from`, `valid_to` are assumed to be DATE/TIMESTAMP-compatible fields.
- `pds$ta_* @pcrs1` source tables are assumed to be available in BigQuery as tables or views with equivalent names or mapped dataset names.
- `sof$ta_apn_carmen` is assumed to be a BigQuery target table with columns:
  - `CNTRCT_ID`
  - `ACCESS_POINT_NAME`
- If source timestamps are TIMESTAMP rather than DATE, comparisons should use `DATE()` or TIMESTAMP-aware cutoff logic consistently.
- No Python is required for this script because all logic is SQL-native in BigQuery.
- If exact Oracle semantics for date/time truncation are required, use explicit casting in BigQuery.

Configuration Files Required for BigQuery Execution
- Optional orchestration config:
  - YAML/JSON for scheduled query or workflow parameterization
- Optional logging config:
  - Cloud Logging sink or audit table schema definition
- Optional dataset/table mapping config:
  - To map Oracle suffix-based tables to BigQuery datasets/views
- No mandatory Python configuration file is required

Pseudocode: BQ SQL Pseudocode

```sql
-- BigQuery Script / Stored Procedure Pseudocode

DECLARE v_carmen STRING DEFAULT '@pcrs1';
DECLARE v_datum STRING;

SET v_datum = (
  SELECT IFNULL(
           FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))),
           '19000101'
         )
  FROM `isbert_schema.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step01: truncate target table
TRUNCATE TABLE `sof.ta_apn_carmen`;

-- Step10: insert filtered contract/APN mappings
INSERT INTO `sof.ta_apn_carmen` (
  CNTRCT_ID,
  ACCESS_POINT_NAME
)
SELECT
  pca.cntrct_id AS CNTRCT_ID,
  ap.access_point_name AS ACCESS_POINT_NAME
FROM `pds.ta_pdp_context_assoc_pcrs1` AS pca
JOIN `pds.ta_pdp_context_pcrs1` AS pc
  ON pca.pdp_context_id = pc.pdp_context_id
JOIN `pds.ta_access_point_pcrs1` AS ap
  ON pc.access_point_id = ap.access_point_id
WHERE pca.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (pca.modified_at IS NULL OR pca.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND pca.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
  AND (pca.valid_to IS NULL OR pca.valid_to > PARSE_DATE('%Y%m%d', v_datum))
  AND pc.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (pc.modified_at IS NULL OR pc.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND pc.is_production = 1
  AND ap.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
  AND (ap.modified_at IS NULL OR ap.modified_at > PARSE_DATE('%Y%m%d', v_datum))
  AND pca.cntrct_id IS NOT NULL;
```

BQ-Compliant Stored Procedure Pseudocode

```sql
CREATE OR REPLACE PROCEDURE `sof.proc_d_ausd_basisprodukt`()
BEGIN
  DECLARE v_carmen STRING DEFAULT '@pcrs1';
  DECLARE v_datum STRING DEFAULT '19000101';

  SET v_datum = (
    SELECT IFNULL(
             FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))),
             '19000101'
           )
    FROM `isbert_schema.dwtk_meldungen`
    WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  TRUNCATE TABLE `sof.ta_apn_carmen`;

  INSERT INTO `sof.ta_apn_carmen` (
    CNTRCT_ID,
    ACCESS_POINT_NAME
  )
  SELECT
    pca.cntrct_id,
    ap.access_point_name
  FROM `pds.ta_pdp_context_assoc_pcrs1` AS pca
  JOIN `pds.ta_pdp_context_pcrs1` AS pc
    ON pca.pdp_context_id = pc.pdp_context_id
  JOIN `pds.ta_access_point_pcrs1` AS ap
    ON pc.access_point_id = ap.access_point_id
  WHERE pca.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
    AND (pca.modified_at IS NULL OR pca.modified_at > PARSE_DATE('%Y%m%d', v_datum))
    AND pca.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
    AND (pca.valid_to IS NULL OR pca.valid_to > PARSE_DATE('%Y%m%d', v_datum))
    AND pc.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
    AND (pc.modified_at IS NULL OR pc.modified_at > PARSE_DATE('%Y%m%d', v_datum))
    AND pc.is_production = 1
    AND ap.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
    AND (ap.modified_at IS NULL OR ap.modified_at > PARSE_DATE('%Y%m%d', v_datum))
    AND pca.cntrct_id IS NOT NULL;
END;
```

Python Pseudocode (if applicable)

```python
# Not required for this script.
# All logic is directly translatable to BigQuery SQL.
```
```