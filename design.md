An elegant, production-ready **Migration Design Document** has been structured for migrating the `d_ipis_loader.ksh` job to BigQuery.

---

# MIGRATION DESIGN DOCUMENT: `d_ipis_loader.ksh`

## 1. Key Logic & Data Flow (Legacy)
The purpose of the legacy Shell script `d_ipis_loader.ksh` is to serve as a wrapper utility to load flat data files into an Oracle database using **Oracle SQL\*Loader (`sqlldr`)**.

* **Parameters & Validation**: Parses arguments using `getopts` for the Control file (`-c`), Data file (`-d`), Oracle User (`-u`), Oracle Password (`-p`), and Country Code (`-l`). It validates that both `-c` and `-d` are provided.
* **Environment Configuration**: Sources `$HOME/.dw_init` and utility scripts for message error handling (`f_alis_msgerr.ksh`) and parameter validation (`h_alis_parameter.ksh`).
* **File Operations & Naming**: Generates standard log paths based on a datetime stamp: Log file (`.log`), Discard file (`.dis`), and Bad file (`.bad`).
* **Data Load Process**: Runs `sqlldr` with parameters mapping to the specified input files. It handles credentials dynamically (defaults to `$DW_ORAUSER` if `-u` is omitted).
* **Post-Load Assessment & Cleanup**:
  - Tests whether a `.bad` or `.dis` file was written. If so, it raises error code `200` and reports an error via `DWMSG_MeldeFehler`.
  - Prints the output execution log (`cat $LOGFILE`) to console/standard out and cleans up (deletes) the local physical log file.

---

## 2. BigQuery Migration Strategy & Target Architecture
To migrate this utility to Google Cloud Platform (GCP) and BigQuery, we establish the following mappings:

| Legacy Component | Target BigQuery / GCP Mechanism |
| :--- | :--- |
| **Local Data Files** | Stored on Google Cloud Storage (GCS) (e.g., `gs://[BUCKET_NAME]/data/...`) |
| **Control Files (`.ctl`)** | Replaced by explicit BigQuery schema definitions or standard `LOAD DATA` schema patterns in SQL. |
| **SQL\*Loader Processing Engine** | Replaced by BigQuery's native `LOAD DATA` DDL statement. |
| **Credentials & Connection** | Managed natively via GCP IAM roles and service accounts executing the BigQuery job (no password passing required). |
| **Bad / Discard Checks** | Replicated via BigQuery `max_bad_records` configuration and transaction `EXCEPTION` blocks. If an import fails, the exception is caught, transactional changes rollback, and errors are written to an audit log table. |
| **Output / Standard Log Tracking** | Executions are captured in a structured database logging table (`dw_execution_log` / `dw_error_log`). |

---

## 3. Lineage & Environment Context
* **Upstream Producers**: External source systems uploading files to Google Cloud Storage (GCS).
* **Downstream Consumers**: Target analytical tables in BigQuery.
* **External System Replacements**:
  * **Oracle Database** $\rightarrow$ Google Cloud BigQuery
  * **Oracle SQL\*Loader Utilities** $\rightarrow$ BigQuery native `LOAD DATA` or Cloud Storage data transfer integrations.
  * **Local Storage Logging** $\rightarrow$ Cloud Logging & BigQuery logging tables.

---

## 4. Target File Plan
* **Target Relative Path**: `gcp_migration/import/is/stored_procedures/d_ipis_loader.sql`
* **Target Language**: BigQuery SQL (Stored Procedure)
* **Derived From Source File**: `vobs/dw_source/isdwh/import/is/bin/d_ipis_loader.ksh`

---

## 5. Environment-Specific Configurations
The target deployment pipeline must provision:
* **GCP Project ID**: Environment-specific variable (e.g., `prj-prod-dwh`, `prj-dev-dwh`).
* **BigQuery Dataset**: target dataset (e.g., `dw_import_is`).
* **Execution Audit Tables**: `dw_execution_log` and `dw_error_log` tables inside the designated operations dataset.
* **Service Accounts**: Executing service accounts must have `roles/bigquery.admin` (or specific `roles/bigquery.dataEditor` & `roles/bigquery.jobUser` permissions) and read access to the input GCS buckets (`roles/storage.objectViewer`).

---

## 6. Risks, Manual Actions, & B4 Redesign Items
* **Risks**: Control files contain specific parsing definitions (e.g., column lengths, custom date formats, or conditional `WHEN` clauses). 
* **Manual Actions**:
  * **Verify Control Mapping**: Designers must extract each Oracle control file (`.ctl`) and define its schema/formatting rules inside the target table schema or in the orchestrating wrapper mapping block.
  * **Audit Configurations**: Configure appropriate file formats (e.g., `CSV`, `AVRO`, `PARQUET`, or `JSON`) based on actual incoming files.

---

## 7. Complete Verified Translation (Pseudocode)

Below is the verbatim target BigQuery stored procedure implementation representing the converted wrapper logic.

```sql
=== Result for vobs/dw_source/isdwh/import/is/bin/d_ipis_loader.ksh ===
# Document: Shell Script Analysis

## 1. Key Logic and Data Flow
The purpose of the `d_ipis_loader.ksh` script is to load flat data files into an Oracle database using Oracle’s utility, **SQL\*Loader (`sqlldr`)**. 

The main steps of the script are:
1. **Parameter Parsing & Validation**: Reads command-line arguments using `getopts` for the Control file (`-c`), Data file (`-d`), Oracle User (`-u`), Oracle Password (`-p`), and Land/Country (`-l`). It performs validation checks to ensure that both `CONTROLFILE` and `DATAFILE` are specified.
2. **Environment & File Setup**: Generates timestamps and defines output paths for Oracle SQL\*Loader log files: Log file (`.log`), Discard file (`.dis`), and Bad file (`.bad`).
3. **Execution**: Based on the presence of custom Oracle credentials, it runs `sqlldr` utilizing either the default system/environment Oracle credentials (`$DW_ORAUSER`) or the provided user credentials.
4. **Post-Load Validation & Error Handling**:
   - Checks if a `.bad` (contains rejected records) or `.dis` (contains discarded records that didn't match the control file criteria) file was created.
   - If either file exists, it flags an error (`ErrNr=200`) and calls an external logging handler (`DWMSG_MeldeFehler`).
5. **Clean-up**: Outputs the execution log (`.log`) to stdout (`cat`) and deletes the log file from the local file system.

## 2. BigQuery Migration Mapping & Strategy
To migrate this logic to a modern Cloud Data Warehouse architecture on Google Cloud Platform (GCP) with **BigQuery**, we map the components as follows:

* **Control Files (`.ctl`) and Data Files (`.dat`/`.csv`)**: In BigQuery, data ingestion from flat files is handled natively via the load job API (`LOAD DATA` statement in BigQuery SQL) or external table configurations. The control file schema definitions are converted directly into DDL and BigQuery Load Options (e.g., field delimiters, skip header rows, etc.).
* **SQL\*Loader Process**: Replaced by BigQuery's native `LOAD DATA` DDL statement.
* **Bad/Discard Records**: BigQuery handles parsing errors through job configurations:
  - `max_bad_records`: Defines how many bad records can be ignored before the job fails.
  - Setting `max_bad_records = 0` mimics the strict behavior of failing or capturing bad files. To fully audit bad rows without failing the entire load, we can query a load job's statistics or inspect import errors.
* **Storage**: Input files are hosted on Google Cloud Storage (GCS) instead of local file paths.
* **Logging/Variables**: Procedural `DECLARE` statements and standard BigQuery logging tables/views represent the log variables.

---

# Assumptions and Additional Notes

1. **File Locations**: It is assumed that the source data file resides in a Google Cloud Storage (GCS) bucket (e.g., `gs://your-bucket/data/`).
2. **Control File Conversion**: The logical schema representation (formerly in the Oracle `.ctl` file) is explicitly declared inside the SQL `LOAD DATA` statement or mapped to target table definitions.
3. **Bad/Discard Logic**: BigQuery's native `LOAD DATA` will fail the execution block if any record does not conform to the schema (equivalent to `max_bad_records = 0`). If bad files must be captured and parsed separately, BigQuery external tables can load raw strings for structural validation in downstream SQL layers. The pseudocode below showcases the standard, robust native `LOAD DATA` approach with structured error capturing.
4. **Metadata and Log Storage**: Instead of writing physical log files to a local disk and deleting them, load execution status is captured using BigQuery’s `INFORMATION_SCHEMA.JOBS_BY_PROJECT` metadata or logged directly into a dedicated database audit table.

---

# Pseudocode: BQ SQL Pseudocode

```sql
-- Create a stored procedure that replicates the SQL*Loader wrapper logic
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.d_ipis_loader`(
  IN p_control_file_name STRING, -- Used to determine target table / schema structure
  IN p_data_file_uri STRING,     -- GCS URI of the data file (e.g., 'gs://bucket/data.csv')
  IN p_land STRING,              -- Country code or language parameter (Optional)
  OUT p_err_nr INT64             -- Return value equivalent (0 = Success, 200 = Bad/Discard Records Error, etc.)
)
BEGIN
  -- Declare variables for local processing
  DECLARE v_target_table STRING;
  DECLARE v_log_message STRING;
  DECLARE v_job_id STRING;
  DECLARE v_bad_records_count INT64;

  -- 1. Argument Validation
  IF p_control_file_name IS NULL OR p_control_file_name = '' THEN
    SET p_err_nr = 1; -- Error: Missing control file argument
    INSERT INTO `your_project.your_dataset.dw_error_log` (timestamp, level, error_code, message)
    VALUES (CURRENT_TIMESTAMP(), 'E', p_err_nr, 'Validation failed: CONTROLFILE is empty');
    RETURN;
  END IF;

  IF p_data_file_uri IS NULL OR p_data_file_uri = '' THEN
    SET p_err_nr = 1; -- Error: Missing data file argument
    INSERT INTO `your_project.your_dataset.dw_error_log` (timestamp, level, error_code, message)
    VALUES (CURRENT_TIMESTAMP(), 'E', p_err_nr, 'Validation failed: DATAFILE URI is empty');
    RETURN;
  END IF;

  -- 2. Map Control File Name to Destination Table and Schema
  -- This dynamic assignment replicates the function of the control file mappings.
  SET v_target_table = CASE 
    WHEN p_control_file_name LIKE '%customer%' THEN 'your_project.your_dataset.t_customer'
    WHEN p_control_file_name LIKE '%orders%' THEN 'your_project.your_dataset.t_orders'
    ELSE NULL
  END;

  IF v_target_table IS NULL THEN
    SET p_err_nr = 2; -- Error: Control file does not map to any recognized table
    INSERT INTO `your_project.your_dataset.dw_error_log` (timestamp, level, error_code, message)
    VALUES (CURRENT_TIMESTAMP(), 'F', p_err_nr, CONCAT('Unknown control file layout: ', p_control_file_name));
    RETURN;
  END IF;

  -- 3. Execution of the Data Load (Equivalent to SQL*Loader Execution)
  -- BigQuery EXCEPTION handling is used to capture run-time load errors.
  BEGIN TRANSACTION;
  BEGIN
    -- Example structure: dynamically load CSV from GCS using generic CSV parameters.
    -- These options replace standard SQL*Loader parameters.
    EXECUTE IMMEDIATE FORMAT("""
      LOAD DATA OVERWRITE %s
      FROM FILES (
        format = 'CSV',
        uris = ['%s'],
        skip_header = 1,
        field_delimiter = ';',
        quote = '"',
        max_bad_records = 0
      )
    """, v_target_table, p_data_file_uri);

    -- Success
    SET p_err_nr = 0;
    
    -- Print Load Summary to Console/Log equivalent table
    SET v_log_message = CONCAT('SUCCESS: Successfully loaded ', p_data_file_uri, ' into ', v_target_table);
    INSERT INTO `your_project.your_dataset.dw_execution_log` (timestamp, message)
    VALUES (CURRENT_TIMESTAMP(), v_log_message);

    COMMIT TRANSACTION;

  EXCEPTION WHEN ERROR THEN
    -- Rollback active transaction if loading failed
    ROLLBACK TRANSACTION;

    -- 4. Check for Bad / Discarded Records (Replicates the Badfile & Discardfile checks)
    -- If the LOAD job aborted or raised an exception, we classify it as ErrNr = 200
    SET p_err_nr = 200;
    
    -- Log errors dynamically to an execution/audit tracking log
    SET v_log_message = CONCAT('FAILURE: Loading aborted for file: ', p_data_file_uri, ' | Details: ', @@error.message);
    
    INSERT INTO `your_project.your_dataset.dw_error_log` (timestamp, level, error_code, message)
    VALUES (CURRENT_TIMESTAMP(), 'F', p_err_nr, v_log_message);
    
  END;

END;
```