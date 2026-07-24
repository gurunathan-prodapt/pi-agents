# Migration Validation Test Suite: `DW.RPOS_CARM_IMPORT`

This document defines the comprehensive migration-validation test suite for the migrated job `DW.RPOS_CARM_IMPORT`. These tests are designed to prove behavioral equivalence between the legacy Ab Initio/KornShell/Oracle implementation and the modernized Apache Airflow/Dataproc Serverless/BigQuery implementation.

---

## Test Case 1: End-to-End Output Parity (Happy Path)

### Purpose
Verify that processing a standard, valid transaction file produces identical outputs in all five target tables and the two operational audit tables compared to the legacy execution.

### Setup
1. Populate the BigQuery master contract table `dwh_ta_c_vertrag` with active contract records.
2. Clear any existing records in the target tables:
   * `dwh_ta_f_rpos_carm`
   * `dwh_ta_f_rpos_fact_carm`
   * `dwh_ta_f_gpos_fact_carm`
   * `dwh_ta_f_rpos_reselling_carm`
   * `dwh_ta_t_rpos_carm`
3. Insert a seed record into `dwh_ta_k_meldungen` with `entrynr = 99999`.
4. Upload a valid mock transaction file named `CARMEN_B_20260421_pos.fix` to `gs://{GCS_BUCKET}/crs/work/` containing:
   * One Header record (`H`)
   * Four Payload records (`P`) representing each business stream (Factoring Invoice, Factoring Credit, Reselling, Temporary)
   * One Trailer record (`X`)

### Action
1. Set the environment variable `BHB_Eintragsnr=99999`.
2. Execute the PySpark migration script `map_rpos_carmen_import.py`.

### Pass/Fail Criterion
* **Pass**: 
  * Target tables contain the exact expected row counts and column values.
  * `dwh_ta_k_meldungen` is updated with correct trailer metrics.
  * `dwh_ta_k_rech_absgrp` contains the newly upserted run record.
* **Fail**: Any row count mismatch, schema mismatch, or incorrect column value.

### Test Code (Pytest & PySpark)

```python
import pytest
from pyspark.sql import SparkSession
import os

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("test_rpos_carm_import_parity") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def test_happy_path_parity(spark):
    gcp_project = os.environ.get("GCP_PROJECT")
    bq_dataset = os.environ.get("BQ_DATASET", "bq_dataset")
    
    # Assert row counts in target tables
    df_rpos_carm = spark.read.format("bigquery").option("table", f"{gcp_project}.{bq_dataset}.dwh_ta_f_rpos_carm").load()
    df_fact_f = spark.read.format("bigquery").option("table", f"{gcp_project}.{bq_dataset}.dwh_ta_f_rpos_fact_carm").load()
    df_fact_g = spark.read.format("bigquery").option("table", f"{gcp_project}.{bq_dataset}.dwh_ta_f_gpos_fact_carm").load()
    df_reselling = spark.read.format("bigquery").option("table", f"{gcp_project}.{bq_dataset}.dwh_ta_f_rpos_reselling_carm").load()
    df_temp = spark.read.format("bigquery").option("table", f"{gcp_project}.{bq_dataset}.dwh_ta_t_rpos_carm").load()
    
    # Assertions based on 4 payload records
    assert df_rpos_carm.count() == 4, f"Expected 4 records in general table, got {df_rpos_carm.count()}"
    assert df_fact_f.count() == 1, f"Expected 1 record in Factoring Invoices, got {df_fact_f.count()}"
    assert df_fact_g.count() == 1, f"Expected 1 record in Factoring Credits, got {df_fact_g.count()}"
    assert df_reselling.count() == 1, f"Expected 1 record in Reselling, got {df_reselling.count()}"
    assert df_temp.count() == 1, f"Expected 1 record in Temporary, got {df_temp.count()}"
    
    # Verify specific column transformations
    fact_f_row = df_fact_f.filter(df_fact_f.rechnung_id == "REC_FACT_F").first()
    assert fact_f_row["rech_leistung_id_carm"] == "RABATT_SH", "Expected truncated 9-character item code"
    
    temp_row = df_temp.first()
    assert temp_row["bearbeitung_datum"].strftime("%Y-%m-%d %H:%M:%S") == "1900-01-01 00:00:00", "Expected default epoch timestamp"
```

---

## Test Case 2: Validation Rules & Crash Strategy

### Purpose
Verify that invalid formats in key fields (`monats_id`, `rechnung_datum`, `standardvertrags_id`, `vertrags_id`, `rechpos_brutto_eur`, `rechpos_netto_eur`, `rechpos_mwst_eur`) trigger hard errors/exceptions verbatim to the legacy `force_error` behavior.

### Setup
1. Create five distinct invalid mock files, each containing one specific validation error:
   * File A: Invalid `monats_id` format (e.g., `20261` instead of `202604`).
   * File B: Invalid `rechnung_datum` format (e.g., `2026043` instead of `20260421`).
   * File C: Invalid non-numeric `standardvertrags_id` (e.g., `ABC` instead of a number or `#`).
   * File D: Invalid non-numeric `rechpos_brutto_eur` (e.g., `12O.50` containing letter 'O').
2. Upload these files sequentially to the GCS staging directory.

### Action
1. Execute the PySpark job for each invalid file.
2. Capture the execution logs and exceptions.

### Pass/Fail Criterion
* **Pass**: The PySpark job crashes immediately with the exact verbatim error message specified in the legacy design document (e.g., `"Invalid data format in monats_id"`).
* **Fail**: The job completes successfully, ignores the invalid record, or throws a generic/unhandled exception.

### Test Code (Pytest)

```python
import pytest
from pyspark.sql.utils import AnalysisException

def test_validation_failures(spark):
    # Test Case A: Invalid monats_id
    invalid_monats_data = [("P;20261;12345;REC01;20260421;999;888;RABATT;100.00;84.00;16.00;N;999;888;P;1;1;F;N;999")]
    df_raw = spark.createDataFrame(invalid_monats_data, "string").toDF("value")
    
    # Simulate the parsing and validation logic
    df_split = df_raw.select(
        # ... parsing logic ...
    )
    
    with pytest.raises(Exception) as excinfo:
        # Trigger action that evaluates the validation rules
        df_validated = df_raw.select(
            # ... validation expression containing raise_error ...
        )
        df_validated.collect()
        
    assert "Invalid data format in monats_id" in str(excinfo.value)
```

---

## Test Case 3: Contract History Deduplication & Proof Join (Temporal Logic)

### Purpose
Verify that:
1. The contract master ledger deduplication correctly selects the most recent contract version (`rankindex = 1` ordered by `gueltig_von DESC`, `dwh_vertrag_id DESC`).
2. The `Proof Join` temporal validation correctly nullifies contract fields if the transaction's month-end date falls outside the contract's active range.

### Setup
1. Populate `dwh_ta_c_vertrag` with two versions of contract `12345`:
   * Version 1: `dwh_vertrag_id = 1001`, `gueltig_von = 2026-01-01`, `gueltig_bis = 2026-06-30`, `rahmenvertrag_id = RV_OLD`
   * Version 2: `dwh_vertrag_id = 1002`, `gueltig_von = 2026-03-01`, `gueltig_bis = 2026-12-31`, `rahmenvertrag_id = RV_NEW`
2. Create two payload records:
   * Record A: `vertrags_id = 12345`, `monats_id = 202605` (Month-end = `2026-05-31`). This falls inside Version 2's range.
   * Record B: `vertrags_id = 12345`, `monats_id = 202602` (Month-end = `2026-02-28`). This falls outside Version 2's range.

### Action
1. Run the PySpark transformation pipeline.
2. Inspect the joined and proofed DataFrame.

### Pass/Fail Criterion
* **Pass**:
  * Record A successfully joins with Version 2 and retains `rahmenvertrag_id = RV_NEW`.
  * Record B joins with Version 2 (as it is the highest ranked), but because its month-end (`2026-02-28`) is prior to `gueltig_von` (`2026-03-01`), the contract fields are nullified (`rahmenvertrag_id_checked = NULL`).
* **Fail**: Record A joins with Version 1, or Record B retains contract details despite failing the temporal range check.

### Test Code (PySpark Assertion)

```python
def test_contract_dedup_and_proof_join(spark):
    # Mock Contract Master Ledger
    contracts_data = [
        ("RV_OLD", "12345", 1001, "2026-01-01", "2026-06-30"),
        ("RV_NEW", "12345", 1002, "2026-03-01", "2026-12-31")
    ]
    df_contracts = spark.createDataFrame(contracts_data, ["rahmenvertrag_id", "vertrag_id_carmen", "dwh_vertrag_id", "gueltig_von", "gueltig_bis"])
    
    # Mock Payload
    payload_data = [
        ("202605", "12345", "REC_A"),  # Month-end: 2026-05-31 (Valid for RV_NEW)
        ("202602", "12345", "REC_B")   # Month-end: 2026-02-28 (Invalid for RV_NEW)
    ]
    df_payload = spark.createDataFrame(payload_data, ["monats_id", "vertrags_id", "rechnung_id"])
    
    # 1. Deduplicate Contracts
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F
    
    window_spec = Window.partitionBy("vertrag_id_carmen").orderBy(F.col("gueltig_von").desc(), F.col("dwh_vertrag_id").desc())
    df_contract_dedup = df_contracts \
        .withColumn("rankindex", F.row_number().over(window_spec)) \
        .filter(F.col("rankindex") == 1)
        
    # 2. Join and Proof
    df_joined = df_payload.join(df_contract_dedup, on=df_payload.vertrags_id == df_contract_dedup.vertrag_id_carmen, how="left")
    
    month_last_day = F.last_day(F.to_date(F.col("monats_id"), "yyyyMM"))
    valid_flag_expr = F.when(
        (F.col("gueltig_von").isNull() | (month_last_day > F.col("gueltig_von"))) &
        (F.col("gueltig_bis").isNull() | (month_last_day <= F.col("gueltig_bis"))),
        0
    ).otherwise(1)
    
    df_proofed = df_joined.withColumn(
        "rahmenvertrag_id_checked",
        F.when(valid_flag_expr == 0, F.col("rahmenvertrag_id")).otherwise(F.lit(None))
    )
    
    # Assertions
    row_a = df_proofed.filter(F.col("rechnung_id") == "REC_A").first()
    row_b = df_proofed.filter(F.col("rechnung_id") == "REC_B").first()
    
    assert row_a["rahmenvertrag_id_checked"] == "RV_NEW", "Record A should have joined with RV_NEW"
    assert row_b["rahmenvertrag_id_checked"] is None, "Record B contract fields should be nullified"
```

---

## Test Case 4: Business Stream Routing & Field Transformations

### Purpose
Verify that records are routed to their correct target tables based on `rpos_geschaftsform_kenn` and `typ`, and that specific field transformations (truncation of `rech_leistung_id_carm` and default epoch timestamping) are applied correctly.

### Setup
1. Create a mock payload DataFrame containing:
   * Record 1: `rpos_geschaftsform_kenn = 'F'`, `rech_leistung_id_carm = 'RABATT_LONG_NAME'`
   * Record 2: `rpos_geschaftsform_kenn = 'G'`, `rech_leistung_id_carm = 'CREDIT_LONG_NAME'`
   * Record 3: `rpos_geschaftsform_kenn = 'R'`, `rech_leistung_id_carm = 'RESELL_LONG_NAME'`
   * Record 4: `typ = 'T'`, `rech_leistung_id_carm = 'TEMP_ITEM'`

### Action
1. Run the PySpark routing and transformation logic.
2. Inspect the resulting DataFrames for each target stream.

### Pass/Fail Criterion
* **Pass**:
  * Record 1 is routed to `dwh_ta_f_rpos_fact_carm` with `rech_leistung_id_carm` truncated to `"RABATT_LO"` (9 characters).
  * Record 2 is routed to `dwh_ta_f_gpos_fact_carm` with `rech_leistung_id_carm` truncated to `"CREDIT_LO"` (9 characters).
  * Record 3 is routed to `dwh_ta_f_rpos_reselling_carm` with `rech_leistung_id_carm` truncated to `"RESELL_LO"` (9 characters).
  * Record 4 is routed to `dwh_ta_t_rpos_carm` with `bearbeitung_datum` set to `1900-01-01 00:00:00`.
  * All records are routed to `dwh_ta_f_rpos_carm` with their original untruncated values.
* **Fail**: Any record is routed to the wrong table, or field transformations are incorrect.

### Test Code (PySpark Assertion)

```python
def test_business_stream_routing(spark):
    from pyspark.sql import functions as F
    
    # Mock input data
    data = [
        ("F", "RABATT_LONG_NAME", "P"),
        ("G", "CREDIT_LONG_NAME", "P"),
        ("R", "RESELL_LONG_NAME", "P"),
        ("X", "TEMP_ITEM", "T")
    ]
    df = spark.createDataFrame(data, ["rpos_geschaftsform_kenn", "rech_leistung_id_carm", "typ"])
    
    # Stream Factoring Invoices (F)
    df_fact_f = df.filter(F.col("rpos_geschaftsform_kenn") == "F").select(
        F.substring(F.col("rech_leistung_id_carm"), 1, 9).alias("rech_leistung_id_carm")
    )
    
    # Stream Temporary (T)
    df_temp = df.filter(F.col("typ") == "T").select(
        F.to_timestamp(F.lit("19000101000000"), "yyyyMMddHHmmss").alias("bearbeitung_datum")
    )
    
    # Assertions
    assert df_fact_f.first()["rech_leistung_id_carm"] == "RABATT_LO"
    assert df_temp.first()["bearbeitung_datum"].strftime("%Y-%m-%d %H:%M:%S") == "1900-01-01 00:00:00"
```

---

## Test Case 5: Idempotency (Paired Reload / Left-Anti-Join)

### Purpose
Verify that the target table reload logic correctly deletes existing records matching the incoming keys `(rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm)` before inserting the new batch, preventing duplicates.

### Setup
1. Populate the target BigQuery table `dwh_ta_f_rpos_carm` with:
   * Record A (Overlapping): `rechnung_id = 'REC01'`, `rechnung_datum = '2026-04-21'`, `standardvertrags_id = 999`, `vertrags_id = 888`, `rech_leistung_id_carm = 'RABATT'`, `rechpos_brutto_eur = 100.00`
   * Record B (Non-overlapping): `rechnung_id = 'REC02'`, `rechnung_datum = '2026-04-21'`, `standardvertrags_id = 999`, `vertrags_id = 888`, `rech_leistung_id_carm = 'RABATT'`, `rechpos_brutto_eur = 200.00`
2. Prepare an incoming batch containing a new version of Record A:
   * Record A_new: `rechnung_id = 'REC01'`, `rechnung_datum = '2026-04-21'`, `standardvertrags_id = 999`, `vertrags_id = 888`, `rech_leistung_id_carm = 'RABATT'`, `rechpos_brutto_eur = 150.00` (updated amount)

### Action
1. Execute the `save_with_paired_reload` function on the incoming batch.
2. Read the final target table.

### Pass/Fail Criterion
* **Pass**:
  * The target table contains exactly two records (Record B and Record A_new).
  * Record A_new has the updated amount (`150.00`).
  * No duplicate records exist for key `REC01`.
* **Fail**: Record A is not updated, duplicates exist, or Record B is accidentally deleted.

### Test Code (PySpark Assertion)

```python
def test_paired_reload_idempotency(spark):
    # Mock existing target table
    existing_data = [
        ("REC01", "2026-04-21", 999, 888, "RABATT", 100.00),
        ("REC02", "2026-04-21", 999, 888, "RABATT", 200.00)
    ]
    df_target_exist = spark.createDataFrame(existing_data, ["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm", "rechpos_brutto_eur"])
    
    # Mock incoming batch
    incoming_data = [
        ("REC01", "2026-04-21", 999, 888, "RABATT", 150.00)
    ]
    df_new = spark.createDataFrame(incoming_data, ["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm", "rechpos_brutto_eur"])
    
    # Execute Left Anti Join Deletion Simulation
    df_keys_to_delete = df_new.select("rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm").distinct()
    df_cleared = df_target_exist.join(
        df_keys_to_delete,
        on=["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm"],
        how="leftanti"
    )
    df_final = df_cleared.unionByName(df_new, allowMissingColumns=True)
    
    # Assertions
    assert df_final.count() == 2, f"Expected 2 records, got {df_final.count()}"
    rec01_row = df_final.filter(df_final.rechnung_id == "REC01").first()
    assert rec01_row["rechpos_brutto_eur"] == 150.00, "Expected updated amount of 150.00"
```

---

## Test Case 6: Audit & Control Table Updates

### Purpose
Verify that the trailer record (`X`) is parsed correctly, updating `dwh_ta_k_meldungen` and upserting `dwh_ta_k_rech_absgrp` with correct business logic (1-month back-shift of `monats_id` and extraction of `abs_grp`).

### Setup
1. Create a mock trailer record:
   * `X;CARMEN_B_20260421_pos.fix;20260515;150;SUCCESS;20260515120000;`
2. Seed `dwh_ta_k_meldungen` with a record matching `entrynr = 12345`.

### Action
1. Run the trailer parsing and BigQuery update logic.
2. Query the audit tables in BigQuery.

### Pass/Fail Criterion
* **Pass**:
  * `dwh_ta_k_meldungen` is updated with `anzahl_ds_eof = 150`, `dateiname = 'CARMEN_B_20260421_pos.fix'`, and `enderecord_text = 'SUCCESS'`.
  * `dwh_ta_k_rech_absgrp` is upserted with:
    * `monats_id = '202604'` (one month back from `stichtag` month `202605`).
    * `abs_grp = '20260'` (substring of `dateiname` from index 10, length 5).
    * `rechnung_datum = '2026-05-15'`.
* **Fail**: Audit tables are not updated, or values are calculated incorrectly (e.g., incorrect month shift).

### Test Code (BigQuery SQL Assertions)

```sql
-- Assertion 1: Verify dwh_ta_k_meldungen Update
SELECT 
  anzahl_ds_eof, 
  dateiname, 
  enderecord_text 
FROM 
  `bq_dataset.ta_k_meldungen` 
WHERE 
  entrynr = 12345;

-- EXPECTED OUTPUT:
-- anzahl_ds_eof: 150
-- dateiname: 'CARMEN_B_20260421_pos.fix'
-- enderecord_text: 'SUCCESS'


-- Assertion 2: Verify dwh_ta_k_rech_absgrp Upsert
SELECT 
  monats_id, 
  abs_grp, 
  rechnung_datum, 
  rechnungsteil 
FROM 
  `bq_dataset.ta_k_rech_absgrp` 
WHERE 
  dateiname = 'CARMEN_B_20260421_pos.fix';

-- EXPECTED OUTPUT:
-- monats_id: '202604'
-- abs_grp: '20260'
-- rechnung_datum: '2026-05-15'
-- rechnungsteil: 'P'
```

---

## Test Case 7: GCS File Discovery & Wrapper Orchestration

### Purpose
Verify that the Python wrapper script (`map_rpos_carmen_import.py`) correctly discovers files matching the wildcard mask `CARMEN_B_*_pos.fix` in GCS, submits the Dataproc Serverless job with correct arguments, and handles EME project path errors gracefully.

### Setup
1. Upload a file named `CARMEN_B_20260421_pos.fix` to `gs://{GCS_BUCKET}/crs/work/`.
2. Set environment variables:
   * `GCP_PROJECT`, `DATAPROC_REGION`, `GCS_BUCKET`
   * `BHB_Dateimaske = CARMEN_B_*_pos.fix`
   * `AB_GRAPH_SCRIPT_REPOSIT_TRACKING = true` (to trigger the EME check simulation)

### Action
1. Execute the Python wrapper script.

### Pass/Fail Criterion
* **Pass**:
  * The script successfully identifies `CARMEN_B_20260421_pos.fix` as the active file.
  * The script throws the expected EME error: `"Error: cannot determine path to project in EME Datastore; exiting"` when executing with reposit tracking enabled without the `air` CLI.
  * When reposit tracking is disabled, the script successfully triggers the Dataproc Serverless PySpark job with the correct arguments.
* **Fail**: The script fails to find the file, crashes on standard environment checks, or fails to submit the Dataproc job.

### Test Code (Pytest Subprocess Execution)

```python
import subprocess
import os
import pytest

def test_wrapper_gcs_discovery_and_eme_error():
    # Set up environment to trigger the EME check error
    env = os.environ.copy()
    env["AB_GRAPH_SCRIPT_REPOSIT_TRACKING"] = "true"
    env["PROJECT_DIR"] = "/tmp/mock_project"
    
    # Run the wrapper script
    result = subprocess.run(
        ["python3", "abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py"],
        env=env,
        capture_output=True,
        text=True
    )
    
    # Assert that the script exited with code 1 and printed the verbatim EME error message
    assert result.returncode == 1
    assert "Error: cannot determine path to project in EME Datastore; exiting" in result.stdout
```