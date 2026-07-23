# Migration Validation Test Suite: DW.RPOS_CARM_IMPORT

This document defines the comprehensive migration-validation test suite for the migrated job `DW.RPOS_CARM_IMPORT`. These tests are designed to prove behavioral equivalence between the legacy Ab Initio/KornShell/Oracle implementation and the migrated Cloud Composer/Dataproc Serverless (PySpark)/BigQuery implementation.

---

## Test Suite Overview

The validation strategy is divided into three layers:
1. **Unit & Transformation Validation (PySpark / Local Spark)**: Validates parsing, temporal joins, ranking, routing, and aggregation logic using a local Spark session.
2. **Integration & Idempotency Validation (BigQuery SQL)**: Validates pre-delete operations, target table updates, and audit log updates directly on BigQuery.
3. **System & Orchestration Validation (Airflow / GCS)**: Validates file sensing, parameter resolution, and archiving.

---

## Test Case 1: End-to-End Output Parity & Routing Validation

### Purpose
Verify that a standard input file containing all transaction types (`F`, `G`, `R`, `S`, and `T` candidates) is parsed, validated, and routed to the correct target BigQuery tables with exact field-level parity.

### Setup
1. **Reference Data**: Populate `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_c_vertrag` with active contract records:
   ```sql
   INSERT INTO `your_project.dw_dataset.dwh_ta_c_vertrag` 
   (rahmenvertrag_id, vertrag_id_carmen, dwh_vertrag_id, dwh_gp_id, dwh_konto_id, dwh_tarifgr_id, vo_kenn, zv_id, gueltig_von, gueltig_bis)
   VALUES 
   ('RV_100', 12345, 999001, 888001, 777001, 555, 'VO01', 'ZV01', DATE('2005-01-01'), DATE('2030-12-31'));
   ```
2. **Input File**: Create a mock input file `CARMEN_B_TEST_pos.fix` in `gs://{GCS_STAGING_BUCKET}/crs/work/` containing:
   * Header record (`H`)
   * Payload records (`P`) representing different routing paths:
     * Row 1: Factoring Invoice (`rpos_geschaftsform_kenn = 'F'`, `vas_kenn = 'P11111'`) -> Routes to `dwh_ta_f_rpos_fact_carm`
     * Row 2: Factoring Gutschrift (`rpos_geschaftsform_kenn = 'F'`, `vas_kenn = 'P30002'`) -> Decodes to `G` -> Routes to `dwh_ta_f_gpos_fact_carm`
     * Row 3: Reselling (`rpos_geschaftsform_kenn = 'R'`) -> Routes to `dwh_ta_f_rpos_reselling_carm`
     * Row 4: Standard Position (`rpos_geschaftsform_kenn = 'S'`) -> Routes to `dwh_ta_f_rpos_carm`
     * Row 5: Temporary Position (`rpos_geschaftsform_kenn = 'S'`, `rech_leistung_id_carm = 'RABATT'`, `vertrags_id = '#'`/0) -> Routes to `dwh_ta_t_rpos_carm`
   * Trailer record (`X`)

### Action
Execute the Dataproc Serverless PySpark job with the test file:
```bash
gcloud dataproc batches submit pyspark \
    gs://{GCS_STAGING_BUCKET}/abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py \
    --project={GCP_PROJECT} \
    --region={GCP_REGION} \
    --jars=gs://{GCS_STAGING_BUCKET}/bin/spark-bigquery-latest_2.12.jar \
    --env-vars="GCP_PROJECT={GCP_PROJECT},GCS_STAGING_BUCKET={GCS_STAGING_BUCKET},BHB_Dateiname=CARMEN_B_TEST_pos.fix,BHB_Eintragsnr=1001,BQ_DATASET={BQ_DATASET}"
```

### Pass/Fail Criterion
Run the following BigQuery validation query. All assertions must return `TRUE`.

```sql
WITH p1_fact AS (
  SELECT COUNT(1) as cnt FROM `your_project.dw_dataset.dwh_ta_f_rpos_fact_carm` WHERE rechnung_id = 'REC001'
),
p1_gut AS (
  SELECT COUNT(1) as cnt FROM `your_project.dw_dataset.dwh_ta_f_gpos_fact_carm` WHERE rechnung_id = 'REC002'
),
p1_resell AS (
  SELECT COUNT(1) as cnt FROM `your_project.dw_dataset.dwh_ta_f_rpos_reselling_carm` WHERE rechnung_id = 'REC003'
),
p2_std AS (
  SELECT COUNT(1) as cnt FROM `your_project.dw_dataset.dwh_ta_f_rpos_carm` WHERE rechnung_id = 'REC004'
),
p2_temp AS (
  SELECT COUNT(1) as cnt FROM `your_project.dw_dataset.dwh_ta_t_rpos_carm` WHERE rechnung_id = 'REC005'
)
SELECT 
  (p1_fact.cnt = 1) AS fact_invoice_routed_correctly,
  (p1_gut.cnt = 1) AS fact_credit_decoded_and_routed_correctly,
  (p1_resell.cnt = 1) AS reselling_routed_correctly,
  (p2_std.cnt = 1) AS standard_routed_correctly,
  (p2_temp.cnt = 1) AS temporary_routed_correctly
FROM p1_fact, p1_gut, p1_resell, p2_std, p2_temp;
```

---

## Test Case 2: Temporal Contract Join & Nullification

### Purpose
Verify that the left outer join with `dwh_ta_c_vertrag` correctly evaluates temporal validity: if the last day of the transaction month (`month_last_day`) falls outside the contract's `gueltig_von` and `gueltig_bis` range, the contract reference fields are nullified.

### Setup
1. **Reference Data**: Populate `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_c_vertrag` with a contract that has expired:
   ```sql
   INSERT INTO `your_project.dw_dataset.dwh_ta_c_vertrag` 
   (rahmenvertrag_id, vertrag_id_carmen, dwh_vertrag_id, dwh_gp_id, dwh_konto_id, dwh_tarifgr_id, vo_kenn, zv_id, gueltig_von, gueltig_bis)
   VALUES 
   ('RV_EXPIRED', 99999, 999002, 888002, 777002, 555, 'VO02', 'ZV02', DATE('2005-01-01'), DATE('2005-03-31'));
   ```
2. **Input File**: Create a mock transaction record with `monats_id = '200504'` (April 2005). The last day of this month is `2005-04-30`, which is strictly greater than the contract's `gueltig_bis` (`2005-03-31`).

### Action
Execute the PySpark transformation logic. Below is a runnable `pytest` unit test that validates this specific transformation step.

```python
import pytest
from datetime import datetime
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test_temporal_join") \
        .getOrCreate()

def test_temporal_join_nullification(spark):
    # 1. Create Mock Transaction Data (monats_id = 200504 -> Last Day = 2005-04-30)
    tx_data = [(
        "200504", "DEB001", "REC001", datetime.strptime("20050415", "%Y%m%d"),
        Decimal("0.0"), Decimal("99999.0"), "LEISTUNG1", 
        Decimal("100.0"), Decimal("80.0"), Decimal("20.0"), "S", "P1", "POOL1", Decimal("0.0")
    )]
    tx_schema = [
        "monats_id_str", "debitor_id", "rechnung_id", "rechnung_datum",
        "standardvertrags_id", "vertrags_id", "rech_leistung_id_carm",
        "rechpos_brutto_eur", "rechpos_netto_eur", "rechpos_mwst_eur",
        "rpos_geschaftsform_kenn", "vas_kenn", "pooling", "rechnungvertrag_id"
    ]
    tx_df = spark.createDataFrame(tx_data, tx_schema) \
        .withColumn("monats_id", F.to_date(F.col("monats_id_str"), "yyyyMM"))

    # 2. Create Mock Contract Reference Data (Expired on 2005-03-31)
    contract_data = [(
        "RV_EXPIRED", 99999.0, 999002, 888002, 777002, 555, "VO02", "ZV02",
        datetime.strptime("20050101", "%Y%m%d"), datetime.strptime("20050331", "%Y%m%d")
    )]
    contract_schema = [
        "rahmenvertrag_id", "vertrag_id_carmen", "dwh_vertrag_id", "dwh_gp_id",
        "dwh_konto_id", "dwh_tarifgr_id", "vo_kenn", "zv_id", "gueltig_von", "gueltig_bis"
    ]
    contract_df = spark.createDataFrame(contract_data, contract_schema)

    # 3. Perform Left Join
    joined_df = tx_df.join(contract_df, tx_df.vertrags_id == contract_df.vertrag_id_carmen, "left_outer")

    # 4. Apply Temporal Validation Logic
    last_day_monats = F.last_day(F.col("monats_id"))
    valid_flag_col = F.when(
        (F.col("gueltig_von").isNull() | (last_day_monats > F.col("gueltig_von"))) &
        (F.col("gueltig_bis").isNull() | (last_day_monats <= F.col("gueltig_bis"))),
        0
    ).otherwise(1)

    processed_df = joined_df.withColumn("valid_flag", valid_flag_col)
    
    # Nullify contract fields if valid_flag != 0
    final_df = processed_df.select(
        F.col("rechnung_id"),
        F.when(F.col("valid_flag") == 0, F.col("rahmenvertrag_id")).otherwise(F.lit(None)).alias("rahmenvertrag_id"),
        F.when(F.col("valid_flag") == 0, F.col("dwh_vertrag_id")).otherwise(F.lit(None)).alias("dwh_vertrag_id")
    )

    result = final_df.collect()[0]
    
    # Assertions: valid_flag should be 1 (invalid) and contract fields must be nullified
    assert processed_df.collect()[0]["valid_flag"] == 1
    assert result["rahmenvertrag_id"] is None
    assert result["dwh_vertrag_id"] is None
```

### Pass/Fail Criterion
The test passes if the `valid_flag` evaluates to `1` and the resolved `rahmenvertrag_id` and `dwh_vertrag_id` are nullified.

---

## Test Case 3: Deduplication & Ranking (Scan_Ranking)

### Purpose
Verify that when duplicate contract versions exist for the same `vertrags_id` and `monats_id`, the ranking window correctly selects the latest version based on `gueltig_von DESC` and `dwh_vertrag_id DESC`.

### Setup
1. **Reference Data**: Populate `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_c_vertrag` with two overlapping versions of the same contract:
   * Version 1: `dwh_vertrag_id = 100001`, `gueltig_von = '2005-01-01'`
   * Version 2: `dwh_vertrag_id = 100002`, `gueltig_von = '2005-02-01'` (Latest)
2. **Input File**: Create a transaction record with `vertrags_id = 55555` and `monats_id = '200503'`.

### Action
Execute the PySpark ranking logic. Below is a runnable `pytest` unit test validating the window ranking behavior.

```python
def test_scan_ranking_deduplication(spark):
    # 1. Create Mock Transaction Data
    tx_data = [("200503", "REC001", Decimal("55555.0"))]
    tx_schema = ["monats_id_str", "rechnung_id", "vertrags_id"]
    tx_df = spark.createDataFrame(tx_data, tx_schema) \
        .withColumn("monats_id", F.to_date(F.col("monats_id_str"), "yyyyMM"))

    # 2. Create Overlapping Contract Versions
    contract_data = [
        ("RV_OLD", 55555.0, 100001, datetime.strptime("20050101", "%Y%m%d"), datetime.strptime("20051231", "%Y%m%d")),
        ("RV_NEW", 55555.0, 100002, datetime.strptime("20050201", "%Y%m%d"), datetime.strptime("20051231", "%Y%m%d"))
    ]
    contract_schema = ["rahmenvertrag_id", "vertrag_id_carmen", "dwh_vertrag_id", "gueltig_von", "gueltig_bis"]
    contract_df = spark.createDataFrame(contract_data, contract_schema)

    # 3. Join
    joined_df = tx_df.join(contract_df, tx_df.vertrags_id == contract_df.vertrag_id_carmen, "left_outer")

    # 4. Apply Window Ranking (Scan_Ranking)
    window_spec = Window.partitionBy("vertrags_id", "rechnung_id") \
        .orderBy(F.col("gueltig_von").desc(), F.col("dwh_vertrag_id").desc())

    ranked_df = joined_df.withColumn("rankindex", F.row_number().over(window_spec))
    final_df = ranked_df.filter(F.col("rankindex") == 1)

    results = final_df.collect()
    
    # Assertions
    assert len(results) == 1
    assert results[0]["rahmenvertrag_id"] == "RV_NEW"
    assert results[0]["dwh_vertrag_id"] == 100002
```

### Pass/Fail Criterion
The test passes if exactly one record is retained and it maps to the contract version with `dwh_vertrag_id = 100002` (the latest version).

---

## Test Case 4: Idempotency & Pre-Delete Verification

### Purpose
Verify that the pre-delete step correctly purges existing records matching incoming keys (`rechnung_id`, `rechnung_datum`, `standardvertrags_id`, `vertrags_id`) before appending new ones, preventing duplicates.

### Setup
1. **Target Table State**: Populate `{GCP_PROJECT}.{BQ_DATASET}.dwh_ta_f_rpos_carm` with a pre-existing record:
   ```sql
   INSERT INTO `your_project.dw_dataset.dwh_ta_f_rpos_carm` 
   (monats_id, debitor_id, rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id, rech_leistung_id_carm, rechpos_brutto_eur)
   VALUES 
   (200504, 'DEB_OLD', 'REC_ID_999', DATE('2005-04-15'), 111, 222, 'LEISTUNG', 500.00);
   ```
2. **Input File**: Create an input file containing a new version of the same record with updated metrics:
   * `rechnung_id = 'REC_ID_999'`
   * `rechnung_datum = '20050415'`
   * `standardvertrags_id = 111`
   * `vertrags_id = 222`
   * `debitor_id = 'DEB_NEW'`
   * `rechpos_brutto_eur = 750.00`

### Action
Execute the PySpark job.

### Pass/Fail Criterion
Run the following BigQuery query to verify that the old record was deleted and only the new record exists.

```sql
SELECT 
  COUNT(1) as total_records,
  SUM(CASE WHEN debitor_id = 'DEB_OLD' THEN 1 ELSE 0 END) as old_records_count,
  SUM(CASE WHEN debitor_id = 'DEB_NEW' THEN 1 ELSE 0 END) as new_records_count,
  SUM(rechpos_brutto_eur) as total_brutto
FROM `your_project.dw_dataset.dwh_ta_f_rpos_carm`
WHERE rechnung_id = 'REC_ID_999';
```

**Assertions**:
* `total_records` must equal `1`.
* `old_records_count` must equal `0`.
* `new_records_count` must equal `1`.
* `total_brutto` must equal `750.00`.

---

## Test Case 5: Schema Validation & Verbatim Error Handling

### Purpose
Verify that malformed data triggers the exact verbatim error messages specified in the design document during the validation phase.

### Setup
Create an input file with a null/empty `rechnung_datum` field (which is a mandatory field).

### Action
Execute the PySpark job. Capture the stdout/stderr of the Spark execution.

### Pass/Fail Criterion
The job must fail with an execution exception, and the logs must contain the exact verbatim error string:
`"Invalid Data in field rechnung_datum"` or `"Invalid data format in rechnung_datum"`.

Below is a `pytest` test verifying this behavior:

```python
def test_verbatim_error_handling(spark):
    # Create record with empty rechnung_datum_raw
    malformed_data = [("200504", "DEB001", "REC001", " ", "111", "222")]
    schema = ["monats_id_raw", "debitor_id_raw", "rechnung_id_raw", "rechnung_datum_raw", "standardvertrags_id_raw", "vertrags_id_raw"]
    
    df = spark.createDataFrame(malformed_data, schema)
    
    # Apply validation logic
    validation_expr = F.when(
        F.trim(F.col("rechnung_datum_raw")) == "" | F.col("rechnung_datum_raw").isNull(),
        F.raise_error("Invalid Data in field rechnung_datum")
    ).otherwise(F.to_date(F.trim(F.col("rechnung_datum_raw")), "yyyyMMdd"))
    
    with pytest.raises(Exception) as excinfo:
        df.withColumn("rechnung_datum", validation_expr).collect()
        
    assert "Invalid Data in field rechnung_datum" in str(excinfo.value)
```

---

## Test Case 6: Trailer Parsing & Audit Log Updates

### Purpose
Verify that the trailer record (`X`) is parsed correctly, the reporting month is calculated as `stichtag - 1 month`, and the control tables `dwh_ta_k_rech_absgrp` and `dwh_ta_k_meldungen` are updated correctly.

### Setup
1. **Control Tables State**: Ensure target tables exist.
2. **Input Trailer Record**:
   `X;BEMERKUNG_GRP12345;20050501;150;INHALT_TEXT;20050501120000`
   * `stichtag = '20050501'` (May 1, 2005)
   * Expected reporting `monats_id` = `stichtag - 1 month` = `'200504'`
   * `abs_grp` = substring of bemerkung from index 9 to 14 = `'GRP12'`
   * `anzahl = 150`

### Action
Execute the PySpark job with `BHB_Eintragsnr=9999`.

### Pass/Fail Criterion
Run the following BigQuery assertions to verify the audit updates:

```sql
-- Assertion 1: Verify dwh_ta_k_rech_absgrp update
SELECT 
  monats_id,
  abs_grp,
  rechnung_datum,
  rechnungsteil
FROM `your_project.dw_dataset.dwh_ta_k_rech_absgrp`
WHERE dateiname = 'CARMEN_B_TEST_pos.fix';
```
*Expected Result*:
* `monats_id` = `'200504'`
* `abs_grp` = `'GRP12'`
* `rechnung_datum` = `2005-05-01`
* `rechnungsteil` = `'P'`

```sql
-- Assertion 2: Verify dwh_ta_k_meldungen update
SELECT 
  anzahl_ds_eof,
  enderecord_text,
  zusatzinfo
FROM `your_project.dw_dataset.dwh_ta_k_meldungen`
WHERE entrynr = 9999;
```
*Expected Result*:
* `anzahl_ds_eof` = `150`
* `enderecord_text` = `'INHALT_TEXT'`
* `zusatzinfo` = `'BEMERKUNG_GRP12345'`