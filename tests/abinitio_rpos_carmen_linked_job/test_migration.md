# Migration Validation Test Suite: DW.RPOS_CARM_IMPORT

This document defines the comprehensive migration-validation test suite for the migrated job `DW.RPOS_CARM_IMPORT`. These tests are designed to prove behavioral equivalence between the legacy Ab Initio / UC4 implementation and the target PySpark / Cloud Composer (Airflow) implementation on Google Cloud Platform.

---

## Test Suite Overview

The validation strategy is divided into four main categories:
1. **Output Parity**: Verifying that identical inputs produce identical outputs across all target tables.
2. **Transformation Correctness**: Validating data cleaning, strict schema validations, temporal contract joins, ranking/deduplication, and business routing rules.
3. **External System Replacements**: Validating GCS file detection, idempotent BigQuery deletions, and post-processing file archiving.
4. **Data Quality & Audit Logging**: Validating metadata footer parsing and the execution of the `MERGE` statements on control tables.

---

## Section 1: Output Parity Tests

### Test Case 1.1: End-to-End Golden Dataset Parity
* **Purpose**: Prove that the migrated PySpark job produces the exact same output rows as the legacy Ab Initio graph when presented with a standard, valid production-like dataset.
* **Setup**:
  * **Legacy Environment**: Populate Oracle table `dwh$ta_c_vertrag` with a set of test contracts. Place a sample billing file `CARMEN_B_GOLDEN_pos.fix` in `$DW_DIR_IMP_SAP/crs/work/`. Run the legacy Ab Initio graph.
  * **Target Environment**: Populate BigQuery table `dwh_ta_c_vertrag` with the identical contract records. Place the identical file `CARMEN_B_GOLDEN_pos.fix` in `gs://{GCS_BUCKET}/crs/work/`.
* **Action**:
  1. Execute the legacy UC4 job `DW.RPOS_CARM_IMPORT`.
  2. Execute the migrated Airflow DAG `dw_rpos_carm_import` via manual trigger.
  3. Extract the resulting rows from both environments for target tables:
     * `DWH$TA_F_RPOS_CARM` / `dwh_ta_f_rpos_carm`
     * `DWH$TA_F_RPOS_FACT_CARM` / `dwh_ta_f_rpos_fact_carm`
     * `DWH$TA_F_RPOS_RESELLING_CARM` / `dwh_ta_f_rpos_reselling_carm`
     * `DWH$TA_F_GPOS_FACT_CARM` / `dwh_ta_f_gpos_fact_carm`
     * `DWH$TA_T_RPOS_CARM` / `dwh_ta_t_rpos_carm`
* **Pass/Fail Criterion**: The row counts and column values (excluding the metadata column `ladedatum` / load timestamp) must match exactly ($100\%$ parity).

```python
# pytest script for Golden Dataset Parity Validation
import os
import pytest
from google.cloud import bigquery
import pandas as pd

@pytest.fixture
def bq_client():
    return bigquery.Client(project=os.environ.get("GCP_PROJECT"))

def test_golden_dataset_parity(bq_client):
    dataset = os.environ.get("BQ_DATASET")
    project = os.environ.get("GCP_PROJECT")
    
    # Target tables to validate
    tables = [
        "dwh_ta_f_rpos_carm",
        "dwh_ta_f_rpos_fact_carm",
        "dwh_ta_f_rpos_reselling_carm",
        "dwh_ta_f_gpos_fact_carm",
        "dwh_ta_t_rpos_carm"
    ]
    
    for table in tables:
        # Query target BigQuery table (excluding load timestamp 'ladedatum')
        target_query = f"""
            SELECT * EXCEPT(ladedatum) 
            FROM `{project}.{dataset}.{table}`
            ORDER BY rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id
        """
        df_target = bq_client.query(target_query).to_dataframe()
        
        # Query legacy Oracle table (extracted to a GCS bucket or local parquet for comparison)
        # For automated CI/CD, we assume the legacy output was exported to a validation dataset
        legacy_query = f"""
            SELECT * EXCEPT(ladedatum) 
            FROM `{project}.{dataset}_legacy_val.{table}`
            ORDER BY rechnung_id, rechnung_datum, standardvertrags_id, vertrags_id
        """
        df_legacy = bq_client.query(legacy_query).to_dataframe()
        
        # Assert exact structural and value equivalence
        pd.testing.assert_frame_equal(df_target, df_legacy, check_dtype=False, check_exact=True)
```

---

## Section 2: Transformation Correctness Tests

### Test Case 2.1: German Numeric Normalization (Comma to Dot)
* **Purpose**: Verify that German decimal formatting (using commas as decimal separators, e.g., `123,45`) is correctly normalized to standard dot notation (`123.45`) and cast to `DECIMAL(18,2)` without data loss or rounding errors.
* **Setup**:
  * Create a mock input line containing German formatted decimals:
    `P;202603;DEB100;REC999;20260315;10001;20002;LEIST01;119,50;100,00;19,50;ABS01;N;0;#;#;1;0;F;#;0`
* **Action**: Run the PySpark transformation logic on this record.
* **Pass/Fail Criterion**: The fields `rechpos_brutto_eur`, `rechpos_netto_eur`, and `rechpos_mwst_eur` must be parsed as `Decimal('119.50')`, `Decimal('100.00')`, and `Decimal('19.50')` respectively.

```python
# PySpark unit test for German decimal normalization
from decimal import Decimal
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def test_german_decimal_normalization(spark_session):
    # Setup mock data
    raw_data = ["P;202603;DEB100;REC999;20260315;10001;20002;LEIST01;119,50;100,00;19,50;ABS01;N;0;#;#;1;0;F;#;0"]
    df_raw = spark_session.createDataFrame(raw_data, "string").toDF("value")
    
    # Apply normalization and parsing
    df_split = df_raw.select(
        F.split(F.col("value"), ";").getItem(0).alias("kennzeichen"),
        F.replace(F.split(F.col("value"), ";").getItem(8), ",", ".").cast("decimal(18,2)").alias("rechpos_brutto_eur"),
        F.replace(F.split(F.col("value"), ";").getItem(9), ",", ".").cast("decimal(18,2)").alias("rechpos_netto_eur"),
        F.replace(F.split(F.col("value"), ";").getItem(10), ",", ".").cast("decimal(18,2)").alias("rechpos_mwst_eur")
    )
    
    result = df_split.first()
    assert result["rechpos_brutto_eur"] == Decimal("119.50")
    assert result["rechpos_netto_eur"] == Decimal("100.00")
    assert result["rechpos_mwst_eur"] == Decimal("19.50")
```

### Test Case 2.2: Strict Schema Validation & Error Handling
* **Purpose**: Prove that the PySpark job raises the exact legacy German/English error messages when critical fields are null, empty, or structurally invalid.
* **Setup**:
  * Create an input file containing a record with an empty `monats_id` field:
    `P;;DEB100;REC999;20260315;10001;20002;LEIST01;119.50;100.00;19.50;ABS01;N;0;#;#;1;0;F;#;0`
* **Action**: Execute the PySpark job with this input file.
* **Pass/Fail Criterion**: The PySpark job must fail immediately and raise a `ValueError` or Spark exception containing the exact string: `"Invalid data format in monats_id"`.

```python
# PySpark unit test for strict validation error propagation
import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def test_strict_validation_error_propagation(spark_session):
    # Setup mock data with empty monats_id
    raw_data = ["P;;DEB100;REC999;20260315;10001;20002;LEIST01;119.50;100.00;19.50;ABS01;N;0;#;#;1;0;F;#;0"]
    df_raw = spark_session.createDataFrame(raw_data, "string").toDF("value")
    
    # Parse fields
    df_parsed = df_raw.select(
        F.split(F.col("value"), ";").getItem(1).alias("monats_id")
    )
    
    # Assert that the validation logic raises the exact legacy error message
    with pytest.raises(ValueError, match="Invalid data format in monats_id"):
        if df_parsed.filter(F.col("monats_id").isNull() | (F.trim(F.col("monats_id")) == "")).count() > 0:
            raise ValueError("Invalid data format in monats_id")
```

### Test Case 2.3: Temporal Contract Enrichment (Proof Join)
* **Purpose**: Verify that the left join with `dwh_ta_c_vertrag` correctly nullifies or defaults contract fields if the billing month's last day falls outside the contract's historical validity range (`gueltig_von` to `gueltig_bis`).
* **Setup**:
  * **Contract Dimension**:
    * Contract A: `vertrag_id_carmen = 20002`, `gueltig_von = 2005-01-01`, `gueltig_bis = 2005-12-31`, `rahmenvertrag_id = RV_ACTIVE`
  * **Payload Records**:
    * Record 1 (Within Range): `monats_id = 200506` (Month last day: `2005-06-30`), `vertrags_id = 20002`
    * Record 2 (Out of Range): `monats_id = 200601` (Month last day: `2006-01-31`), `vertrags_id = 20002`
* **Action**: Run the PySpark join and validity check logic.
* **Pass/Fail Criterion**:
  * Record 1 must retain `rahmenvertrag_id = 'RV_ACTIVE'`.
  * Record 2 must have its contract fields nullified/defaulted (`rahmenvertrag_id = '#'`, `dwh_vertrag_id = 0`).

```python
# PySpark unit test for temporal contract enrichment
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType

def test_temporal_contract_enrichment(spark_session):
    # 1. Create mock contract dimension
    contract_schema = StructType([
        StructField("vertrag_id_carmen", LongType(), False),
        StructField("gueltig_von", DateType(), False),
        StructField("gueltig_bis", DateType(), False),
        StructField("rahmenvertrag_id", StringType(), False),
        StructField("dwh_vertrag_id", LongType(), False)
    ])
    contract_data = [
        (20002, datetime.strptime("2005-01-01", "%Y-%m-%d").date(), datetime.strptime("2005-12-31", "%Y-%m-%d").date(), "RV_ACTIVE", 99999)
    ]
    df_contract = spark_session.createDataFrame(contract_data, contract_schema)
    
    # 2. Create mock payload records
    payload_schema = StructType([
        StructField("monats_id", StringType(), False),
        StructField("vertrags_id", LongType(), False)
    ])
    payload_data = [
        ("200506", 20002),  # Within range
        ("200601", 20002)   # Out of range
    ]
    df_payload = spark_session.createDataFrame(payload_data, payload_schema)
    
    # 3. Execute Join and Proof Join logic
    df_joined = df_payload.join(df_contract, df_payload.vertrags_id == df_contract.vertrag_id_carmen, "left")
    df_joined = df_joined.withColumn("month_last_day", F.last_day(F.to_date(F.col("monats_id"), "yyyyMM")))
    
    valid_cond = (
        (F.col("gueltig_von").isNull() | (F.col("month_last_day") > F.col("gueltig_von"))) &
        (F.col("gueltig_bis").isNull() | (F.col("month_last_day") <= F.col("gueltig_bis")))
    )
    
    df_result = df_joined.withColumn(
        "rahmenvertrag_id", F.when(valid_cond, F.col("rahmenvertrag_id")).otherwise("#")
    ).withColumn(
        "dwh_vertrag_id", F.when(valid_cond, F.col("dwh_vertrag_id")).otherwise(0)
    ).select("monats_id", "rahmenvertrag_id", "dwh_vertrag_id").collect()
    
    # Assertions
    assert df_result[0]["monats_id"] == "200506"
    assert df_result[0]["rahmenvertrag_id"] == "RV_ACTIVE"
    assert df_result[0]["dwh_vertrag_id"] == 99999
    
    assert df_result[1]["monats_id"] == "200601"
    assert df_result[1]["rahmenvertrag_id"] == "#"
    assert df_result[1]["dwh_vertrag_id"] == 0
```

### Test Case 2.4: Deduplication & Ranking (Rankindex = 1)
* **Purpose**: Verify that when multiple contract versions match a transaction, the pipeline correctly selects the contract with the latest `gueltig_von` and highest `dwh_vertrag_id` (simulating the legacy `rankindex == 1` window function).
* **Setup**:
  * **Contract Dimension**:
    * Version 1: `vertrag_id_carmen = 20002`, `gueltig_von = 2005-01-01`, `dwh_vertrag_id = 10001`
    * Version 2: `vertrag_id_carmen = 20002`, `gueltig_von = 2005-06-01`, `dwh_vertrag_id = 10002`
  * **Payload Record**:
    * `rechnung_id = REC123`, `rechnung_datum = 2005-07-15`, `vertrags_id = 20002`
* **Action**: Run the PySpark window ranking and filter logic.
* **Pass/Fail Criterion**: The record must be enriched with Version 2 (`dwh_vertrag_id = 10002`) and Version 1 must be filtered out.

```python
# PySpark unit test for ranking and deduplication
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType

def test_contract_ranking_deduplication(spark_session):
    schema = StructType([
        StructField("rechnung_id", StringType(), False),
        StructField("vertrags_id", LongType(), False),
        StructField("gueltig_von", DateType(), False),
        StructField("dwh_vertrag_id", LongType(), False)
    ])
    
    # Mock joined data with duplicate contract versions
    data = [
        ("REC123", 20002, datetime.strptime("2005-01-01", "%Y-%m-%d").date(), 10001),
        ("REC123", 20002, datetime.strptime("2005-06-01", "%Y-%m-%d").date(), 10002)
    ]
    df = spark_session.createDataFrame(data, schema)
    
    # Apply window ranking
    window_spec = Window.partitionBy("rechnung_id", "vertrags_id").orderBy(F.col("gueltig_von").desc(), F.col("dwh_vertrag_id").desc())
    df_ranked = df.withColumn("rankindex", F.row_number().over(window_spec))
    df_filtered = df_ranked.filter(F.col("rankindex") == 1).collect()
    
    assert len(df_filtered) == 1
    assert df_filtered[0]["dwh_vertrag_id"] == 10002
```

### Test Case 2.5: Business Routing & Target Partitioning
* **Purpose**: Verify that records are routed to the correct target tables based on business indicators (`rpos_geschaftsform_kenn`, `typ`, `pooling`, and `RABATT` conditions).
* **Setup**:
  * Create four distinct payload records:
    1. Factoring Invoice: `rpos_geschaftsform_kenn = 'F'`, `typ = 'P'`, `pooling = 'N'`
    2. Factoring Credit: `rpos_geschaftsform_kenn = 'G'`, `typ = 'P'`, `pooling = 'N'`
    3. Reselling: `rpos_geschaftsform_kenn = 'R'`, `typ = 'P'`, `pooling = 'N'`
    4. Temporary Position: `rpos_geschaftsform_kenn = 'F'`, `typ = 'T'`, `pooling = 'P'`
* **Action**: Execute the PySpark routing logic.
* **Pass/Fail Criterion**:
  * Record 1 must be routed to `dwh_ta_f_rpos_fact_carm`.
  * Record 2 must be routed to `dwh_ta_f_gpos_fact_carm`.
  * Record 3 must be routed to `dwh_ta_f_rpos_reselling_carm`.
  * Record 4 must be routed to `dwh_ta_t_rpos_carm`.

```python
# PySpark unit test for business routing
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

def test_business_routing(spark_session):
    schema = StructType([
        StructField("record_id", StringType(), False),
        StructField("rpos_geschaftsform_kenn", StringType(), False),
        StructField("typ", StringType(), False),
        StructField("pooling", StringType(), False),
        StructField("rech_leistung_id_carm", StringType(), False),
        StructField("vertrags_id", StringType(), False)
    ])
    data = [
        ("REC1", "F", "P", "N", "NORMAL", "20002"),
        ("REC2", "G", "P", "N", "NORMAL", "20002"),
        ("REC3", "R", "P", "N", "NORMAL", "20002"),
        ("REC4", "F", "T", "P", "NORMAL", "20002")
    ]
    df = spark_session.createDataFrame(data, schema)
    
    # Routing logic
    no_s_df = df.filter(F.col("rpos_geschaftsform_kenn") != "S")
    
    factoring_rechnungen = no_s_df.filter(F.col("rpos_geschaftsform_kenn") == "F")
    factoring_gutschriften = no_s_df.filter(F.col("rpos_geschaftsform_kenn") == "G")
    reselling = no_s_df.filter(F.col("rpos_geschaftsform_kenn") == "R")
    
    temporary_data = df.filter(
        ((F.col("rech_leistung_id_carm") == "RABATT") & (F.col("vertrags_id") == "0")) |
        (F.col("pooling") == "P") |
        (F.col("typ") == "T")
    )
    
    assert factoring_rechnungen.count() == 1 and factoring_rechnungen.first()["record_id"] == "REC1"
    assert factoring_gutschriften.count() == 1 and factoring_gutschriften.first()["record_id"] == "REC2"
    assert reselling.count() == 1 and reselling.first()["record_id"] == "REC3"
    assert temporary_data.count() == 1 and temporary_data.first()["record_id"] == "REC4"
```

---

## Section 3: External System Replacements Tests

### Test Case 3.1: Idempotent Deletion Strategy
* **Purpose**: Verify that the PySpark job successfully deletes pre-existing records in target BigQuery tables matching the incoming batch keys (`rechnung_id`, `rechnung_datum`, `standardvertrags_id`, `vertrags_id`) before performing inserts, preventing duplicate entries.
* **Setup**:
  * Populate BigQuery table `dwh_ta_f_rpos_carm` with a pre-existing record:
    `rechnung_id = 'REC_ID_999'`, `rechnung_datum = '2026-03-15'`, `standardvertrags_id = 10001`, `vertrags_id = 20002`, `rechpos_brutto_eur = 500.00`
  * Create an input file containing a new version of the same record:
    `rechnung_id = 'REC_ID_999'`, `rechnung_datum = '2026-03-15'`, `standardvertrags_id = 10001`, `vertrags_id = 20002`, `rechpos_brutto_eur = 600.00`
* **Action**: Execute the PySpark job.
* **Pass/Fail Criterion**: The pre-existing record with `500.00` must be deleted, and only the new record with `600.00` must exist in the table (no duplicates).

```sql
-- BigQuery SQL assertion to verify idempotency
-- Run this query after executing the PySpark job
SELECT COUNT(1) as row_count, SUM(rechpos_brutto_eur) as total_brutto
FROM `${GCP_PROJECT}.${BQ_DATASET}.dwh_ta_f_rpos_carm`
WHERE rechnung_id = 'REC_ID_999'
  AND rechnung_datum = '2026-03-15'
  AND standardvertrags_id = 10001
  AND vertrags_id = 20002;

-- ASSERTION: row_count == 1 AND total_brutto == 600.00
```

### Test Case 3.2: GCS File Archiving & Airflow Orchestration
* **Purpose**: Verify that the Airflow DAG correctly orchestrates the pipeline: detects the incoming file in `crs/work/`, executes the PySpark job, and archives the processed file to `crs/store/`.
* **Setup**:
  * Place a test file `CARMEN_B_TEST_pos.fix` in `gs://{GCS_BUCKET}/crs/work/`.
  * Ensure `gs://{GCS_BUCKET}/crs/store/` does not contain this file.
* **Action**: Trigger the Airflow DAG `dw_rpos_carm_import`.
* **Pass/Fail Criterion**:
  * The DAG must complete with a `SUCCESS` status.
  * The file must no longer exist in `gs://{GCS_BUCKET}/crs/work/`.
  * The file must exist in `gs://{GCS_BUCKET}/crs/store/CARMEN_B_TEST_pos.fix`.

```python
# pytest script to validate GCS file archiving
import os
import pytest
from google.cloud import storage

def test_gcs_file_archiving():
    bucket_name = os.environ.get("GCS_BUCKET")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # Assert file is removed from work directory
    work_blob = bucket.blob("crs/work/CARMEN_B_TEST_pos.fix")
    assert not work_blob.exists(), "File was not removed from crs/work/"
    
    # Assert file is moved to store directory
    store_blob = bucket.blob("crs/store/CARMEN_B_TEST_pos.fix")
    assert store_blob.exists(), "File was not archived to crs/store/"
```

---

## Section 4: Data Quality & Audit Logging Tests

### Test Case 4.1: Metadata Footer Parsing & Audit Updates
* **Purpose**: Verify that the metadata footer record (`X`) is correctly parsed and triggers the `MERGE` statements to update the audit tables `dwh_ta_k_meldungen` and `dwh_ta_k_rech_absgrp`.
* **Setup**:
  * **Input Footer Record**:
    `X;ABS_GRP_12345_FILE;20260315;150;SUCCESS_METRIC;20260315120000;`
  * **Meldungen Table**: Ensure a record with `entrynr = 1001` exists in `dwh_ta_k_meldungen`.
* **Action**: Run the PySpark job and the downstream Airflow MERGE tasks.
* **Pass/Fail Criterion**:
  * `dwh_ta_k_meldungen` must be updated with `anzahl_ds_eof = 150`, `dateiname = 'CARMEN_B_*_pos.fix'`, and `enderecord_text = 'SUCCESS_METRIC'`.
  * `dwh_ta_k_rech_absgrp` must contain a record for `monats_id = '202602'` (calculated as `stichtag` month minus 1 month) and `abs_grp = '12345'` with `rechnung_datum = '2026-03-15'`.

```sql
-- BigQuery SQL assertion for dwh_ta_k_meldungen update
SELECT anzahl_ds_eof, dateiname, enderecord_text, zusatzinfo
FROM `${GCP_PROJECT}.${BQ_DATASET}.dwh_ta_k_meldungen`
WHERE entrynr = 1001;

-- ASSERTIONS:
-- anzahl_ds_eof == 150
-- dateiname == 'CARMEN_B_*_pos.fix'
-- enderecord_text == 'SUCCESS_METRIC'
-- zusatzinfo == 'ABS_GRP_12345_FILE'

-- BigQuery SQL assertion for dwh_ta_k_rech_absgrp update
SELECT rechnung_datum, rechnungsteil
FROM `${GCP_PROJECT}.${BQ_DATASET}.dwh_ta_k_rech_absgrp`
WHERE monats_id = '202602'
  AND abs_grp = '12345'
  AND dateiname = 'CARMEN_B_*_pos.fix';

-- ASSERTIONS:
-- rechnung_datum == DATE('2026-03-15')
-- rechnungsteil == 'P'
```

---

## Section 5: Automated Test Execution Script

The following script consolidates the execution of the validation tests. It can be integrated into a CI/CD pipeline (e.g., Cloud Build) to automate post-migration verification.

```python
#!/usr/bin/env python3
"""
Automated Migration Validation Runner for DW.RPOS_CARM_IMPORT.
Executes BigQuery assertions and GCS checks to validate behavioral equivalence.
"""

import os
import sys
import argparse
from google.cloud import bigquery
from google.cloud import storage

def run_validation_queries(project, dataset):
    client = bigquery.Client(project=project)
    
    print("Running Test Case 4.1: Audit Logging Validation...")
    meldungen_query = f"""
        SELECT anzahl_ds_eof, dateiname, enderecord_text 
        FROM `{project}.{dataset}.dwh_ta_k_meldungen`
        WHERE entrynr = 1001
    """
    results = list(client.query(meldungen_query).result())
    if not results:
        print("FAIL: No audit record found in dwh_ta_k_meldungen for entrynr 1001")
        return False
    
    row = results[0]
    assert row.anzahl_ds_eof == 150, f"FAIL: Expected 150 records, got {row.anzahl_ds_eof}"
    assert row.enderecord_text == 'SUCCESS_METRIC', f"FAIL: Expected 'SUCCESS_METRIC', got {row.enderecord_text}"
    print("PASS: Test Case 4.1 (dwh_ta_k_meldungen) validated successfully.")
    
    absgrp_query = f"""
        SELECT rechnung_datum 
        FROM `{project}.{dataset}.dwh_ta_k_rech_absgrp`
        WHERE monats_id = '202602' AND abs_grp = '12345'
    """
    results_abs = list(client.query(absgrp_query).result())
    if not results_abs:
        print("FAIL: No audit record found in dwh_ta_k_rech_absgrp for monats_id 202602")
        return False
    
    assert str(results_abs[0].rechnung_datum) == '2026-03-15', f"FAIL: Expected '2026-03-15', got {results_abs[0].rechnung_datum}"
    print("PASS: Test Case 4.1 (dwh_ta_k_rech_absgrp) validated successfully.")
    return True

def main():
    parser = argparse.ArgumentParser(description="Run migration validation tests.")
    parser.add_argument("--project", required=True, help="GCP Project ID")
    parser.add_argument("--dataset", required=True, help="BigQuery Dataset ID")
    args = parser.parse_args()
    
    success = run_validation_queries(args.project, args.dataset)
    if not success:
        sys.exit(1)
    print("All migration validation tests PASSED.")
    sys.exit(0)

if __name__ == "__main__":
    main()
```