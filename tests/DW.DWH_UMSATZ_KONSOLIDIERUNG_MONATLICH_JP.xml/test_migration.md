Here is a comprehensive suite of migration-validation tests designed to prove that the migrated Airflow orchestration, PySpark data pipeline, and BigQuery SQL assets are behaviorally equivalent to the legacy UC4, KornShell, and Ab Initio components.

---

# MIGRATION VALIDATION TEST SUITE
**Target Job:** `DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP`  
**Target Platform:** Google Cloud Platform (Composer/Airflow, Dataproc Serverless, BigQuery)

---

## SECTION 1: ORCHESTRATION & PARAMETER PARITY TESTS

### Test Case 1.1: Airflow DAG Structure and Task Lineage Validation
* **Purpose:** Verify that the migrated Airflow DAG (`dw_dwh_umsatz_konsolidierung_monatlich_jp`) matches the legacy UC4 Job Plan (`JOBP`) structure, task dependencies, and default execution properties.
* **Setup:** 
  * Deploy the migrated DAG file `dw_dwh_umsatz_konsolidierung_monatlich_jp.py` to a test Airflow environment.
  * Initialize the Airflow metadata database.
* **Action:** Execute a Python test script using the Airflow DAG bag to inspect the DAG structure programmatically.
  ```python
  # test_dag_structure.py
  from airflow.models import DagBag

  def test_dag_metadata_and_structure():
      dagbag = DagBag(include_examples=False)
      dag_id = "dw_dwh_umsatz_konsolidierung_monatlich_jp"
      dag = dagbag.get_dag(dag_id)
      
      assert dag is not None, f"DAG {dag_id} failed to load."
      assert len(dag.tasks) == 2, f"Expected exactly 2 tasks, found {len(dag.tasks)}"
      
      # Verify Task IDs and Types
      log_start_task = dag.get_task("log_start")
      dataproc_task = dag.get_task("dw_dwh_umsatz_konsolidierung_monatlich_js")
      
      assert log_start_task.__class__.__name__ == "PythonOperator"
      assert dataproc_task.__class__.__name__ == "DataprocSubmitJobOperator"
      
      # Verify Lineage / Execution Order
      assert log_start_task in dataproc_task.upstream_list
      assert dataproc_task in log_start_task.downstream_list
      
      # Verify Default Args & Scheduling
      assert dag.schedule_interval is None, "DAG must be configured with manual/external schedule (None)"
      assert dag.catchup is False, "Catchup must be disabled"
      assert dag.default_args.get('retries') == 0, "Retries must default to 0 to match UC4"
  ```
* **Pass/Fail Criterion:** The test passes if all assertions in `test_dag_metadata_and_structure` evaluate to `True` without throwing exceptions.

---

### Test Case 1.2: Dynamic Parameter Resolution and Jinja Rendering
* **Purpose:** Prove that the dynamic processing month (`VERARBEITUNGSMONAT`) resolves to the correct `YYYYMM` format based on the execution context, matching the legacy UC4 `SYS_DATE("YYYYMM")` behavior.
* **Setup:**
  * Define mock Airflow Variables in the test environment: `GCP_PROJECT="test-proj"`, `GCP_REGION="europe-west3"`, `DATAPROC_CLUSTER="test-cluster"`, `GCS_BUCKET="test-bucket"`.
* **Action:** Render the templates for the `DataprocSubmitJobOperator` task using a specific execution date.
  ```python
  # test_jinja_rendering.py
  from datetime import datetime
  from airflow.models import DagBag, TaskInstance

  def test_parameter_rendering():
      dagbag = DagBag(include_examples=False)
      dag = dagbag.get_dag("dw_dwh_umsatz_konsolidierung_monatlich_jp")
      task = dag.get_task("dw_dwh_umsatz_konsolidierung_monatlich_js")
      
      # Simulate execution for 2026-03-15
      execution_date = datetime(2026, 3, 15)
      ti = TaskInstance(task=task, execution_date=execution_date)
      
      # Render templates
      rendered_job = task.render_template(task.job, ti.get_template_context())
      args = rendered_job["pyspark_job"]["args"]
      
      # Assertions
      assert "-m" in args
      month_index = args.index("-m") + 1
      assert args[month_index] == "202603", f"Expected '202603', but got '{args[month_index]}'"
      
      assert "-k" in args
      company_index = args.index("-k") + 1
      assert args[company_index] == "ALL", f"Expected 'ALL', but got '{args[company_index]}'"
  ```
* **Pass/Fail Criterion:** The test passes if the rendered arguments correctly resolve the execution date to `202603` and the company parameter to `ALL`.

---

### Test Case 1.3: Log Output Parity (Verbatim Print Rule)
* **Purpose:** Ensure that the exact German log message from the legacy UC4 script is written to the execution logs.
* **Setup:**
  * Configure a standard Python logging capture handler.
* **Action:** Execute the `log_start_message` Python callable inside a mock Airflow context.
  ```python
  # test_logging_parity.py
  import logging
  from datetime import datetime
  from unittest.mock import MagicMock

  def test_verbatim_log_output(caplog):
      from dags.dw.dw_dwh_umsatz_konsolidierung_monatlich_jp import log_start_message
      
      # Mock Airflow Context
      context = {
          'execution_date': datetime(2026, 5, 20)
      }
      
      with caplog.at_level(logging.INFO):
          log_start_message(**context)
          
      expected_message = "Umsatzkonsolidierung fuer Monat 202605, Konzerngesellschaft ALL angestossen"
      assert any(expected_message in record.message for record in caplog.records), \
          f"Expected log message '{expected_message}' was not found in logs."
  ```
* **Pass/Fail Criterion:** The test passes if the captured log output contains the exact string: `"Umsatzkonsolidierung fuer Monat 202605, Konzerngesellschaft ALL angestossen"`.

---

## SECTION 2: TRANSFORMATION & DATA PARITY TESTS

### Test Case 2.1: Data Normalization and Cent Rounding Validation
* **Purpose:** Verify that the PySpark transformation correctly rounds monetary values to cents, handles null currencies by defaulting to `EUR`, and maps transaction types (`STORNO`/`GUTSCHRIFT` -> `STORNO`, others -> `REGULAER`).
* **Setup:** Create a local PySpark session and build a mock input DataFrame representing raw staging transactions.
* **Action:** Run the `normalize_umsatz` function on the mock data and assert the output values.
  ```python
  # test_normalization.py
  import pytest
  from pyspark.sql import SparkSession
  from abinitio.umsatz_konsolidierung import normalize_umsatz

  @pytest.fixture(scope="module")
  def spark():
      return SparkSession.builder.master("local[*]").appName("test").getOrCreate()

  def test_normalize_umsatz_logic(spark):
      # Create mock staging data
      schema = "umsatz_id string, konzerngesellschaft string, vertrag string, kunde string, " \
               "tarifgruppen_code string, buchungsdatum string, waehrung string, " \
               "buchungsart string, umsatz_betrag double"
               
      data = [
          ("1", " de01 ", "V1", "K1", " tg01 ", "2026-01-01", None, "REGULAER", 123.456),
          ("2", "AT02", "V2", "K2", "TG02", "2026-01-02", "USD", "STORNO", -50.00),
          ("3", "CH01", "V3", "K3", "TG03", "2026-01-03", "CHF", "GUTSCHRIFT", -10.204),
      ]
      
      df_stg = spark.createDataFrame(data, schema)
      df_normalized = normalize_umsatz(df_stg)
      results = df_normalized.collect()
      
      # Assert Record 1: Trimming, Upper-casing, Default Currency, Cent Rounding (123.456 * 100 = 12345.6 -> 12346)
      assert results[0]["konzerngesellschaft"] == "DE01"
      assert results[0]["tarifgruppen_code"] == "TG01"
      assert results[0]["waehrung"] == "EUR"
      assert results[0]["buchungsart"] == "REGULAER"
      assert results[0]["umsatz_betrag_cent"] == 12346
      
      # Assert Record 2: Storno mapping and negative rounding
      assert results[1]["buchungsart"] == "STORNO"
      assert results[1]["umsatz_betrag_cent"] == -5000
      
      # Assert Record 3: Gutschrift mapping to STORNO and rounding (-10.204 * 100 = -1020.4 -> -1020)
      assert results[2]["buchungsart"] == "STORNO"
      assert results[2]["umsatz_betrag_cent"] == -1020
  ```
* **Pass/Fail Criterion:** The test passes if all string cleaning, currency defaulting, transaction type mapping, and cent-rounding calculations match the expected values exactly.

---

### Test Case 2.2: Join Logic and Unmatched Record Isolation (GCS Drop)
* **Purpose:** Prove that records with invalid/unmatched group companies are successfully filtered out of the main pipeline and written to the GCS error directory as a pipe-delimited file.
* **Setup:**
  * Prepare a mock staging dataset containing one matched and one unmatched company.
  * Prepare a mock company dimension dataset.
* **Action:** Execute `enrich_and_split_data` and verify both the returned DataFrame and the output file.
  ```python
  # test_enrichment_and_split.py
  import os
  import tempfile
  from pyspark.sql import SparkSession
  from abinitio.umsatz_konsolidierung import enrich_and_split_data

  def test_enrich_and_split(spark):
      # Mock Normalized Data
      norm_schema = "umsatz_id string, konzerngesellschaft string, vertrag string, kunde string, " \
                    "tarifgruppen_code string, buchungsdatum string, waehrung string, " \
                    "buchungsart string, umsatz_betrag_cent long"
      norm_data = [
          ("1", "DE01", "V1", "K1", "TG01", "2026-01-01", "EUR", "REGULAER", 10000), # Matched
          ("2", "XX99", "V2", "K2", "TG02", "2026-01-02", "EUR", "REGULAER", 20000), # Unmatched
      ]
      df_norm = spark.createDataFrame(norm_data, norm_schema)
      
      # Mock Dimension Data
      dim_schema = "konzerngesellschaft string, is_current string"
      dim_data = [("DE01", "Y")]
      df_dim = spark.createDataFrame(dim_data, dim_schema)
      
      # Use a temporary local directory to simulate GCS bucket path
      with tempfile.TemporaryDirectory() as tmp_dir:
          class Args:
              konzerngesellschaft = "ALL"
              verarbeitungsmonat = "202601"
          
          df_matched = enrich_and_split_data(df_norm, df_dim, tmp_dir, Args())
          
          # Verify matched output
          matched_records = df_matched.collect()
          assert len(matched_records) == 1
          assert matched_records[0]["konzerngesellschaft"] == "DE01"
          
          # Verify unmatched output written to simulated GCS path
          expected_err_path = os.path.join(
              tmp_dir, "errors", "umsatz", "umsatz_unmatched_ALL_202601.dat"
          )
          assert os.path.exists(expected_err_path)
          
          # Read the written CSV to verify delimiter and content
          with open(os.path.join(expected_err_path, [f for f in os.listdir(expected_err_path) if f.endswith(".csv")][0]), 'r') as f:
              lines = f.readlines()
              # Header check
              assert "umsatz_id|konzerngesellschaft" in lines[0]
              # Data check (unmatched record XX99 must be present)
              assert "2|XX99" in lines[1]
  ```
* **Pass/Fail Criterion:** The test passes if only matched records remain in the returned DataFrame, and unmatched records are written to the output path using a pipe (`|`) delimiter.

---

### Test Case 2.3: Aggregation and Rollup Parity
* **Purpose:** Prove that the PySpark aggregation logic yields the exact same aggregated sums and counts for regular and storno transactions as the legacy Ab Initio `rollup` components.
* **Setup:** Create a mock dataset containing multiple regular and storno records for the same group company, tariff group, and currency.
* **Action:** Run `aggregate_and_rollup` and verify the aggregated metrics.
  ```python
  # test_aggregation.py
  from pyspark.sql import SparkSession
  from abinitio.umsatz_konsolidierung import aggregate_and_rollup

  def test_aggregation_logic(spark):
      schema = "konzerngesellschaft string, tarifgruppen_code string, waehrung string, " \
               "buchungsart string, umsatz_betrag_cent long, umsatz_id string"
               
      data = [
          # Regular transactions (should sum to 30000, count = 2)
          ("DE01", "TG01", "EUR", "REGULAER", 10000, "ID1"),
          ("DE01", "TG01", "EUR", "REGULAER", 20000, "ID2"),
          # Storno transactions (should sum to -5000)
          ("DE01", "TG01", "EUR", "STORNO", -5000, "ID3"),
          # Different group slice (should not mix)
          ("DE01", "TG02", "EUR", "REGULAER", 15000, "ID4")
      ]
      
      df_matched = spark.createDataFrame(data, schema)
      # Mock tariff mapping (identity mapping for simplicity)
      map_schema = "tarifgruppen_code string, map_tarifgruppen_code string"
      map_data = [("TG01", "TG01"), ("TG02", "TG02")]
      df_map = spark.createDataFrame(map_data, map_schema)
      
      df_result = aggregate_and_rollup(df_matched, df_map, "202601")
      results = df_result.collect()
      
      assert len(results) == 2
      
      # Find TG01 record
      tg01_record = next(r for r in results if r["tarifgruppen_code"] == "TG01")
      assert tg01_record["umsatz_summe_cent"] == 30000
      assert tg01_record["storno_summe_cent"] == -5000
      assert tg01_record["anzahl_buchungen"] == 2
      assert tg01_record["verarbeitungsmonat"] == "202601"
  ```
* **Pass/Fail Criterion:** The test passes if the sums and counts match the expected values, and storno sums are correctly aligned with regular sums via the left outer join.

---

## SECTION 3: EXTERNAL SYSTEM & PIPELINE INTEGRATION TESTS

### Test Case 3.1: BigQuery Target Table Append Validation
* **Purpose:** Verify that the PySpark job successfully appends the final consolidated data to the target BigQuery table without schema mismatch.
* **Setup:**
  * Create a temporary BigQuery dataset and target table `FACT_UMSATZ_KONZERN_MONAT` matching the production schema.
* **Action:** Write a sample DataFrame to the BigQuery table and read it back to verify persistence.
  ```python
  # test_bigquery_write.py
  import os
  import pytest
  from pyspark.sql import SparkSession

  @pytest.mark.integration
  def test_bigquery_append(spark):
      project_id = os.environ.get("GCP_PROJECT")
      dataset_id = os.environ.get("BQ_DATASET", "dwh_kern")
      table_name = f"{project_id}.{dataset_id}.FACT_UMSATZ_KONZERN_MONAT"
      
      schema = "konzerngesellschaft string, verarbeitungsmonat string, tarifgruppen_code string, " \
               "waehrung string, umsatz_summe_cent long, storno_summe_cent long, anzahl_buchungen long"
               
      data = [("DE01", "202601", "TG01", "EUR", 10000, -1000, 5)]
      df_to_write = spark.createDataFrame(data, schema)
      
      # Write to BigQuery
      df_to_write.write.format("bigquery") \
          .option("table", table_name) \
          .mode("append") \
          .save()
          
      # Read back and verify
      df_read = spark.read.format("bigquery") \
          .option("table", table_name) \
          .load() \
          .filter((col("verarbeitungsmonat") == "202601") & (col("konzerngesellschaft") == "DE01"))
          
      assert df_read.count() >= 1
  ```
* **Pass/Fail Criterion:** The test passes if the write operation completes without error and the written record is retrievable from BigQuery.

---

### Test Case 3.2: Audit Log Generation and Format Parity
* **Purpose:** Prove that the audit log is written to GCS in JSON format containing the exact count of processed records.
* **Setup:** Provide a mock GCS bucket path or local directory.
* **Action:** Execute the `write_outputs` function and verify the generated JSON audit file.
  ```python
  # test_audit_log.py
  import os
  import json
  import tempfile
  from pyspark.sql import SparkSession
  from abinitio.umsatz_konsolidierung import write_outputs

  def test_audit_log_generation(spark):
      schema = "konzerngesellschaft string, verarbeitungsmonat string, tarifgruppen_code string, " \
               "waehrung string, umsatz_summe_cent long, storno_summe_cent long, anzahl_buchungen long"
      data = [
          ("DE01", "202601", "TG01", "EUR", 10000, 0, 1),
          ("DE01", "202601", "TG02", "EUR", 20000, 0, 1)
      ]
      df_final = spark.createDataFrame(data, schema)
      
      with tempfile.TemporaryDirectory() as tmp_dir:
          class Args:
              konzerngesellschaft = "DE01"
              verarbeitungsmonat = "202601"
              
          write_outputs(df_final, "mock-proj", "mock-ds", tmp_dir, Args(), spark)
          
          expected_audit_path = os.path.join(
              tmp_dir, "logs", "umsatz", "umsatz_konsolidierung_audit_DE01_202601.log"
          )
          assert os.path.exists(expected_audit_path)
          
          # Read JSON file
          json_file = [f for f in os.listdir(expected_audit_path) if f.endswith(".json")][0]
          with open(os.path.join(expected_audit_path, json_file), 'r') as f:
              audit_data = json.load(f)
              
          assert audit_data["total_records_processed"] == 2
  ```
* **Pass/Fail Criterion:** The test passes if the audit log file is successfully created at the correct path and contains the correct record count (`2`).

---

## SECTION 4: DATA QUALITY & EXCEPTION HANDLING TESTS

### Test Case 4.1: Missing Environment Variables Enforcement
* **Purpose:** Verify that the PySpark job fails immediately with a clear error message if critical global environment variables are missing.
* **Setup:** Clear `GCP_PROJECT` and `GCS_BUCKET` from the environment.
* **Action:** Run the PySpark `main()` function and catch the expected exception.
  ```python
  # test_exception_handling.py
  import pytest
  from unittest.mock import patch
  from abinitio.umsatz_konsolidierung import main

  def test_missing_env_vars_raises_value_error():
      # Patch sys.argv to simulate command line execution
      test_args = ["prog", "--verarbeitungsmonat", "202601", "--konzerngesellschaft", "ALL"]
      
      with patch("sys.argv", test_args), \
           patch.dict("os.environ", {}, clear=True): # Clear all env variables
          
          with pytest.raises(ValueError) as exc_info:
              main()
              
          assert "Missing environment variables" in str(exc_info.value)
  ```
* **Pass/Fail Criterion:** The test passes if a `ValueError` is raised containing the message `"Missing environment variables: GCP_PROJECT and GCS_BUCKET must be provided."`