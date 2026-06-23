‌
﻿@extends('layouts.app')

@section('content')
<div class="container">
    <h1>Job Migration Validation Tests: `r_ausd_bp_ta_bpr_beschr.ksh` to BigQuery</h1>

    <p>As a senior data-migration QA engineer, I have designed the following tests to ensure the migrated BigQuery Stored Procedure and its orchestration are behaviorally equivalent to the legacy KornShell script. Due to the unavailability of the core script <code>k_ausd_bp_ta_bpr_beschr.ksh</code>, the transformation correctness and output parity tests for the core logic are based on the assumptions and pseudocode provided in the migration design document and the generated BigQuery Stored Procedure code.</p>

    <p><strong>Project/Dataset Placeholders:</strong>
        Throughout these tests, <code>project</code> refers to your GCP Project ID and <code>dataset</code> refers to your BigQuery Dataset ID. Please replace these with your actual values when running the tests.</p>

    <hr>

    <h2>1. Output Parity & Transformation Correctness (Core Logic)</h2>
    <p>These tests verify that the BigQuery Stored Procedure's core data manipulation logic, as defined in the migration, produces the expected results in the target table <code>fos_tabelle</code> and that the logging/auditing mechanisms function correctly.</p>

    <h3>Test Case 1.1: Default Stichtag and Full Refresh (Wiederanlaufwert = 0)</h3>
    <p>Verifies the behavior when no <code>Stichtag</code> is provided (defaults to <code>CURRENT_DATE()</code>) and a full refresh is triggered (default <code>Wiederanlaufwert = 0</code>).</p>
    <ul>
        <li><strong>Purpose:</strong>
            <ul>
                <li>Validate <code>p_stichtag_string</code> defaulting to <code>CURRENT_DATE()</code>.</li>
                <li>Validate <code>p_wiederanlaufWert</code> defaulting to 0.</li>
                <li>Verify the <code>DELETE FROM fos_tabelle WHERE TRUE</code> operation.</li>
                <li>Verify correct data insertion into <code>fos_tabelle</code> based on the assumed core logic filters for <code>CURRENT_DATE()</code>.</li>
                <li>Verify audit and log entries for a successful run.</li>
            </ul>
        </li>
        <li><strong>Setup:</strong>
            <ol>
                <li>Ensure <code>project.dataset.job_audit</code>, <code>project.dataset.job_log</code>, <code>project.dataset.ta_vertrag_cache</code>, and <code>project.dataset.fos_tabelle</code> tables exist.</li>
                <li>Clear <code>project.dataset.job_audit</code> and <code>project.dataset.job_log</code>.</li>
                <li>Populate <code>project.dataset.ta_vertrag_cache</code> with test data, including records valid for <code>CURRENT_DATE()</code> and records that should be filtered out.</li>
                <li>Populate <code>project.dataset.fos_tabelle</code> with some existing data to confirm the <code>DELETE</code> operation.</li>
            </ol>
            <p><strong>Example <code>ta_vertrag_cache</code> data for <code>CURRENT_DATE() = '2023-10-26'</code>:</strong></p>
            <pre><code class="language-sql">INSERT INTO project.dataset.ta_vertrag_cache (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, FOSHoleLadedatum, ATTRIBUTE_1, PROVISION_VALUE) VALUES
('1001', '2023-01-01', '2023-12-31', '2023-10-26', NULL, 'AttrA', 100.00), -- Expected to be inserted
('1002', '2023-10-26', '2024-10-25', '2023-10-26', NULL, 'AttrB', 200.00), -- Expected to be inserted
('1003', '2023-01-01', '2023-10-25', '2023-10-26', NULL, 'AttrC', 300.00), -- Filtered: Gueltig_bis < Stichtag
('1004', '2023-10-27', '2024-12-31', '2023-10-26', NULL, 'AttrD', 400.00), -- Filtered: Gueltig_von > Stichtag
('1005', '2023-01-01', '2023-12-31', '2023-10-25', NULL, 'AttrE', 500.00), -- Filtered: LADEDATUM != Stichtag
('1006', '2023-01-01', '2023-12-31', '2023-10-26', '2023-10-20', 'AttrF', 600.00), -- Filtered: FOSHoleLadedatum IS NOT NULL
('1007', '2023-01-01', '2023-12-31', '2023-10-26', NULL, 'AttrG', 700.00); -- Expected to be inserted
</code></pre>
            <p><strong>Example <code>fos_tabelle</code> initial data:</strong></p>
            <pre><code class="language-sql">INSERT INTO project.dataset.fos_tabelle (DWH_VERTRAG_ID, FOS_ATTRIBUTE_1, FOS_PROVISION_DATE, FOS_PROVISION_VALUE) VALUES
('9999', 'OldData', '2023-01-01', 999.99); -- Should be deleted
</code></pre>
        </li>
        <li><strong>Action:</strong>
            <p>Execute the BigQuery Stored Procedure without providing <code>p_stichtag_string</code> and <code>p_wiederanlaufWert</code> (or pass NULL/empty string for <code>p_stichtag_string</code> and NULL for <code>p_wiederanlaufWert</code>).</p>
            <pre><code class="language-python"># pytest code (assuming BigQuery client is configured)
from google.cloud import bigquery
import datetime

def test_default_stichtag_full_refresh():
    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"
    sp_name = "ausd_bp_ta_bpr_beschr"
    
    # Setup: Clear tables, insert test data
    client.query(f"DELETE FROM `{project_id}.{dataset_id}.job_audit` WHERE TRUE").result()
    client.query(f"DELETE FROM `{project_id}.{dataset_id}.job_log` WHERE TRUE").result()
    client.query(f"DELETE FROM `{project_id}.{dataset_id}.ta_vertrag_cache` WHERE TRUE").result()
    client.query(f"DELETE FROM `{project_id}.{dataset_id}.fos_tabelle` WHERE TRUE").result()

    today = datetime.date.today()
    today_str_ddmmyyyy = today.strftime('%Y-%m-%d') # For SQL insert
    
    # Insert ta_vertrag_cache data
    client.query(f"""
        INSERT INTO `{project_id}.{dataset_id}.ta_vertrag_cache` (DWH_VERTRAG_ID, Gueltig_von, Gueltig_bis, LADEDATUM, FOSHoleLadedatum, ATTRIBUTE_1, PROVISION_VALUE) VALUES
        ('1001', '2023-01-01', '{today_str_ddmmyyyy}', '{today_str_ddmmyyyy}', NULL, 'AttrA', 100.00),
        ('1002', '{today_str_ddmmyyyy}', '{today_str_ddmmyyyy}', '{today_str_ddmmyyyy}', NULL, 'AttrB', 200.00),
        ('1003', '2023-01-01', '{today_str_ddmmyyyy - datetime.timedelta(days=1)}', '{today_str_ddmmyyyy}', NULL, 'AttrC', 300.00),
        ('1004', '{today_str_ddmmyyyy + datetime.timedelta(days=1)}', '2024-12-31', '{today_str_ddmmyyyy}', NULL, 'AttrD', 400.00),
        ('1005', '2023-01-01', '2023-12-31', '{today_str_ddmmyyyy - datetime.timedelta(days=1)}', NULL, 'AttrE', 500.00),
        ('1006', '2023-01-01', '2023-12-31', '{today_str_ddmmyyyy}', '{today_str_ddmmyyyy - datetime.timedelta(days=5)}', 'AttrF', 600.00),
        ('1007', '2023-01-01', '2023-12-31', '{today_str_ddmmyyyy}', NULL, 'AttrG', 700.00);
    """).result()
    
    # Insert initial fos_tabelle data
    client.query(f"""
        INSERT INTO `{project_id}.{dataset_id}.fos_tabelle` (DWH_VERTRAG_ID, FOS_ATTRIBUTE_1, FOS_PROVISION_DATE, FOS_PROVISION_VALUE) VALUES
        ('9999', 'OldData', '2023-01-01', 999.99);
    """).result()

    # Action: Call SP with default parameters
    query = f"CALL `{project_id}.{dataset_id}.{sp_name}`(NULL, NULL);"
    client.query(query).result()

    # Assertions
    # 1. Check fos_tabelle content
    result_fos = client.query(f"SELECT DWH_VERTRAG_ID, FOS_ATTRIBUTE_1, FOS_PROVISION_DATE, FOS_PROVISION_VALUE FROM `{project_id}.{dataset_id}.fos_tabelle` ORDER BY DWH_VERTRAG_ID").result()
    rows_fos = [dict(row) for row in result_fos]
    
    expected_fos_ids = ['1001', '1002', '1007']
    assert len(rows_fos) == len(expected_fos_ids)
    assert all(row['DWH_VERTRAG_ID'] in expected_fos_ids for row in rows_fos)
    assert all(row['FOS_PROVISION_DATE'] == today for row in rows_fos)

    # 2. Check job_