"""
Module: dw_dwh_kunde_abgl_woechentlich_sql.py
Path: dags/dw_dwh_kunde/sql/dw_dwh_kunde_abgl_woechentlich_sql.py

Description:
    Contains the SQL statement generator functions for BigQuery execution.
    Converts legacy Oracle dialect (NVL, TO_DATE) to Standard BigQuery SQL
    (COALESCE, PARSE_DATE) and supports parameterized table destinations.
"""

import os

# Fetch global environment-wide configurations
GCP_PROJECT = os.environ.get("GCP_PROJECT", "project")
CORE_DATASET = os.environ.get("CORE_DATASET", "DWH_KERN")
STAMMDATEN_DATASET = os.environ.get("STAMMDATEN_DATASET", "STAMMDATEN")
REPORTING_DATASET = os.environ.get("REPORTING_DATASET", "REPORTING")


def get_reconciliation_query(logical_date_placeholder: str = "{{ ds_nodash }}") -> str:
    """
    Generates the BigQuery SQL to find mismatches between active customer
    master records and reference customer records based on logical date (p_Stichtag).

    Args:
        logical_date_placeholder (str): The Airflow context macro for date representation.
                                        Defaults to '{{ ds_nodash }}' (YYYYMMDD).

    Returns:
        str: Clean standard BigQuery SQL execution query.
    """
    return f"""
    SELECT
      'ABWEICHUNG' AS MARKER,
      k.KUNDE,
      k.NACHNAME,
      k.VORNAME,
      k.PLZ,
      k.ORT,
      k.STRASSE,
      r.PLZ       AS REF_PLZ,
      r.ORT       AS REF_ORT,
      r.STRASSE   AS REF_STRASSE
    FROM `{GCP_PROJECT}.{CORE_DATASET}.T_KUNDE` k
    JOIN `{GCP_PROJECT}.{STAMMDATEN_DATASET}.T_KUNDE_REFERENZ` r
      ON r.KUNDE = k.KUNDE
    WHERE k.AKTUALISIERT_AM <= PARSE_DATE('%Y%m%d', '{logical_date_placeholder}')
      AND (
        COALESCE(k.PLZ, 'x')     != COALESCE(r.PLZ, 'x')
     OR COALESCE(k.ORT, 'x')     != COALESCE(r.ORT, 'x')
     OR COALESCE(k.STRASSE, 'x') != COALESCE(r.STRASSE, 'x')
      )
    ORDER BY k.KUNDE;
    """


def get_insert_errors_query(logical_date_placeholder: str = "{{ ds }}") -> str:
    """
    Generates the SQL statement to audit and insert detected discrepancies
    into the long-term T_ABGL_KUNDE_ERR tracking table.

    Args:
        logical_date_placeholder (str): The logical partition execution date (YYYY-MM-DD).

    Returns:
        str: SQL insert query.
    """
    return f"""
    INSERT INTO `{GCP_PROJECT}.{REPORTING_DATASET}.T_ABGL_KUNDE_ERR` (
      STICHTAG,
      KUNDEN_ID,
      STG_STRASSE, HIST_STRASSE,
      STG_HAUSNUMMER, HIST_HAUSNUMMER,
      STG_PLZ, HIST_PLZ,
      STG_ORT, HIST_ORT,
      STG_LAND, HIST_LAND,
      LOG_TIMESTAMP
    )
    SELECT
      DATE('{logical_date_placeholder}') as STICHTAG,
      s.KUNDEN_ID,
      s.STRASSE, h.STRASSE,
      s.HAUSNUMMER, h.HAUSNUMMER,
      s.PLZ, h.PLZ,
      s.ORT, h.ORT,
      s.LAND, h.LAND,
      CURRENT_TIMESTAMP() as LOG_TIMESTAMP
    FROM `{GCP_PROJECT}.staging_dataset.STG_KUNDE` s
    INNER JOIN `{GCP_PROJECT}.{CORE_DATASET}.T_KUNDE_HIST` h
      ON s.KUNDEN_ID = h.KUNDEN_ID
    WHERE h.AKTIV_FLAG = TRUE
      AND (
        COALESCE(s.STRASSE, '') != COALESCE(h.STRASSE, '') OR
        COALESCE(s.HAUSNUMMER, '') != COALESCE(h.HAUSNUMMER, '') OR
        COALESCE(s.PLZ, '') != COALESCE(h.PLZ, '') OR
        COALESCE(s.ORT, '') != COALESCE(h.ORT, '') OR
        COALESCE(s.LAND, '') != COALESCE(h.LAND, '')
      );
    """


def get_count_query(logical_date_placeholder: str = "{{ ds }}") -> str:
    """
    Generates a tracking check query returning row mismatch occurrences for auditing.
    """
    return f"""
    SELECT COUNT(1) as total_errors
    FROM `{GCP_PROJECT}.{REPORTING_DATASET}.T_ABGL_KUNDE_ERR`
    WHERE STICHTAG = DATE('{logical_date_placeholder}');
    """