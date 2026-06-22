# This Airflow DAG replaces the legacy UC4 job EXIS_SD_APT_NNA_VOIC.
# It orchestrates the export of telephone system master data from BigQuery to SFTP.

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.utils.dates import days_ago
import logging
import csv
import io
import gzip
import os

# --- SFTP Configuration (Placeholder - Replace with actual values and secure handling) ---
SFTP_HOST = os.environ.get("SFTP_HOST", "sftp.example.com")
SFTP_PORT = int(os.environ.get("SFTP_PORT", 22))
SFTP_USERNAME = os.environ.get("SFTP_USERNAME", "sftpuser")
# Recommended: Use Airflow Connections or Google Secret Manager for passwords/keys
SFTP_PASSWORD = os.environ.get("SFTP_PASSWORD", "sftppassword")
SFTP_REMOTE_PATH = os.environ.get("SFTP_REMOTE_PATH", "/upload/nna_voice")
# --------------------------------------------------------------------------------------

# --- GCP Configuration ---
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your_project")
DATASET_ID = os.environ.get("GCP_DATASET_ID", "your_dataset")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "your-gcs-bucket")
# -------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def build_bigquery_sql(**kwargs):
    """
    Constructs the BigQuery SQL query for data extraction and transformation.
    The MONATS_ID is dynamically set to the previous month's YYYYMM.
    """
    ds_nodash = kwargs["ds_nodash"] # e.g., 20231026
    # For MONATS_ID (YYYYMM), we want the month of the current DAG run date.
    # If the DAG runs on '2023-10-26', then MONATS_ID should be 202310.
    # The original design document used CURRENT_DATE() and FORMAT_DATE.
    # If historical data is needed, this parameterization should be handled by Airflow variables or DAG configuration.
    # For this implementation, we will use the execution date's month.
    monats_id = ds_nodash[:6] # Extracts YYYYMM

    sql = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.DWHM_APT_NNA_Voice`
        OPTIONS(
          description = 'Data export of telephone system masterdata.'
        )
        AS
        SELECT
          NNA.MONATS_ID,
          NNA.RAHMENVERTRAG,
          VER.MSISDN,
          VER.KUNDENKONTO,
          VER.T_MOBILE_KUNDENNUMMER,
          TAR.TARIF_ID,
          CONCAT(TAR.MP_MARKTPRODUKT_BEZ, ',', TAR.MP_EG_JN_BEZ, ',', TAR.MP_GENERATION_BEZ) AS TARIF,
          TVD.LEISTUNGSKLASSE_ID,
          TVD.LEISTUNGSKLASSE_TEXT,
          NNA.VERBINDUNGEN,
          ROUND(NNA.DAUER_SEK / 60, 2) AS DAUER_MIN,
          ROUND(NNA.RBETRAG_VBUD_NETTO_CENT / 100, 2) AS RBETRAG_VBUD_NETTO_EURO,
          TAR.MP_EG_JN_ID,
          TAR.MP_EG_JN_BEZ,
          TAR.MP_GENERATION_ID,
          TAR.MP_GENERATION_BEZ
        FROM (
          SELECT
            TRF.DWH_TARIF_ID,
            TRF.TARIF_ID,
            D.MP_MARKTPRODUKT_BEZ,
            D.MP_EG_JN_BEZ,
            D.MP_GENERATION_BEZ,
            TRF.GUELTIG_BIS,
            D.MP_EG_JN_ID,
            D.MP_GENERATION_ID
          FROM `{PROJECT_ID}.{DATASET_ID}.DWH_VI_L_MAP_FA_TARIF` AS TRF
          JOIN `{PROJECT_ID}.{DATASET_ID}.BL_D_TARIF` AS D
            ON TRF.TARIF_ID = D.TARIF_ID
        ) AS TAR
        JOIN `{PROJECT_ID}.{DATASET_ID}.DWH_VI_C_VERTRAG` AS VER
          ON TAR.DWH_TARIF_ID = VER.DWH_TARIF_ID
        JOIN `{PROJECT_ID}.{DATASET_ID}.DWH_VI_F_NNV_TVD_12_MONATE` AS NNA
          ON VER.DWH_VERTRAG_ID = NNA.DWH_VERTRAG_ID
        JOIN `{PROJECT_ID}.{DATASET_ID}.DWH_VI_L_TVD_LEISTUNGSKLASSE` AS TVD
          ON NNA.LEISTUNGSKLASSE_ID = TVD.LEISTUNGSKLASSE_ID
        WHERE NNA.RAHMENVERTRAG IS NOT NULL
          AND NNA.MONATS_ID = CAST('{monats_id}' AS INT64)
          AND TAR.GUELTIG_BIS = DATE '4712-12-31'
          AND (
            (TVD.LEISTUNGSKLASSEGR_ID = 1 AND (TVD.LEISTUNGSKLASSE_ID < 300 OR TVD.LEISTUNGSKLASSE_ID > 399))
            OR (
              LENGTH(TRIM(CAST(TVD.LEISTUNGSKLASSE_ID AS STRING))) = 6
              AND TVD.LEISTUNGSKLASSE_ID < 699999
              AND CAST(FLOOR(TVD.LEISTUNGSKLASSE_ID / 1000) AS INT64) <> 622
            )
          );
    """
    return sql

def add_header_trailer_and_sftp(**kwargs):
    """
    Downloads the gzipped CSV from GCS, adds header/trailer (nawk-like logic),
    re-compresses, and SFTPs the file to the external target.
    """
    from google.cloud import storage
    import paramiko # paramiko needs to be installed in the Airflow environment

    ds_nodash = kwargs["ds_nodash"] # e.g., 20231026
    sftp_filename = f"DWHM_APT_NNA_Voice_{ds_nodash}.csv.gz"
    gcs_source_blob = f"exis_sd_apt_nna_voic/{sftp_filename}"
    gcs_source_path = f"gs://{GCS_BUCKET}/{gcs_source_blob}"

    logging.info(f"Processing GCS file: {gcs_source_path}")

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_source_blob)

    if not blob.exists():
        raise FileNotFoundError(f"Source file not found in GCS: {gcs_source_path}")

    # Download gzipped content
    gzipped_content = blob.download_as_bytes()

    # Decompress and read rows
    with gzip.open(io.BytesIO(gzipped_content), 'rt', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)

    num_records = len(rows) # Count of data rows

    # Prepare header and trailer based on nawk logic
    # Header: H|<FROM YYYYMMDD>|V_F_NNA_Voice|<SYSDATE YYYYMMDD>
    # Trailer: X|<DESTINATION_FILE>|<FROM YYYYMMDD>|NR|V_F_NNA_Voice|<SYSDATE YYYYMMDD>
    header_line = f"H|{ds_nodash}|V_F_NNA_Voice|{ds_nodash}"
    trailer_line = f"X|{sftp_filename}|{ds_nodash}|{num_records}|V_F_NNA_Voice|{ds_nodash}"

    # Reconstruct CSV content with header and trailer
    output_buffer = io.StringIO()
    output_buffer.write(header_line + "\n")
    writer = csv.writer(output_buffer)
    writer.writerows(rows)
    output_buffer.write(trailer_line + "\n")

    final_csv_content = output_buffer.getvalue().encode('utf-8')

    # Re-compress
    gzipped_output_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gzipped_output_buffer, mode='wb') as gz_file:
        gz_file.write(final_csv_content)
    final_gzipped_content = gzipped_output_buffer.getvalue()

    # SFTP Transfer
    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        remote_file_path = os.path.join(SFTP_REMOTE_PATH, sftp_filename)
        logging.info(f"Attempting to SFTP file to {SFTP_HOST}:{remote_file_path}")

        with sftp.open(remote_file_path, 'wb') as remote_file:
            remote_file.write(final_gzipped_content)

        logging.info(f"Successfully SFTPed {sftp_filename} to {remote_file_path}")

    except Exception as e:
        logging.error(f"SFTP failed: {e}")
        raise
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()


with DAG(
    dag_id="dw_dwh_exis_sd_apt_nna_voic",
    default_args=default_args,
    description="Data export of telephone system masterdata for EXIS_SD_APT_NNA_VOIC.",
    schedule_interval="@monthly", # Assuming a monthly run as per design
    catchup=False,
    tags=["bigquery", "export", "telephone", "masterdata"],
) as dag:

    process_voice_export = BigQueryExecuteQueryOperator(
        task_id="process_voice_export",
        sql=build_bigquery_sql(ds_nodash="{{ ds_nodash }}"),
        use_legacy_sql=False,
        # create_disposition is not needed with CREATE OR REPLACE TABLE
        write_disposition="WRITE_TRUNCATE", # This applies to the query result being written to the table
        location="US", # Specify your BigQuery dataset location
        gcp_conn_id="google_cloud_default",
    )

    export_to_gcs = BigQueryToGCSOperator(
        task_id="export_to_gcs",
        source_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.DWHM_APT_NNA_Voice",
        destination_cloud_storage_uris=[f"gs://{GCS_BUCKET}/exis_sd_apt_nna_voic/DWHM_APT_NNA_Voice_{{ ds_nodash }}.csv.gz"],
        compression="GZIP",
        export_format="CSV",
        field_delimiter=",",
        print_header=False, # We will add custom header later
        gcp_conn_id="google_cloud_default",
    )

    add_header_trailer_and_sftp_task = PythonOperator(
        task_id="add_header_trailer_and_sftp",
        python_callable=add_header_trailer_and_sftp,
        op_kwargs={
            "ds_nodash": "{{ ds_nodash }}"
        },
    )

    process_voice_export >> export_to_gcs >> add_header_trailer_and_sftp_task