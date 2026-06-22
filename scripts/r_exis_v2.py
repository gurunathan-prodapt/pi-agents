# Legacy Source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
# Job: DW.BERT_ABLAUFSTEUERUNG
# Re-implementation of r_exis_v2 shell script for data export.
# This script exports data from BigQuery to GCS.

import argparse
import logging
from google.cloud import bigquery # Only import bigquery client if no storage client needed explicitly
import os # For os.path.abspath in DAG, not for GCS operations directly

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def export_bigquery_to_gcs(
    project_id: str,
    dataset_id: str,
    table_id: str,
    gcs_bucket_name: str,
    gcs_destination_path: str,
    file_format: str = 'CSV',
    field_delimiter: str = ',',
    print_header: bool = True
):
    """Exports data from a BigQuery table to a GCS bucket."""
    client = bigquery.Client(project=project_id)
    destination_uri = f"gs://{gcs_bucket_name}/{gcs_destination_path}"
    table_ref = client.dataset(dataset_id, project=project_id).table(table_id)

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = file_format.upper()
    job_config.field_delimiter = field_delimiter
    job_config.print_header = print_header

    logging.info(f"Exporting {project_id}.{dataset_id}.{table_id} to {destination_uri}")
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="US",  # Adjust to your BigQuery dataset location if needed
    )
    extract_job.result()  # Waits for the job to complete
    logging.info(f"Data exported successfully to {destination_uri}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Export BigQuery table data to Google Cloud Storage."
    )
    parser.add_argument("--project_id", required=True, help="Google Cloud project ID.")
    parser.add_argument("--dataset_id", required=True, help="BigQuery dataset ID.")
    parser.add_argument("--table_id", required=True, help="BigQuery table ID to export.")
    parser.add_argument("--gcs_bucket_name", required=True, help="GCS bucket name for export.")
    parser.add_argument("--gcs_destination_path", required=True, help="GCS path within the bucket (e.g., folder/file.csv).")
    parser.add_argument("--file_format", default="CSV", help="Output file format (e.g., CSV, JSON, AVRO, PARQUET).")
    parser.add_argument("--field_delimiter", default=",", help="Delimiter for CSV files.")
    parser.add_argument("--print_header", type=bool, default=True, help="Whether to print header row for CSV.")

    args = parser.parse_args()

    # Example usage:
    # python r_exis_v2.py --project_id your-gcp-project --dataset_id your_dataset --table_id your_table --gcs_bucket_name your-bucket --gcs_destination_path exports/your_table_export_{{ ds_nodash }}.csv
    export_bigquery_to_gcs(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        table_id=args.table_id,
        gcs_bucket_name=args.gcs_bucket_name,
        gcs_destination_path=args.gcs_destination_path,
        file_format=args.file_format,
        field_delimiter=args.field_delimiter,
        print_header=args.print_header
    )