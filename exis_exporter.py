# Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2 (redesigned)
# Job: EXIS

import argparse
import os
import io
import gzip
from google.cloud import bigquery
from google.cloud import storage
import paramiko
from datetime import datetime
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ExisExporter:
    def __init__(self, config):
        self.config = config
        self.bq_client = bigquery.Client()
        self.gcs_client = storage.Client()
        # Use config or environment variables for project ID and dataset
        self.project_id = os.environ.get("GCP_PROJECT_ID", self.config.get("gcp_project_id", "your-gcp-project-id"))
        self.dwh_raw_layer_dataset = self.config.get("dwh_raw_layer_dataset", "dwh_raw_layer")
        # GCS bucket name
        self.gcs_bucket_name = self.config.get("gcs_bucket_name", "your-exis-exports-bucket")
        # SFTP configuration
        self.sftp_host = self.config.get("sftp_host")
        self.sftp_port = self.config.get("sftp_port", 22)
        self.sftp_user = self.config.get("sftp_user")
        self.sftp_password = self.config.get("sftp_password") # In production, use Secret Manager or Airflow Connections
        self.sftp_remote_path = self.config.get("sftp_remote_path")

    def _execute_bigquery_query(self, sql_file_path, query_params=None):
        """Executes a BigQuery SQL query and returns the results as a CSV string."""
        logging.info(f"Executing BigQuery query from: {sql_file_path}")
        try:
            with open(sql_file_path, 'r') as f:
                query_template = f.read()
        except FileNotFoundError:
            logging.error(f"SQL file not found: {sql_file_path}")
            raise

        # Replace project_id placeholder and then create BigQuery query parameters
        query = query_template.replace("`project_id.dwh_raw_layer.", f"`{self.project_id}.{self.dwh_raw_layer_dataset}.")

        job_config = bigquery.QueryJobConfig()
        if query_params:
            bq_params = []
            for key, value in query_params.items():
                # Assuming all params in this context are simple types (INT64, STRING)
                if isinstance(value, int):
                    bq_params.append(bigquery.ScalarQueryParameter(key, "INT64", value))
                else:
                    bq_params.append(bigquery.ScalarQueryParameter(key, "STRING", value))
            job_config.query_parameters = bq_params

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            results = query_job.result()
            logging.info(f"BigQuery query completed. Rows returned: {results.total_rows}")
        except Exception as e:
            logging.error(f"Error executing BigQuery query: {e}")
            raise

        # Fetch results as CSV string
        data_rows = []
        headers = [field.name for field in results.schema]
        data_rows.append(','.join(headers)) # CSV header

        for row in results:
            data_rows.append(','.join([str(item) if item is not None else '' for item in row]))
        return "\n".join(data_rows)

    def _post_process_data(self, data_string, footer_config):
        """Appends a footer to the data string, similar to nawk logic."""
        if not footer_config:
            return data_string

        footer_elements = []
        # Footer format: X|<DESTINATION_FILE>|<FROM YYYYMMDD>|NR|V_F_NNA_Daten|<SYSDATE YYYYMMDD>
        footer_template_keys = ["header", "destination_file", "from_date", "record_count_placeholder", "fixed_string", "sysdate"]
        
        for key in footer_template_keys:
            if key == "record_count_placeholder":
                # Assuming the first line is header, so exclude it from record count
                record_count = len(data_string.splitlines()) - 1
                footer_elements.append(str(record_count))
            else:
                val = footer_config.get(key, '')
                footer_elements.append(str(val)) # Ensure string conversion

        footer_line = '|'.join(footer_elements)
        logging.info(f"Appending footer: {footer_line}")
        return data_string + "\n" + footer_line + "\n"

    def _compress_and_upload_to_gcs(self, data_string, gcs_path):
        """Compresses data with gzip and uploads to GCS."""
        logging.info(f"Compressing data and uploading to GCS path: gs://{self.gcs_bucket_name}/{gcs_path}")
        compressed_data = gzip.compress(data_string.encode('utf-8'))
        bucket = self.gcs_client.bucket(self.gcs_bucket_name)
        blob = bucket.blob(gcs_path)
        
        try:
            blob.upload_from_string(compressed_data, content_type='application/gzip')
            logging.info("Upload to GCS complete.")
            return f"gs://{self.gcs_bucket_name}/{gcs_path}"
        except Exception as e:
            logging.error(f"Error uploading to GCS: {e}")
            raise

    def _sftp_distribute(self, local_file_path, remote_file_name):
        """Transfers a local file to a remote SFTP server."""
        if not all([self.sftp_host, self.sftp_user, self.sftp_password, self.sftp_remote_path]):
            logging.warning("SFTP configuration incomplete (host, user, password, or remote_path missing). Skipping SFTP distribution.")
            return

        logging.info(f"Initiating SFTP transfer to {self.sftp_host}:{self.sftp_remote_path}/{remote_file_name}")
        transport = None
        sftp = None
        try:
            transport = paramiko.Transport((self.sftp_host, self.sftp_port))
            transport.connect(username=self.sftp_user, password=self.sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            remote_full_path = os.path.join(self.sftp_remote_path, remote_file_name).replace("\\", "/") # Handle Windows path if any
            sftp.put(local_file_path, remote_full_path)
            logging.info(f"SFTP transfer of {remote_file_name} to {self.sftp_host}:{remote_full_path} complete.")
        except Exception as e:
            logging.error(f"Error during SFTP transfer: {e}")
            raise
        finally:
            if sftp:
                sftp.close()
            if transport:
                transport.close()

    def run_export_job(self):
        """Orchestrates the entire export process."""
        job_name = self.config.get("job_name", "UNKNOWN_JOB")
        sql_file_path = self.config.get("sql_file_path")
        output_base_name = self.config.get("output_base_name")
        query_parameters = self.config.get("query_parameters", {})
        footer_config = self.config.get("footer_config", {})
        enable_sftp = self.config.get("enable_sftp", False)
        sftp_output_name = self.config.get("sftp_output_name")
        gcs_archive_path = self.config.get("gcs_archive_path")

        if not all([sql_file_path, output_base_name, gcs_archive_path]):
            logging.error(f"Missing essential configuration for job {job_name} (sql_file_path, output_base_name, or gcs_archive_path). Exiting.")
            return

        logging.info(f"Starting EXIS export job: {job_name}")

        try:
            # 1. Execute BigQuery Query
            raw_data = self._execute_bigquery_query(sql_file_path, query_parameters)

            # 2. Post-process data (add footer)
            processed_data = self._post_process_data(raw_data, footer_config)

            # Output file naming
            output_file_name_gcs = f"{output_base_name}.csv.gz"
            output_file_name_sftp = sftp_output_name if sftp_output_name else f"{output_base_name}.csv.gz"

            # 3. Compress and Upload to GCS (to the final archive path)
            final_gcs_path = os.path.join(gcs_archive_path, output_file_name_gcs).replace("\\", "/")
            gcs_uri = self._compress_and_upload_to_gcs(processed_data, final_gcs_path)

            # 4. SFTP Distribution (if enabled)
            if enable_sftp:
                # To SFTP, we need a local file. We create a temporary one.
                temp_local_file_path = f"/tmp/{output_file_name_sftp}"
                with gzip.open(temp_local_file_path, 'wb') as f:
                    f.write(processed_data.encode('utf-8'))

                self._sftp_distribute(temp_local_file_path, output_file_name_sftp)
                os.remove(temp_local_file_path) # Clean up temp file

            logging.info(f"EXIS export job: {job_name} completed. Output available at {gcs_uri}")
            return gcs_uri
        except Exception as e:
            logging.error(f"Job {job_name} failed with error: {e}")
            raise


def main():
    parser = argparse.ArgumentParser(description="EXIS Data Exporter to BigQuery, GCS, and SFTP.")
    parser.add_argument("--config_file", required=True, help="Path to a JSON configuration file.")
    args = parser.parse_args()

    with open(args.config_file, 'r') as f:
        config = json.load(f)

    exporter = ExisExporter(config)
    exporter.run_export_job()

if __name__ == "__main__":
    main()