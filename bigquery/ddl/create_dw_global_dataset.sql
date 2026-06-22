-- DDL for dw_global dataset
-- Legacy source: vobs/dw_source/istools/seu/template/.dw_global
-- Job: vobs/dw_source/istools/seu/template/.dw_global

-- Create a BigQuery dataset to house the stored procedure and any related configuration tables.
-- Replace `your_project_id`, `your_dataset_name`, and `your_gcp_region` with your actual GCP project ID,
-- desired dataset name, and GCP region (e.g., 'US', 'EU', 'us-central1').
CREATE SCHEMA IF NOT EXISTS `your_project_id.your_dataset_name`
OPTIONS(
  location = 'your_gcp_region'
);