# This file replaces the legacy customer/crm_lineage_tracker.py script.
# It tracks and documents source-to-target data lineage within Google Cloud Platform,
# leveraging BigQuery Information Schema, IAM, and Google Cloud Storage.
# Job: customer/crm_lineage_tracker.py

import argparse
import json
from datetime import datetime

from google.cloud import bigquery
from google.cloud import storage
from google.cloud import iam_admin_v1  # For advanced IAM policy checks, though simplified in this example

# --- Configuration Constants ---
# These should ideally be externalized (e.g., via environment variables,
# Secret Manager, or a dedicated configuration file in GCS).
# For demonstration purposes, they are defined here.
DEFAULT_GCP_PROJECT = "your-gcp-project-id"  # Replace with your actual GCP project ID
DEFAULT_BIGQUERY_DATASETS = ["your_dataset_1", "your_dataset_2"]  # Replace with relevant BigQuery dataset IDs
DEFAULT_AUDIT_DATASET = "your_audit_dataset"  # Dataset where ETL_JOB_AUDIT table resides
DEFAULT_AUDIT_TABLE = "etl_job_audit"  # Table name for ETL job audits
DEFAULT_OUTPUT_GCS_BUCKET = "your-lineage-output-bucket"  # GCS bucket for lineage JSON
DEFAULT_OUTPUT_GCS_PREFIX = "lineage/crm"

# --- Helper Functions ---

def get_bigquery_client():
    """Initializes and returns a BigQuery client."""
    return bigquery.Client(project=DEFAULT_GCP_PROJECT)

def get_storage_client():
    """Initializes and returns a Google Cloud Storage client."""
    return storage.Client(project=DEFAULT_GCP_PROJECT)

def _schema_to_domain(project_id: str, dataset_id: str) -> str:
    """
    Maps BigQuery project/dataset IDs to business domains.
    This is a conceptual mapping and should be customized.
    """
    if project_id == "finance-proj" or dataset_id.startswith("finance_"):
        return "finance"
    elif project_id == "sales-proj" or dataset_id.startswith("sales_"):
        return "sales"
    elif project_id == "crm-proj" or dataset_id.startswith("crm_"):
        return "customer"
    return "unknown"

def extract_metadata_from_bigquery(
    client: bigquery.Client,
    gcp_projects: list[str],
    bigquery_datasets: list[str],
    run_date: str
) -> dict:
    """
    Extracts metadata from BigQuery Information Schema, and a hypothetical
    ETL job audit table.

    Args:
        client: BigQuery client.
        gcp_projects: List of GCP project IDs to scan.
        bigquery_datasets: List of BigQuery dataset IDs to scan within each project.
        run_date: The date for which to extract audit information (YYYY-MM-DD).

    Returns:
        A dictionary containing extracted tables, views, routines, and job audits.
    """
    extracted_metadata = {
        "tables": [],
        "views": [],
        "routines": [],
        "table_dependencies": [],
        "cross_dataset_access": [],
        "etl_job_audits": []
    }

    print(f"Extracting metadata for projects: {gcp_projects}, datasets: {bigquery_datasets}")

    for project_id in gcp_projects:
        for dataset_id in bigquery_datasets:
            full_dataset_id = f"{project_id}.{dataset_id}"

            # --- Extract Tables ---
            tables_query = f"""
                SELECT
                    table_catalog,
                    table_schema,
                    table_name,
                    table_type,
                    creation_time,
                    row_count,
                    num_partitions
                FROM
                    `{project_id}`.`{dataset_id}`.INFORMATION_SCHEMA.TABLES
                WHERE
                    table_type = 'BASE TABLE'
            """
            for row in client.query(tables_query).result():
                extracted_metadata["tables"].append(dict(row))

            # --- Extract Views and their inferred dependencies ---
            views_query = f"""
                SELECT
                    table_catalog,
                    table_schema,
                    table_name,
                    view_definition,
                    creation_time
                FROM
                    `{project_id}`.`{dataset_id}`.INFORMATION_SCHEMA.VIEWS
            """
            for row in client.query(views_query).result():
                view_info = dict(row)
                extracted_metadata["views"].append(view_info)

                # Simple parsing of view_definition for dependencies (can be complex)
                # In a real scenario, consider a more robust SQL parser or Data Catalog.
                view_def = view_info['view_definition'].lower()
                for table in extracted_metadata["tables"]:
                    full_table_name = f"`{table['table_catalog']}.{table['table_schema']}.{table['table_name']}`".lower()
                    if full_table_name in view_def:
                        extracted_metadata["table_dependencies"].append({
                            "source": full_table_name,
                            "target": f"`{view_info['table_catalog']}.{view_info['table_schema']}.{view_info['table_name']}`".lower(),
                            "type": "READS_FROM_VIEW_DEFINITION"
                        })
                for other_view in extracted_metadata["views"]:
                    if other_view['table_name'] != view_info['table_name']: # Avoid self-reference
                        full_other_view_name = f"`{other_view['table_catalog']}.{other_view['table_schema']}.{other_view['table_name']}`".lower()
                        if full_other_view_name in view_def:
                            extracted_metadata["table_dependencies"].append({
                                "source": full_other_view_name,
                                "target": f"`{view_info['table_catalog']}.{view_info['table_schema']}.{view_info['table_name']}`".lower(),
                                "type": "READS_FROM_VIEW_DEFINITION"
                            })

            # --- Extract Routines (Stored Procedures/Functions) ---
            routines_query = f"""
                SELECT
                    specific_catalog,
                    specific_schema,
                    specific_name,
                    routine_type,
                    routine_definition,
                    creation_time
                FROM
                    `{project_id}`.`{dataset_id}`.INFORMATION_SCHEMA.ROUTINES
            """
            for row in client.query(routines_query).result():
                routine_info = dict(row)
                extracted_metadata["routines"].append(routine_info)
                # Similar to views, routine_definition can be parsed for dependencies
                # This is a placeholder for more advanced parsing.
                routine_def = (routine_info.get('routine_definition') or '').lower()
                for table in extracted_metadata["tables"]:
                    full_table_name = f"`{table['table_catalog']}.{table['table_schema']}.{table['table_name']}`".lower()
                    if full_table_name in routine_def:
                        extracted_metadata["table_dependencies"].append({
                            "source": full_table_name,
                            "target": f"`{routine_info['specific_catalog']}.{routine_info['specific_schema']}.{routine_info['specific_name']}`".lower(),
                            "type": "READS_FROM_ROUTINE_DEFINITION"
                        })


            # --- Extract Cross-Dataset Access (Simplified via IAM) ---
            # This is a simplified approach. A comprehensive solution would
            # involve analyzing IAM policies at project and dataset levels
            # using google.cloud.iam.admin_v1 and resource manager APIs.
            # Here we demonstrate checking dataset access for current dataset.
            try:
                dataset = client.get_dataset(full_dataset_id)
                access_entries = dataset.access_entries
                for entry in access_entries:
                    if entry.entity_type and entry.entity_id and entry.role:
                        extracted_metadata["cross_dataset_access"].append({
                            "dataset": full_dataset_id,
                            "entity_type": entry.entity_type,
                            "entity_id": entry.entity_id,
                            "role": entry.role,
                            "notes": "Inferred from BigQuery Dataset Access Controls"
                        })
            except Exception as e:
                print(f"Warning: Could not get access entries for {full_dataset_id}: {e}")

    # --- Extract ETL Job Audits (from a hypothetical BigQuery table) ---
    # This assumes a dedicated `etl_job_audit` table exists in BigQuery.
    # Its schema should ideally mirror the legacy Oracle ETL_JOB_AUDIT table.
    audit_table_full_name = f"`{DEFAULT_GCP_PROJECT}.{DEFAULT_AUDIT_DATASET}.{DEFAULT_AUDIT_TABLE}`"
    audit_query = f"""
        SELECT
            job_id,
            job_name,
            start_time,
            end_time,
            status,
            source_table,
            target_table,
            workflow_name
        FROM
            {audit_table_full_name}
        WHERE
            DATE(start_time) = PARSE_DATE('%Y-%m-%d', @run_date)
    """
    try:
        query_params = [
            bigquery.ScalarQueryParameter("run_date", "STRING", run_date)
        ]
        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        for row in client.query(audit_query, job_config=job_config).result():
            extracted_metadata["etl_job_audits"].append(dict(row))
    except Exception as e:
        print(f"Warning: Could not query ETL job audit table {audit_table_full_name}: {e}")
        print("Please ensure the audit table exists and has the correct schema/permissions.")

    return extracted_metadata

def build_lineage_graph(metadata: dict) -> dict:
    """
    Constructs the lineage graph from extracted BigQuery metadata.
    This involves processing `lineage_chains` and integrating discovered dependencies.

    Args:
        metadata: Dictionary containing extracted BigQuery metadata.

    Returns:
        A dictionary representing the lineage graph with nodes and edges.
    """
    nodes = {}
    edges = []

    # --- Redesigned Lineage Chains for GCP ---
    # This is a CRITICAL REDESIGN AREA. These chains define high-level ETL patterns
    # specific to the GCP environment. They replace the legacy Oracle-centric chains.
    # In a production system, these would likely come from an external configuration
    # (e.g., YAML file in GCS, BigQuery config table) and be more dynamic.
    gcp_lineage_chains = [
        {
            "name": "Cloud Composer DAG -> Dataflow Job -> BigQuery Table",
            "components": [
                {"type": "Composer DAG", "identifier_field": "job_name_prefix"},
                {"type": "Dataflow Job", "identifier_field": "job_name_prefix"},
                {"type": "BigQuery Table", "identifier_field": "table_name_suffix"}
            ],
            "pattern": [
                {"source": "Composer DAG", "target": "Dataflow Job", "relation": "INVOKES"},
                {"source": "Dataflow Job", "target": "BigQuery Table", "relation": "WRITES_TO"}
            ]
        },
        {
            "name": "BigQuery Stored Procedure -> BigQuery Table",
            "components": [
                {"type": "BigQuery Stored Procedure", "identifier_field": "routine_name_prefix"},
                {"type": "BigQuery Table", "identifier_field": "table_name_suffix"}
            ],
            "pattern": [
                {"source": "BigQuery Stored Procedure", "target": "BigQuery Table", "relation": "WRITES_TO"}
            ]
        },
        {
            "name": "dbt Model -> BigQuery View",
            "components": [
                {"type": "dbt Model", "identifier_field": "dbt_model_name"},
                {"type": "BigQuery View", "identifier_field": "view_name_suffix"}
            ],
            "pattern": [
                {"source": "dbt Model", "target": "BigQuery View", "relation": "GENERATES"}
            ]
        }
        # Add more GCP-specific lineage chains as needed
    ]

    # Add BigQuery tables, views, and routines as nodes
    for table in metadata["tables"]:
        table_id = f"BQ_TABLE:{table['table_catalog']}.{table['table_schema']}.{table['table_name']}"
        nodes[table_id] = {
            "name": table['table_name'],
            "type": "BigQuery Table",
            "full_path": f"{table['table_catalog']}.{table['table_schema']}.{table['table_name']}",
            "domain": _schema_to_domain(table['table_catalog'], table['table_schema']),
            "attributes": table
        }

    for view in metadata["views"]:
        view_id = f"BQ_VIEW:{view['table_catalog']}.{view['table_schema']}.{view['table_name']}"
        nodes[view_id] = {
            "name": view['table_name'],
            "type": "BigQuery View",
            "full_path": f"{view['table_catalog']}.{view['table_schema']}.{view['table_name']}",
            "domain": _schema_to_domain(view['table_catalog'], view['table_schema']),
            "attributes": view
        }

    for routine in metadata["routines"]:
        routine_id = f"BQ_ROUTINE:{routine['specific_catalog']}.{routine['specific_schema']}.{routine['specific_name']}"
        nodes[routine_id] = {
            "name": routine['specific_name'],
            "type": f"BigQuery {routine['routine_type']}",
            "full_path": f"{routine['specific_catalog']}.{routine['specific_schema']}.{routine['specific_name']}",
            "domain": _schema_to_domain(routine['specific_catalog'], routine['specific_schema']),
            "attributes": routine
        }

    # Add inferred table dependencies (views reading tables)
    for dep in metadata["table_dependencies"]:
        source_node_id = None
        target_node_id = None

        # Resolve source node (table or view)
        for node_id, node_data in nodes.items():
            if node_data['full_path'].lower() == dep['source'].strip('`').lower():
                source_node_id = node_id
                break
        
        # Resolve target node (view or routine)
        for node_id, node_data in nodes.items():
            if node_data['full_path'].lower() == dep['target'].strip('`').lower():
                target_node_id = node_id
                break

        if source_node_id and target_node_id:
            edges.append({
                "source": source_node_id,
                "target": target_node_id,
                "type": dep['type'],
                "evidence": "Inferred from BigQuery Information Schema"
            })
        else:
            print(f"Warning: Could not resolve dependency nodes for {dep['source']} -> {dep['target']}")


    # Process ETL Job Audits to create job nodes and their relationships
    for audit in metadata["etl_job_audits"]:
        job_node_id = f"ETL_JOB:{audit['job_id']}"
        nodes[job_node_id] = {
            "name": audit['job_name'],
            "type": "ETL Job",
            "job_id": audit['job_id'],
            "status": audit['status'],
            "workflow": audit.get('workflow_name', 'unknown'),
            "start_time": audit['start_time'].isoformat() if isinstance(audit['start_time'], datetime) else audit['start_time'],
            "end_time": audit['end_time'].isoformat() if isinstance(audit['end_time'], datetime) else audit['end_time'],
            "attributes": audit
        }

        # Link audit jobs to source and target tables
        if audit.get("source_table"):
            source_parts = audit["source_table"].split('.')
            if len(source_parts) == 3: # Assuming project.dataset.table format
                source_table_id = f"BQ_TABLE:{source_parts[0]}.{source_parts[1]}.{source_parts[2]}"
                if source_table_id in nodes:
                    edges.append({
                        "source": source_table_id,
                        "target": job_node_id,
                        "type": "READS",
                        "evidence": "From ETL Job Audit"
                    })
                else:
                    print(f"Warning: Source table {audit['source_table']} not found in nodes for job {audit['job_id']}")

        if audit.get("target_table"):
            target_parts = audit["target_table"].split('.')
            if len(target_parts) == 3: # Assuming project.dataset.table format
                target_table_id = f"BQ_TABLE:{target_parts[0]}.{target_parts[1]}.{target_parts[2]}"
                if target_table_id in nodes:
                    edges.append({
                        "source": job_node_id,
                        "target": target_table_id,
                        "type": "WRITES",
                        "evidence": "From ETL Job Audit"
                    })
                else:
                    print(f"Warning: Target table {audit['target_table']} not found in nodes for job {audit['job_id']}")

    # Apply GCP-specific lineage chains (conceptual application)
    # This section would contain complex logic to match audit trails and metadata
    # against the defined patterns in 'gcp_lineage_chains' to create higher-level
    # lineage flows. For this example, it's largely illustrative.
    for chain in gcp_lineage_chains:
        # Example: Link Dataflow Jobs from audit to Composer DAGs or BigQuery SPs
        # This part requires more advanced heuristic matching or explicit tags
        # in job names/descriptions.
        pass


    # Final graph structure
    lineage_graph = {
        "metadata_extracted_at": datetime.now().isoformat(),
        "nodes": list(nodes.values()),
        "edges": edges
    }

    return lineage_graph

def save_lineage_to_gcs(storage_client: storage.Client, lineage_graph: dict, gcs_bucket: str, gcs_prefix: str, run_date: str):
    """
    Saves the generated lineage graph to a JSON file in Google Cloud Storage.
    """
    bucket = storage_client.bucket(gcs_bucket)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"{gcs_prefix}/lineage_CRM_WORKFLOW_{run_date.replace('-', '')}_{timestamp}.json"
    blob = bucket.blob(file_name)

    json_data = json.dumps(lineage_graph, indent=2)
    blob.upload_from_string(json_data, content_type="application/json")

    print(f"Lineage graph successfully uploaded to gs://{gcs_bucket}/{file_name}")

def parse_args():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate BigQuery Lineage Graph for CRM workflows."
    )
    parser.add_argument(
        "--gcp_projects",
        nargs="+",
        default=[DEFAULT_GCP_PROJECT],
        help="List of GCP Project IDs to scan for BigQuery metadata."
    )
    parser.add_argument(
        "--bigquery_datasets",
        nargs="+",
        default=DEFAULT_BIGQUERY_DATASETS,
        help="List of BigQuery Dataset IDs to scan within each project."
    )
    parser.add_argument(
        "--run_date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Date for which to extract ETL job audit data (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--output_gcs_bucket",
        default=DEFAULT_OUTPUT_GCS_BUCKET,
        help="Google Cloud Storage bucket name for output JSON."
    )
    parser.add_argument(
        "--output_gcs_prefix",
        default=DEFAULT_OUTPUT_GCS_PREFIX,
        help="GCS prefix/folder path within the bucket for output JSON."
    )
    return parser.parse_args()

def main():
    args = parse_args()

    print("Initializing BigQuery and GCS clients...")
    bq_client = get_bigquery_client()
    gcs_client = get_storage_client()
    print("Clients initialized.")

    print(f"Starting metadata extraction for run date: {args.run_date}")
    metadata = extract_metadata_from_bigquery(
        bq_client, args.gcp_projects, args.bigquery_datasets, args.run_date
    )
    print("Metadata extraction complete.")
    # print(json.dumps(metadata, indent=2)) # Uncomment for debugging extracted metadata

    print("Building lineage graph...")
    lineage_graph = build_lineage_graph(metadata)
    print("Lineage graph built.")
    # print(json.dumps(lineage_graph, indent=2)) # Uncomment for debugging lineage graph

    print("Saving lineage graph to GCS...")
    save_lineage_to_gcs(
        gcs_client, lineage_graph, args.output_gcs_bucket, args.output_gcs_prefix, args.run_date
    )
    print("Lineage tracking complete.")

if __name__ == "__main__":
    main()