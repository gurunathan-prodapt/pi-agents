# Legacy Source: d_ausd_v_ta_disc_zusgf.sql
# Job: BERT_V_TA_DISC_ZUSGF
import pandas as pd
from google.cloud import bigquery
import os

def concat_discounts_logic(group):
    """
    Replicates the PL/SQL concat_discounts pipelined function logic.
    Concatenates 'rabatt' values for a group, respecting a 500-character limit.
    """
    results = []
    current_rabatt_alle = ""
    # Ensure consistent ordering as in legacy system
    group_sorted = group.sort_values(by='rabatt')

    for _, row in group_sorted.iterrows():
        rabatt_item = str(row['rabatt'])
        if not current_rabatt_alle:
            # First item in a new concatenation string
            current_rabatt_alle = rabatt_item
        else:
            # Check if adding the next item exceeds the 500-char limit
            temp_concat = current_rabatt_alle + ', ' + rabatt_item
            if len(temp_concat) > 500:
                # Store current accumulated string, start a new one
                results.append({
                    'cntrct_id': row['cntrct_id'],
                    'cntrct_obj_version': row['cntrct_obj_version'],
                    'rabatt_alle': current_rabatt_alle,
                    'disc_vector_ty': row['disc_vector_ty'] # Assuming this is passed through per group/row
                })
                current_rabatt_alle = rabatt_item # Start new accumulation with the current item
            else:
                current_rabatt_alle = temp_concat
    
    # Add the last accumulated string if not empty
    if current_rabatt_alle:
        # Assuming we can take 'disc_vector_ty' from any row in the group
        # For simplicity, taking from the first row of the original group
        first_row = group.iloc[0] 
        results.append({
            'cntrct_id': first_row['cntrct_id'],
            'cntrct_obj_version': first_row['cntrct_obj_version'],
            'rabatt_alle': current_rabatt_alle,
            'disc_vector_ty': first_row['disc_vector_ty']
        })
    return pd.DataFrame(results)


def transform_and_load_discount_data(
    project_id: str,
    dataset_id: str,
    target_table_name: str,
    source_discount_table: str,
    source_date_table: str
):
    """
    Orchestrates the transformation and loading of discount data into BigQuery.
    """
    client = bigquery.Client(project=project_id)

    # 1. Get v_datum equivalent from dwtk_meldungen
    # Assuming v_datum is the latest 'dat_gueltig_ab' from dwtk_meldungen where 'kenn_meldungsart' is 'DISCOUNT'
    # The original design mentioned 'v_datum' from 'isbert_schema.dwtk_meldungen'.
    # For now, let's assume it's simply a date we get from the table.
    # The design document for the DAG implies a dedicated task for `get_sysdate_equivalent_task`.
    # For this script, we can either receive it as a parameter, or query it ourselves.
    # Let's query it directly for self-containment, but prioritize a parameter if available.

    # Example query for v_datum (adjust as per actual `dwtk_meldungen` schema and logic)
    # The design says "READS FROM: project_id.dataset_id.dwtk_meldungen (BigQuery table). OUTPUTS: A processing date (equivalent to v_datum) via XCom."
    # So, we should *expect* to receive this date. For standalone testing, we'll use a placeholder.
    # For now, let's assume `v_datum` isn't strictly used in the `concat_discounts` part but
    # might be used for filtering the source data. The design only says `v_datum` is read
    # by the PL/SQL script but doesn't explicitly state its role in `concat_discounts` filtering.
    # If it was for filtering, it would be passed to `p_v_datum` parameter of `concat_discounts`.
    # Without that, we proceed with reading all relevant discounts.

    # 2. Read data from sof_ta_discount
    query = f"""
    SELECT
        cntrct_id,
        cntrct_obj_version,
        rabatt,
        disc_vector_ty
    FROM
        `{project_id}.{dataset_id}.{source_discount_table}`
    ORDER BY
        cntrct_id,
        cntrct_obj_version,
        rabatt -- Ensure consistent ordering for concatenation
    """
    df_discounts = client.query(query).to_dataframe()

    if df_discounts.empty:
        print("No discount data to process.")
        # Ensure the target table is still cleared or handled appropriately if no data
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        dummy_df = pd.DataFrame(columns=['cntrct_id', 'cntrct_obj_version', 'rabatt_alle', 'disc_vector_ty'])
        client.load_table_from_dataframe(dummy_df, f"`{project_id}.{dataset_id}.{target_table_name}`", job_config=job_config).result()
        return

    # 3. Apply discount concatenation logic
    # Group by contract and apply the custom concatenation function
    transformed_df = df_discounts.groupby(['cntrct_id', 'cntrct_obj_version'], as_index=False).apply(concat_discounts_logic)
    
    # Reset index and clean up if apply() results in multi-index
    transformed_df = transformed_df.reset_index(drop=True)

    # Ensure column order and types match target table, BigQuery is schema-strict
    # Define the schema for the target table explicitly if not inferred correctly
    # from the dataframe, or ensure the dataframe matches the target schema.
    # Assuming target table `sof_ta_disc_zusgf` has columns:
    # `cntrct_id` (STRING), `cntrct_obj_version` (INTEGER), `rabatt_alle` (STRING), `disc_vector_ty` (STRING)
    transformed_df['cntrct_id'] = transformed_df['cntrct_id'].astype(str)
    transformed_df['cntrct_obj_version'] = transformed_df['cntrct_obj_version'].astype(int)
    transformed_df['rabatt_alle'] = transformed_df['rabatt_alle'].astype(str)
    transformed_df['disc_vector_ty'] = transformed_df['disc_vector_ty'].astype(str) # Assuming this is a string type

    # 4. Load data into sof_ta_disc_zusgf
    table_id = f"{project_id}.{dataset_id}.{target_table_name}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") # Truncate and load
    
    job = client.load_table_from_dataframe(transformed_df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Loaded {job.output_rows} rows into {table_id}")

if __name__ == "__main__":
    # Example usage - replace with actual project_id and dataset_id
    # These would typically come from Airflow environment variables or task parameters
    PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'your-gcp-project-id')
    DATASET_ID = os.environ.get('BQ_DATASET_ID', 'your_bigquery_dataset')
    TARGET_TABLE = 'sof_ta_disc_zusgf'
    SOURCE_DISCOUNT_TABLE = 'sof_ta_discount'
    SOURCE_DATE_TABLE = 'dwtk_meldungen' # Not directly used in current concat logic, but kept for context

    print(f"Starting transformation for project: {PROJECT_ID}, dataset: {DATASET_ID}")
    transform_and_load_discount_data(PROJECT_ID, DATASET_ID, TARGET_TABLE, SOURCE_DISCOUNT_TABLE, SOURCE_DATE_TABLE)
    print("Transformation completed.")