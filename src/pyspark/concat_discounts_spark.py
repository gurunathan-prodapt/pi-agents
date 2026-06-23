# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
#
# PySpark script to replicate the Oracle pipelined function 'concat_discounts' logic.
# Reads from BigQuery SOF_TA_DISCOUNT, groups discounts by contract, and concatenates
# them into a single string with a length limit, then writes to a staging table.

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, concat_ws, collect_list, lit
from pyspark.sql.types import StringType

# UDF to concatenate a list of strings with a character length limit.
# It simulates the Oracle logic of concatenating until a certain length is reached.
@udf(StringType())
def concat_with_limit_udf(rabatt_list):
    """
    Concatenates a list of strings into a single string,
    stopping when the total length exceeds 500 characters (including separators).
    """
    if not rabatt_list:
        return None

    current_result_parts = []
    current_length = 0

    for item in rabatt_list:
        if not item:
            continue

        # If it's not the first item, a separator ", " will be added
        potential_addition_length = len(item)
        if current_result_parts:
            potential_addition_length += len(", ") # account for separator

        if current_length + potential_addition_length <= 500:
            current_result_parts.append(item)
            current_length += potential_addition_length
        else:
            # Adding this item would exceed the limit, so stop
            break
            
    return ", ".join(current_result_parts) if current_result_parts else None

def main():
    if len(sys.argv) != 6:
        print("Usage: spark-submit concat_discounts_spark.py <project_id> <source_dataset> <source_table> <staging_dataset> <staging_table>")
        sys.exit(1)

    project_id = sys.argv[1]
    source_dataset = sys.argv[2]
    source_table_name = sys.argv[3]
    staging_dataset = sys.argv[4]
    staging_table_name = sys.argv[5]

    source_bq_table = f"{project_id}.{source_dataset}.{source_table_name}"
    staging_bq_table = f"{project_id}.{staging_dataset}.{staging_table_name}"

    spark = SparkSession.builder \
        .appName("ConcatDiscountsPipelinedFunction") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0") \
        .getOrCreate()

    print(f"Reading data from BigQuery table: {source_bq_table}")
    df_discount = spark.read \
        .format("bigquery") \
        .option("table", source_bq_table) \
        .load()

    # Create the formatted discount string: rabatt || ' (' || rabatthoehe || '%)'
    # The original Oracle query uses DISTINCT inside the CURSOR subquery for the pipelined function.
    # We should apply this distinct logic here.
    df_formatted_discounts = df_discount.select(
        col("cntrct_id"),
        col("cntrct_obj_version"),
        concat_ws(lit(" "),
                  col("rabatt"),
                  concat_ws(lit(""), lit("("), col("rabatthoehe"), lit("%)"))
                 ).alias("rabatt_formatted")
    ).distinct() # Apply DISTINCT as per Oracle CURSOR subquery

    # Group by contract ID and version, collect all formatted discounts into a list
    df_grouped = df_formatted_discounts.groupBy("cntrct_id", "cntrct_obj_version") \
                                     .agg(collect_list("rabatt_formatted").alias("rabatt_list"))

    # Apply UDF to concatenate with length limit
    df_final_rabatt = df_grouped.withColumn("rabatt_alle", concat_with_limit_udf(col("rabatt_list"))) \
                               .select("cntrct_id", "cntrct_obj_version", "rabatt_alle")

    print(f"Writing aggregated data to BigQuery staging table: {staging_bq_table}")
    # Write the result to the staging BigQuery table
    df_final_rabatt.write \
        .format("bigquery") \
        .option("table", staging_bq_table) \
        .option("temporaryGcsBucket", "your-gcs-staging-bucket") # Replace with a real GCS bucket
        .mode("overwrite") \
        .save()

    spark.stop()
    print("PySpark job completed successfully.")

if __name__ == "__main__":
    main()