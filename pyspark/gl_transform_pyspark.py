# CRM_ABINITIO_TRANSFORM - PySpark General Ledger Transformation
# Legacy Source: finance/gl_transform.xfr

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

def main():
    parser = argparse.ArgumentParser(description="PySpark GL Transformation Job")
    parser.add_argument("--period_name", type=str, help="The period name for GL processing (e.g., YYYYMM)", required=True)
    parser.add_argument("--entity_code", type=str, help="The entity code for GL processing", required=True)
    args = parser.parse_args()

    period_name = args.period_name
    entity_code = args.entity_code

    spark = SparkSession.builder \
        .appName("Finance_GL_Transformation") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0") \
        .getOrCreate()

    spark.conf.set("parentProject", spark.sparkContext.getConf().get("spark.hadoop.google.cloud.project.id"))
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY") # For older date formats if encountered

    # 1. Read raw GL transactions from staging
    # Equivalent to Ab Initio component "read_stg_gl"
    stg_gl_transactions_df = spark.read.format("bigquery") \
        .option("table", "stg_finance.stg_gl_transactions") \
        .load() \
        .filter(F.col("period_name") == F.lit(period_name)) \
        .filter(F.col("entity_code") == F.lit(entity_code)) \
        .filter(F.col("ETL_STATUS") == "PENDING")

    # 2. Read Account Dimension lookup
    # Equivalent to Ab Initio component "read_dim_account"
    dim_account_df = spark.read.format("bigquery") \
        .option("table", "stg_finance.dim_account") \
        .load() \
        .filter(F.col("IS_CURRENT") == "Y") \
        .filter(F.col("entity_code") == F.lit(entity_code)) \
        .select(
            F.col("account_code").alias("dim_account_code"), # Alias to prevent join key conflict
            F.col("entity_code").alias("dim_entity_code"),
            F.col("account_name"),
            F.col("account_type"),
            # Assuming account_subtype, cost_centre_name, department_code exist in dim_account
            F.col("account_subtype"),
            F.col("cost_centre_name"),
            F.col("department_code")
        )

    # 3. Read Exchange Rates (Note: Ab Initio XFR only reads, but doesn't apply conversion logic directly)
    # Equivalent to Ab Initio component "read_exchange_rates"
    # Although read, the provided Ab Initio XFR does not show explicit join or application of FX rates
    # in the reformat_normalise component's transform block.
    # Therefore, FX conversion logic is not implemented in PySpark for now, matching the XFR's detail.
    # If explicit FX conversion is needed, this section would require more logic.
    stg_period_rates_df = spark.read.format("bigquery") \
        .option("table", "stg_finance.stg_period_rates") \
        .load() \
        .filter(F.col("period_name") == F.lit(period_name)) \
        .filter(F.col("currency_to") == "GBP") # Assuming GBP is the target functional currency

    # 4. Reformat - Normalise and derive fields before join
    # Equivalent to Ab Initio component "reformat_normalise"
    normalised_gl_df = stg_gl_transactions_df.withColumn(
        "entity_code", F.upper(F.trim(F.col("entity_code")))
    ).withColumn(
        "account_code", F.upper(F.trim(F.col("account_code")))
    ).withColumn(
        "cost_centre_code", F.upper(F.trim(F.coalesce(F.col("cost_centre_code"), F.lit("DEFAULT"))))
    ).withColumn(
        "debit_amount", F.abs(F.col("debit_amount"))
    ).withColumn(
        "credit_amount", F.abs(F.col("credit_amount"))
    ).withColumn(
        "currency_code", F.coalesce(F.col("currency"), F.lit("GBP")) # Assuming 'currency' column maps to currency_code
    ).withColumn(
        "exchange_rate", F.coalesce(F.col("exchange_rate"), F.lit(1.0)) # Placeholder, as XFR didn't join rates
    ).withColumn(
        "journal_type",
        F.when(F.col("journal_type").isin("MANUAL", "ACCRUAL"), F.lit("ADJUSTING"))
         .when(F.col("journal_type") == "REVERSAL", F.lit("REVERSING"))
         .when(F.col("journal_type") == "INTERCO", F.lit("INTERCOMPANY"))
         .otherwise(F.lit("STANDARD"))
    ).select(
        F.col("transaction_id").alias("gl_txn_id"),
        F.col("gl_date").alias("txn_date"), # Using gl_date as txn_date
        F.col("entity_code"),
        F.col("period_name"),
        F.lit(period_name[:4]).cast(T.IntegerType()).alias("period_year"), # Derive from period_name YYYYMM
        F.lit(period_name[4:]).cast(T.IntegerType()).alias("period_num"), # Derive from period_name YYYYMM
        F.col("account_code"),
        F.col("cost_centre_code"),
        F.col("description"),
        F.col("debit_amount"),
        F.col("credit_amount"),
        (F.col("debit_amount") - F.col("credit_amount")).alias("net_amount"), # Calculated net_amount
        F.col("functional_amount"), # Functional amount as is from source per XFR
        F.col("currency_code"),
        F.col("exchange_rate"),
        F.col("journal_type"),
        F.col("source_system").alias("journal_source") # Assuming source_system maps to journal_source
    )


    # 5. Join GL to Account Dimension
    # Equivalent to Ab Initio component "join_account_dim"
    joined_gl_base_df = normalised_gl_df.alias("gl") \
        .join(dim_account_df.alias("dim"),
              (F.col("gl.account_code") == F.col("dim.dim_account_code")) &
              (F.col("gl.entity_code") == F.col("dim.dim_entity_code")),
              "left_outer")

    joined_gl_df = joined_gl_base_df.filter(F.col("dim.dim_account_code").isNotNull()) \
        .select(
            F.col("gl.gl_txn_id"),
            F.col("gl.entity_code"),
            F.col("gl.period_name"),
            F.col("gl.period_year"),
            F.col("gl.period_num"),
            F.col("gl.account_code"),
            F.coalesce(F.col("dim.account_name"), F.lit("UNMAPPED")).alias("account_name"),
            F.coalesce(F.col("dim.account_type"), F.lit("UNKNOWN")).alias("account_type"),
            F.coalesce(F.col("dim.account_subtype"), F.lit("UNKNOWN")).alias("account_subtype"),
            F.col("gl.cost_centre_code"),
            F.coalesce(F.col("dim.cost_centre_name"), F.lit("DEFAULT")).alias("cost_centre_name"),
            F.coalesce(F.col("dim.department_code"), F.lit("NONE")).alias("department_code"),
            F.col("gl.debit_amount"),
            F.col("gl.credit_amount"),
            F.col("gl.net_amount"),
            F.col("gl.functional_amount"),
            F.col("gl.currency_code"),
            F.col("gl.exchange_rate"),
            F.col("gl.txn_date"),
            F.col("gl.journal_type"),
            F.col("gl.description")
        )

    unmatched_gl_df = joined_gl_base_df.filter(F.col("dim.dim_account_code").isNull()) \
        .select(
            F.col("gl.gl_txn_id"),
            F.col("gl.entity_code"),
            F.col("gl.period_name"),
            F.col("gl.account_code"),
            F.col("gl.debit_amount"),
            F.col("gl.credit_amount"),
            F.col("gl.journal_type"),
            F.col("gl.description"),
            F.lit("Account Dimension Mismatch").alias("error_reason")
        )

    # 6. Filter - Separate adjusting journals from standard
    # Equivalent to Ab Initio component "filter_by_journal_type"
    standard_journals_df = joined_gl_df.filter(F.col("journal_type").isin("STANDARD", "INTERCOMPANY"))
    adjusting_journals_df = joined_gl_df.filter(F.col("journal_type").isin("ADJUSTING", "REVERSING"))

    # 7. Rollup - Aggregate to account/period level
    # Equivalent to Ab Initio component "rollup_period_balances"
    # Assuming rollup for standard_journals_df for now, as per graph flow
    period_balance_summary_df = standard_journals_df.groupBy(
        "entity_code", "period_name", "period_year", "period_num",
        "account_code", "account_name", "account_type",
        "cost_centre_code", "currency_code"
    ).agg(
        F.lit(0.0).alias("opening_balance"), # Initialise opening_balance to 0.0 as per Ab Initio XFR
        F.sum("debit_amount").alias("period_debits"),
        F.sum("credit_amount").alias("period_credits"),
        F.sum("net_amount").alias("net_period_amount"), # This is closing_balance before calculation
        F.sum("functional_amount").alias("functional_balance"),
        F.count("gl_txn_id").alias("transaction_count")
    ).withColumn(
        "closing_balance",
        F.col("opening_balance") + F.col("period_debits") - F.col("period_credits")
    ).withColumn(
        "balance_type", F.lit("ACTUAL") # Assuming "ACTUAL" as default balance type
    ).withColumn(
        "last_updated", F.current_timestamp()
    ).select(
        "account_code",
        "entity_code",
        "period_name",
        "balance_type",
        "opening_balance",
        "period_debits",
        "period_credits",
        "closing_balance",
        "transaction_count",
        "last_updated"
    )

    # 8. Write to FACT_GL_BALANCES
    # Equivalent to Ab Initio component "write_fact_gl_balances"
    period_balance_summary_df.write.format("bigquery") \
        .option("table", "finance_warehouse.fact_gl_balances") \
        .mode("append") \
        .save()
    print(f"Successfully wrote FACT_GL_BALANCES for {period_name}, {entity_code}")

    # 9. Write unmatched GL lines to error output GCS
    # Equivalent to Ab Initio component "write_unmatched_gl"
    gcs_unmatched_gl_path = f"gs://finance_errors/finance_unmatched_gl_{entity_code}_{period_name}.csv"
    unmatched_gl_df.write.format("csv") \
        .option("delimiter", "|") \
        .mode("overwrite") \
        .save(gcs_unmatched_gl_path)
    print(f"Successfully wrote unmatched GL file to {gcs_unmatched_gl_path}")

    spark.stop()

if __name__ == "__main__":
    main()