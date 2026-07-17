from pyspark.sql import SparkSession

def main():
    # Instantiate SparkSession to match Dataproc pattern
    spark = SparkSession.builder \
        .appName("DW.DWH_DUMMY_ABSD_PLATO_TARIFE") \
        .getOrCreate()
    
    # Exact reproduction of original output log
    print("Doing nothinig")
    
    spark.stop()

if __name__ == "__main__":
    main()