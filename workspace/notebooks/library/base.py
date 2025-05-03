import os
from pyspark.sql import SparkSession

def get_spark_context():
    return SparkSession.builder \
        .appName("ClientsAnalysis") \
        .getOrCreate()

def write_parquet(name, df):
    home_dir = os.getenv('HOME_DIR')
    parquet_file = f'{home_dir}/parkets/{name}.parquet'
    df.write.mode("overwrite").parquet(parquet_file)

def load_csv_file(filename, destiny_name="base"):
    spark = get_spark_context()
    csv_file_path = filename
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    df.printSchema()
    df.show()
    write_parquet(destiny_name, df)
    spark.stop()

def read_parquet(spark, file_name):
    home_dir = os.getenv('HOME_DIR')
    return spark.read.parquet(f"{home_dir}/parkets/{file_name}.parquet")