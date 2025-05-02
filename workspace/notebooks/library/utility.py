import os
import datetime
import pandas as pd
import pyarrow as pa
import mysql.connector
import pyarrow.parquet as pq
import pyspark.sql.functions as F
from mysql.connector import Error
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, BooleanType, ByteType, ShortType, IntegerType, FloatType, DoubleType
from pyspark.sql.window import Window

def get_spark_context():
    return SparkSession.builder \
        .appName("ClientsAnalysis") \
        .getOrCreate()

def write_parquet(name, df):
    home_dir = os.getenv('HOME_DIR')
    parquet_file = f'{home_dir}/parkets/{name}.parquet'
    df.write.mode("overwrite").parquet(parquet_file)

def load_csv_file():
    spark = get_spark_context()
    csv_file_path = "./data/RESERVAS Y CONSUMO EL REFUGIO HOSTEL Actual.xlsx - HOSPEDAJE & CONSUMO.csv"
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    df.printSchema()
    df.show()

    write_parquet("base", df)
    
    spark.stop()