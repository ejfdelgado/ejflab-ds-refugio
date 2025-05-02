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

def load_csv_file():
    spark = get_spark_context()
    # Path to your CSV file
    csv_file_path = "path/to/your/file.csv"

    # Load CSV into DataFrame
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

    # Show the first few rows
    df.show()