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

def create():
    spark = get_spark_context()
    data = [("martes, octubre 08, 2019",), ("jueves, diciembre 05, 2019",)]
    df = spark.createDataFrame(data, ["fecha_str"])
    spark.stop()