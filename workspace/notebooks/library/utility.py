import os
import re
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

month_map = {
    "enero": "January",
    "febrero": "February",
    "marzo": "March",
    "abril": "April",
    "mayo": "May",
    "junio": "June",
    "julio": "July",
    "agosto": "August",
    "septiembre": "September",
    "octubre": "October",
    "noviembre": "November",
    "diciembre": "December"
}

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

def read_parquet(spark, file_name):
    home_dir = os.getenv('HOME_DIR')
    return spark.read.parquet(f"{home_dir}/parkets/{file_name}.parquet")

def rename_columns():
    spark = get_spark_context()
    df = read_parquet(spark, "base")
    df.printSchema()

    df = df.drop("_c16")

    # Renaming
    name_map = {
        "NOMBRE ": "names",
        "Cómo se enteró de El Refugio?": "channel",
        "RESERVA POR BOOKING.COM comisión 14% (SI/NO)": "booking_fee",
        "No. DE PERSONAS & Manillas":"pax",
        "FECHA DE LLEGADA o CONSUMO":"date_start",
        "FECHA DE SALIDA":"date_end",
        "No. DE NOCHES":"nights",
        "TOTAL A PAGAR EN HOSPEDAJE / PASADIA":"price",
        " PAGÓ RESERVA 50% O ABONO (si/no)":"half_deposit",# Use intenger
        "UBICACIÓN":"room",
        "CONSUMO ":"meals",
        " TOTAL CONSUMO":"meal_price",
        " PAGO EN ENFECTIVO":"pay_cash",
        " PAGO EN DIGITAL":"pay_online",
        "OBSERVACIONES / PROPINAS":"tip_notes",
        "EGRESOS":"expenses",
    }
    for key in name_map:
        df = df.withColumnRenamed(key, name_map[key])

    df.printSchema()

    write_parquet("base_renamed", df)

    spark.stop()

def parse_integer(number_str):
    if number_str is not None:
        match = re.search(r"^\s*([\d]+)\s*$", number_str)
        if match:
            return int(match.group(1))
    return None

def parse_spanish_date(date_str):
    if date_str is not None:
        # Formats:
        # martes, octubre 08, 2019 - 
        match1 = re.search(r"^([^,]+),\s*([^\s]+)\s*(\d+),\s*(\d*)$", date_str)
        if match1:
            month_day = match1.group(2)
            month = month_map.get(month_day.lower())
            day = match1.group(3)
            year = match1.group(4)
            formatted = f"{month} {day}, {year}"
            return datetime.datetime.strptime(formatted, "%B %d, %Y").date()
        else:
            # sábado, 28 de diciembre de 2019
            match2 = re.search(r"^([^,]+),\s*(\d+)\s+de\s+([^\s]+)\s+de\s+([\d]+)$", date_str)
            if match2:
                month_day = match2.group(3)
                month = month_map.get(month_day.lower())
                day = match2.group(2)
                year = match2.group(4)
                formatted = f"{month} {day}, {year}"
                return datetime.datetime.strptime(formatted, "%B %d, %Y").date()
    return None

def parse_dates():
    spark = get_spark_context()
    df = read_parquet(spark, "base_renamed")

    date_parse_udf = F.udf(parse_spanish_date, DateType())
    number_udf = F.udf(parse_integer, ShortType())
    df = df.withColumn("date_start_p", date_parse_udf(df["date_start"]))
    df = df.withColumn("date_end_p", date_parse_udf(df["date_end"]))
    df = df.drop(*["date_start", "date_end"])
    df = df.withColumnRenamed("date_start_p", "date_start")
    df = df.withColumnRenamed("date_end_p", "date_end")

    df = df.withColumn("nights_p", number_udf(df["nights"]))
    df = df.drop(*["nights"])
    df = df.withColumnRenamed("nights_p", "nights")

    #df.select("date_start", "date_end", "nights").show()

    # Remove with no start date
    df = df.where((F.col("date_start").isNotNull()))

    # Remove rows with date_end null and no nigth information
    df = df.where(~((F.col("date_end").isNull()) & ((F.col("nights").isNull()) | (F.col("nights") == 0))))

    # Check if exists rows with "nights" but no "date_end"
    df.select("date_start", "date_end", "nights").where((F.col("date_end").isNull()) & (F.col("nights").isNotNull())).show()

    write_parquet("base_renamed_dates", df)

    spark.stop()

def channel_analysis():
    spark = get_spark_context()
    df = read_parquet(spark, "base_renamed_dates")
    #df.printSchema()

    only_date_start = df.select("date_start")
    only_date_start.show()

    spark.stop()

    