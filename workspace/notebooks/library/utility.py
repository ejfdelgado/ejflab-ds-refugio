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
from library.parsers import parse_spanish_date, parse_integer
from library.base import get_spark_context, write_parquet, load_csv_file, read_parquet

import seaborn as sns
import matplotlib.pyplot as plt

all_channels = {
    "presencial": [
        "pasó y vio el letrero, nos buscó en booking y reservó",
    ],
    "booking": [
        "booking.com", 
        "booking", 
        "boooking",
        'booking. solo de tres ver el sistema',
        ],
    "genius": [
        "booking/genius"
    ],
    "facebook" : [
        "facebook",
        ],
    "walk_in" : [
        "walkin",
        "walking",
        "walk in",
        "walk - in",
        "walk in, instagram",
        "walk in/ luz",
        "walk in /luz",
        "walk in luz",
        "walk in luzma",
        ],
    "instagram" : [
        "instagram",
        ],
    "redes_sociales": [
        "redes sociales", 
        "redes",
        "redes & booking",
        "booking/redes",
        ],
    "google" : [
        "google",
        "google maps y green door",
        ],
    "amigo" : [
        "referido lilly",
        "amigo mao",
        "amigos",
        "amigo", 
        "amiga", 
        "primo carlos", 
        "familia carlitos",
        ],
    "guaiti": [
        "referido guaiti",
    ],
    "referido" : [
        "familia",
        "referido", 
        "referida", 
        "referidos",
        "referidas",
        "referido de otros húespedes",
        "referida, cliente frecuente",
        "referido barichara",
        "referido papa",
        "referido aleja hernandez",
        "referido mamá",
        "referidos polito",
        "referidos evento",
        ],
    "recurrente" : [
        "se habia hospedado antes", 
        'ya se había hospedado',
        "huesped frecuente",
        "regreso",
        "cliente frecuente",
        "cliente frecuente / redes",
        ],
    "stay_over": [
        "stay over",
    ],
    "fortuito": [
        "evento fortuito",
        "fortuito",
    ],
    "vaolo": [
        "vaolo",
        "visita de vaolo",
    ],
    "otros": [
        "voluntarios",
        "santander weekend",
        "staff",
        "parques naturales de colombia",
        "amigos del agua",
        "socia",
    ]
}

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

def fix_end_date(date_start, nights, date_end):
    if (nights is None):
        return date_end
    return date_start + datetime.timedelta(days=nights)

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
    # Keep only if date_start is not null
    df = df.where((F.col("date_start").isNotNull()))

    # Check if exists rows with "nights" but no "date_end"
    # This case was not found in original dataset
    print('If the following table shows rows, consider fix date_end from nights...')
    df.select("date_start", "date_end", "nights").where((F.col("date_end").isNull()) & (F.col("nights").isNotNull())).show()

    # A mismatch between nights and the difference between date_start and date_end was found
    # Recompute those mismatch cases from date_start and date_end
    df = df.withColumn("nights2", F.datediff("date_end", "date_start"))
    fix_date_end_udf = F.udf(fix_end_date, DateType())
    df = df.withColumn(
    "date_end2",F.when(
        ~(F.col("nights") == F.col("nights2")), 
        fix_date_end_udf(F.col("date_start"), F.col("nights"), F.col("date_end")))
        .otherwise(F.col("date_end"))
    )
    df = df.drop(*["nights2", "date_end"])
    df = df.withColumnRenamed("date_end2", "date_end") # Now fixed
    df = df.withColumn("nights2", F.datediff("date_end", "date_start"))

    # I found nigth with value NULL, so belive nigth2
    df = df.withColumn("nights3", F.when(
        F.col("nights").isNull(),
        F.col("nights2")
    ).otherwise(F.col("nights")))
    df = df.drop(*["nights", "nights2"])
    df = df.withColumnRenamed("nights3", "nights")

    df = df.orderBy(F.asc("date_start"))

    df.select("nights", "date_start", "date_end").show()

    write_parquet("base_renamed_dates", df)

    spark.stop()

def normalize_channel(original):
    if original is not None:
        original = original.strip().lower()

        for key in all_channels:
            list = all_channels[key]
            if (original in list):
                return key
    return None

def create_channel_df():
    spark = get_spark_context()
    channels_data = []
    for key in all_channels:
        channels_data.append((key,),)
    channel_df = spark.createDataFrame(channels_data, ["channel_id"])
    write_parquet("channels", channel_df)
    spark.stop()

def channel_normalization():
    spark = get_spark_context()
    df = read_parquet(spark, "base_renamed_dates")
    #df.printSchema()

    # Normalize column
    normalize_channel_udf = F.udf(normalize_channel, StringType())
    df = df.withColumn("channel2", normalize_channel_udf(F.col("channel")))

    # This help to build channel classification
    if False:
        focus = df.select(*["channel", "channel2"]).where(F.col("channel2").isNull())
        focus = focus.where(F.col("channel").isNotNull())
        focus.show(truncate=False)
        print(focus.count())

    df = df.drop("channel")
    df = df.withColumnRenamed("channel2", "channel")

    # Remove null channels
    df = df.where((F.col("channel").isNotNull()))

    # Parse numbers
    number_short_udf = F.udf(parse_integer, ShortType())
    number_integer_udf = F.udf(parse_integer, IntegerType())
    df = df.withColumn("pax_p", number_short_udf(df["pax"]))
    df = df.withColumn("price_p", number_integer_udf(df["price"]))
    df = df.drop(*["pax", "price"])
    df = df.withColumnRenamed("pax_p", "pax")
    df = df.withColumnRenamed("price_p", "price")

    write_parquet("base_renamed_dates_channel", df)

    spark.stop()

def channel_analysis_1():
    spark = get_spark_context()
    df = read_parquet(spark, "base_renamed_dates_channel")
    # Group it
    channel_report = df.groupBy(*["channel"]).agg(
        F.count("*").alias("count")
    ).orderBy(F.desc("count"))
    #channel_report.show()
    channel_values = channel_report.select("channel").rdd.flatMap(lambda x: x).collect()
    values_general = channel_report.select("count").rdd.flatMap(lambda x: x).collect()
    data = {'Channel': channel_values, 'Count': values_general}
    sns.barplot(x='Channel', y='Count', data=data)
    plt.xticks(rotation=90)
    plt.title("Overall channel distribution")
    plt.show()
    spark.stop()

def add_year_month_column(df, date_colum_name):
    def add_year_month(dt):
        return f"{dt.year}-{dt.month:02}"
    
    add_year_month_udf = F.udf(add_year_month, StringType())
    df = df.withColumn(f"{date_colum_name}_ym", add_year_month_udf(df[date_colum_name]))
    return df

def channel_analysis_2():
    spark = get_spark_context()
    df = read_parquet(spark, "base_renamed_dates_channel")
    #df.printSchema()
    df = df.select(*["date_start", "nights", "channel", "pax", "price"])
    df.show()
    time_line_df = read_parquet(spark, "time_line_2")

    df = add_year_month_column(df, "date_start")
    time_line_df = add_year_month_column(time_line_df, "date")

    channel_report = df.groupBy(*["date_start_ym", "channel"]).agg(
        F.count("*").alias("count")
    ).orderBy(F.desc("count"))

    channel_report.show()

    x_common = ['2019-01', '2019-02', '2019-03']

    sns.lineplot(x=x_common, y=[2, 4, 6], label='Line A')
    sns.lineplot(x=x_common, y=[1, 2, 3], label='Line B')

    plt.title("Multiple Lines (Manual Method)")
    plt.xticks(rotation=90)
    plt.legend()
    plt.show()

    spark.stop()