import os
import math
import datetime
import pandas as pd
import pyarrow as pa
import numpy as np
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

def explode_data():
    spark = get_spark_context()
    df = read_parquet(spark, "base_renamed_dates_channel")
    channels_df = read_parquet(spark, "channels")
    #df.printSchema()
    df = df.select(*["date_start", "nights", "channel", "pax", "price"])

    # Weird cases, remove it!
    #df.where(F.col("pax").isNull()).show()
    df = df.where(F.col("pax").isNotNull())
    # It exists some reservations with zero persons
    df = df.where(F.col("pax") > 0)

    df = df.withColumn("unit_price", F.round(F.col("price") / (F.col("nights") * F.col("pax")), 0))
    #df.orderBy(F.desc("unit_price")).show(truncate=False)

    time_line_df = read_parquet(spark, "time_line_2")

    df = add_year_month_column(df, "date_start")
    time_line_df = add_year_month_column(time_line_df, "date")

    # classify
    df = df.withColumn("single", F.when(F.col("pax") == 1, 1).otherwise(0))
    df = df.withColumn("couple", F.when(F.col("pax") == 2, 1).otherwise(0))
    df = df.withColumn("group", F.when(F.col("pax") > 2, 1).otherwise(0))

    df = df.withColumn("single_inc", F.when(F.col("pax") == 1, F.col("price")).otherwise(0))
    df = df.withColumn("couple_inc", F.when(F.col("pax") == 2, F.col("price")).otherwise(0))
    df = df.withColumn("group_inc", F.when(F.col("pax") > 2, F.col("price")).otherwise(0))

    channel_report = df.groupBy(*["date_start_ym", "channel"]).agg(
        F.count("*").alias("count"),
        F.sum("pax").alias("pax_sum"),
        F.avg("pax").alias("pax_avg"),
        F.sum("single").alias("single_sum"),
        F.sum("couple").alias("couple_sum"),
        F.sum("group").alias("group_sum"),
        F.sum("single_inc").alias("single_inc_sum"),
        F.sum("couple_inc").alias("couple_inc_sum"),
        F.sum("group_inc").alias("group_inc_sum"),
        F.sum("price").alias("price_sum"),
    ).orderBy(F.desc("date_start_ym"))
    channel_report = channel_report.alias("channel_report")

    # Compute classification percentage
    
    channel_report = channel_report.withColumn("single_per", F.when(F.col("count") > 0,F.col("single_sum") / F.col("count")).otherwise(0))
    channel_report = channel_report.withColumn("couple_per", F.when(F.col("count") > 0,F.col("couple_sum") / F.col("count")).otherwise(0))
    channel_report = channel_report.withColumn("group_per", F.when(F.col("count") > 0,F.col("group_sum") / F.col("count")).otherwise(0))

    # Check if create a count 2 it differs
    channel_report = channel_report.withColumn("count2", F.col("single_sum") + F.col("couple_sum") + F.col("group_sum"))
    channel_report.where(~(F.col("count2") == F.col("count"))).show()
    channel_report = channel_report.drop("count2")

    time_line_df = time_line_df.groupBy(*["date_ym"]).agg(F.count("*").alias("count"))
    time_line_df = time_line_df.drop("count")
    time_line_df = time_line_df.orderBy(F.desc("date_ym"))

    # make cartesian product of timeline and channels
    time_channel_product = time_line_df.crossJoin(channels_df)
    time_channel_product = time_channel_product.alias("time_channel_product")
    
    # Join!
    joined_data = time_channel_product.join(channel_report, (F.col("time_channel_product.date_ym") == F.col("channel_report.date_start_ym")) & (F.col("time_channel_product.channel_id") == F.col("channel_report.channel")), how="left")
    joined_data = joined_data.drop(*["date_start_ym", "channel"])

    # Fill with zeros
    joined_data = joined_data.fillna({
        'count': 0, 
        'pax_sum': 0, 
        'pax_avg': 0, 
        'price_sum': 0,
        'single_sum': 0,
        'couple_sum': 0,
        'group_sum': 0,
        'single_inc_sum': 0,
        'couple_inc_sum': 0,
        'group_inc_sum': 0,
        'single_per': 0,
        'couple_per': 0,
        'group_per': 0,
        })

    joined_data.show()

    write_parquet("exploded", joined_data)
    
def channel_analysis_2(custom_channels, focus_column, title, start_date="2020-09", end_date="2025-03"):
    spark = get_spark_context()
    df = read_parquet(spark, "exploded")

    # Filter post pandemic results
    df = df.where((F.col("date_ym") >= start_date) & (F.col("date_ym") < end_date))

    # Channel behavior through time
    df = df.groupBy(*["date_ym", "channel_id"]).agg(
        F.sum("count").alias("group_count"),
        F.sum("pax_avg").alias("pax_avg"),
        F.sum("price_sum").alias("price_sum"),
    )

    #df.show()
    
    x_common = df.select("date_ym").distinct().orderBy(F.asc("date_ym")).rdd.flatMap(lambda x: x).collect()

    plt.figure(figsize=(20, 6))
    sum_plot = None
    for channel_id in all_channels:
    
        simplified = df.where(F.col("channel_id") == channel_id).select(*["date_ym", focus_column]).orderBy(F.asc("date_ym"))
        y_values = simplified.select(focus_column).rdd.flatMap(lambda x: x).collect()
        if (sum_plot is None):
            sum_plot = y_values
        else:
            for index, value in enumerate(y_values):
                sum_plot[index] += value
        if (channel_id in custom_channels):
            sns.lineplot(x=x_common, y=y_values, label=channel_id)

    if not (focus_column in ["pax_avg"]):
        sns.lineplot(x=x_common, y=sum_plot, label="sum")

    plt.title(title)
    plt.xticks(rotation=90)
    plt.legend()
    plt.show()

    spark.stop()

def channel_analysis_3(start_date="2020-09", end_date="2025-03"):
    spark = get_spark_context()
    df = read_parquet(spark, "exploded")

    # Filter post pandemic results
    df = df.where((F.col("date_ym") >= start_date) & (F.col("date_ym") < end_date))

    df = df.groupBy(*["date_ym"]).agg(
        F.sum("count").alias("count"), # Here it is supposed to be equal BUT NOT!!! panic
        F.sum("single_sum").alias("single_sum"),
        F.sum("couple_sum").alias("couple_sum"),
        F.sum("group_sum").alias("group_sum"),
    )
    # Panic here!
    #df = df.withColumn("count", F.col("single_sum") + F.col("couple_sum") + F.col("group_sum"))

    df = df.withColumn("single_per", F.when(F.col("count") > 0,100*F.col("single_sum") / F.col("count")).otherwise(0))
    df = df.withColumn("couple_per", F.when(F.col("count") > 0,100*F.col("couple_sum") / F.col("count")).otherwise(0))
    df = df.withColumn("group_per", F.when(F.col("count") > 0,100*F.col("group_sum") / F.col("count")).otherwise(0))
    #df.show()

    # Start ploting...

    # Sample data
    categories = ['single_per', 'couple_per', 'group_per']

    # Bar width and x locations
    x = df.select("date_ym").distinct().orderBy(F.asc("date_ym")).rdd.flatMap(lambda x: x).collect()

    plt.figure(figsize=(20, 6))

    col = categories[0]
    simplified_single = df.select(*["date_ym", col]).orderBy(F.asc("date_ym"))
    value1 = simplified_single.select(col).rdd.flatMap(lambda x: x).collect()
    plt.bar(x, value1, label=col)

    col = categories[1]
    simplified_single = df.select(*["date_ym", col]).orderBy(F.asc("date_ym"))
    value2 = simplified_single.select(col).rdd.flatMap(lambda x: x).collect()
    plt.bar(x, value2, bottom=value1, label=col)

    col = categories[2]
    simplified_single = df.select(*["date_ym", col]).orderBy(F.asc("date_ym"))
    value3 = simplified_single.select(col).rdd.flatMap(lambda x: x).collect()
    plt.bar(x, value3, bottom=np.array(value1)+np.array(value2), label=col)

    # Labels and legend
    plt.title("Client profiles")
    plt.xticks(rotation=90)
    plt.legend()
    plt.show()
    

    spark.stop()

def channel_analysis_4(start_date="2020-09", end_date="2025-03"):
    spark = get_spark_context()
    df = read_parquet(spark, "exploded")

    # Filter post pandemic results
    df = df.where((F.col("date_ym") >= start_date) & (F.col("date_ym") < end_date))

    df = df.groupBy(*["date_ym"]).agg(
        F.sum("single_inc_sum").alias("single_sum"),
        F.sum("couple_inc_sum").alias("couple_sum"),
        F.sum("group_inc_sum").alias("group_sum"),
    )
    df = df.withColumn("count", F.col("single_sum") + F.col("couple_sum") + F.col("group_sum"))
    df = df.withColumn("single_per", F.when(F.col("count") > 0,F.col("single_sum") / F.col("count")).otherwise(0))
    df = df.withColumn("couple_per", F.when(F.col("count") > 0,F.col("couple_sum") / F.col("count")).otherwise(0))
    df = df.withColumn("group_per", F.when(F.col("count") > 0,F.col("group_sum") / F.col("count")).otherwise(0))

    #df.show()

    # Start ploting...

    # Sample data
    categories = ['single_per', 'couple_per', 'group_per']

    # Bar width and x locations
    x = df.select("date_ym").distinct().orderBy(F.asc("date_ym")).rdd.flatMap(lambda x: x).collect()

    plt.figure(figsize=(20, 6))

    col = categories[0]
    simplified_single = df.select(*["date_ym", col]).orderBy(F.asc("date_ym"))
    value1 = simplified_single.select(col).rdd.flatMap(lambda x: x).collect()
    plt.bar(x, value1, label=col)

    col = categories[1]
    simplified_single = df.select(*["date_ym", col]).orderBy(F.asc("date_ym"))
    value2 = simplified_single.select(col).rdd.flatMap(lambda x: x).collect()
    plt.bar(x, value2, bottom=value1, label=col)

    col = categories[2]
    simplified_single = df.select(*["date_ym", col]).orderBy(F.asc("date_ym"))
    value3 = simplified_single.select(col).rdd.flatMap(lambda x: x).collect()
    plt.bar(x, value3, bottom=np.array(value1)+np.array(value2), label=col)

    # Labels and legend
    plt.title("Client profiles income")
    plt.xticks(rotation=90)
    plt.legend()
    plt.show()
    

    spark.stop()

def channel_analysis_5(start_date="2020-09", end_date="2025-03"):
    spark = get_spark_context()
    df = read_parquet(spark, "exploded")

    # Filter post pandemic results
    df = df.where((F.col("date_ym") >= start_date) & (F.col("date_ym") < end_date))

    df = df.groupBy(*["date_ym"]).agg(
        F.sum("count").alias("count"),
        F.sum("single_sum").alias("single_sum"),
        F.sum("couple_sum").alias("couple_sum"),
        F.sum("group_sum").alias("group_sum"),
    )

    df = df.withColumn("single_per", F.when(F.col("count") > 0,100*F.col("single_sum") / F.col("count")).otherwise(0))
    df = df.withColumn("couple_per", F.when(F.col("count") > 0,100*F.col("couple_sum") / F.col("count")).otherwise(0))
    df = df.withColumn("group_per", F.when(F.col("count") > 0,100*F.col("group_sum") / F.col("count")).otherwise(0))

    # Stationary analysis by month
    # Add column for month only
    MONTH_MAP = {
        "01": "January",
        "02": "February",
        "03": "March",
        "04": "April",
        "05": "May",
        "06": "June",
        "07": "July",
        "08": "August",
        "09": "September",
        "10": "October",
        "11": "Nobember",
        "12": "December",
    }
    def get_only_month(ym_text):
        # 2025-01
        return ym_text[5:]
    get_only_month_udf = F.udf(get_only_month, StringType())
    df = df.withColumn("month", get_only_month_udf(F.col("date_ym")))

    df = df.groupBy(*["month"]).agg(
        F.var_samp("single_per").alias("single_var"),
        F.var_samp("couple_per").alias("couple_var"),
        F.var_samp("group_per").alias("group_var"),
        F.avg("single_per").alias("single_avg"),
        F.avg("couple_per").alias("couple_avg"),
        F.avg("group_per").alias("group_avg"),
    )

    # Compute standar deviation from variance
    def compute_standar_deviation(variance):
        return  math.sqrt(variance)
    compute_standar_deviation_udf = F.udf(compute_standar_deviation, FloatType())

    groups = [
        {"name": "single", "ds_avg_ratio": 1}, 
        {"name": "couple", "ds_avg_ratio": 0.2}, 
        {"name": "group", "ds_avg_ratio": 0.2}, 
        ]

    for group_item in groups:
        group = group_item["name"]
        ds_avg_ratio = group_item["ds_avg_ratio"]
        df = df.withColumn(f"{group}_ds", compute_standar_deviation_udf(F.col(f"{group}_var")))
        df.select(*["month", f"{group}_ds", f"{group}_avg"]).where(F.col(f"{group}_ds") < ds_avg_ratio*F.col(f"{group}_avg")).orderBy(F.asc("month")).show()
    

    spark.stop()