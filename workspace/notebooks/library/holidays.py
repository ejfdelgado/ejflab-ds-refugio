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
from library import utility

# https://www.festivos.com.co/historico

def recreate():
    spark = utility.get_spark_context()
    data = [
            # 2019
            ("2019, 1, 1",),
            ("2019, 1, 7",),
            ("2019, 3, 25",),
            ("2019, 4, 18",),
            ("2019, 4, 19",),
            ("2019, 5, 1",),
            ("2019, 6, 3",),
            ("2019, 6, 24",),
            ("2019, 7, 1",),
            ("2019, 7, 20",),
            ("2019, 8, 7",),
            ("2019, 8, 19",),
            ("2019, 10, 14",),
            ("2019, 11, 7",),
            ("2019, 11, 11",),
            ("2019, 12, 8",),
            ("2019, 12, 25",),
            # 2020
            ("2020, 1, 1",), # Año Nuevo
            ("2020, 1, 6",), # Epifanía
            ("2020, 3, 23",), # Día de San José
            ("2020, 4, 9",), # Jueves Santo	- Semana Santa
            ("2020, 4, 10",), # Viernes Santo - Semana Santa
            ("2020, 5, 1",), # Día del trabajo
            ("2020, 5, 25",), # Ascensión de Jesús
            ("2020, 6, 15",), # Corpus Christi
            ("2020, 6, 22",), # Sagrado Corazón de Jesús
            ("2020, 6, 29",), # San Pedro y San Pablo
            ("2020, 7, 20",), # Día de la independencia
            ("2020, 8, 7",), # Batalla de Boyacá
            ("2020, 8, 17",), # Asunción de la Virgen
            ("2020, 10, 12",), # Día de la Diversidad Étnica y Cultural
            ("2020, 11, 2",), # Todos los Santos
            ("2020, 11, 16",), # Independencia de Cartagena
            ("2020, 12, 8",), # Dia de las velitas
            ("2020, 12, 25",), #Navidad
            # 2021 -
            ("2021, 1, 1",),
            ("2021, 1, 11",),
            ("2021, 3, 22",),
            ("2021, 4, 1",),
            ("2021, 4, 2",),
            ("2021, 5, 1",),
            ("2021, 5, 17",),
            ("2021, 6, 7",),
            ("2021, 6, 14",),
            ("2021, 7, 5",),
            ("2021, 7, 20",),
            ("2021, 8, 7",),
            ("2021, 8, 16",),
            ("2021, 10, 18",),
            ("2021, 11, 1",),
            ("2021, 11, 15",),
            ("2021, 12, 8",),
            ("2021, 12, 25",),
            # 2022
            ("2022, 1, 1",),
            ("2022, 1, 10",),
            ("2022, 3, 21",),
            ("2022, 4, 14",),
            ("2022, 4, 15",),
            ("2022, 5, 1",),
            ("2022, 5, 30",),
            ("2022, 6, 20",),
            ("2022, 6, 27",),
            ("2022, 7, 4",),
            ("2022, 7, 20",),
            ("2022, 8, 7",),
            ("2022, 8, 15",),
            ("2022, 10, 17",),
            ("2022, 11, 7",),
            ("2022, 11, 14",),
            ("2022, 12, 8",),
            ("2022, 12, 25",),
            # 2023
            ("2023, 1, 1",),
            ("2023, 1, 9",),
            ("2023, 3, 20",),
            ("2023, 4, 6",),
            ("2023, 4, 7",),
            ("2023, 5, 1",),
            ("2023, 5, 22",),
            ("2023, 6, 12",),
            ("2023, 6, 19",),
            ("2023, 7, 3",),
            ("2023, 7, 20",),
            ("2023, 8, 7",),
            ("2023, 8, 21",),
            ("2023, 10, 16",),
            ("2023, 11, 6",),
            ("2023, 11, 13",),
            ("2023, 12, 8",),
            ("2023, 12, 25",),
            # 2024
            ("2024, 1, 1",),
            ("2024, 1, 8",),
            ("2024, 3, 25",),
            ("2024, 4, 28",),
            ("2024, 4, 29",),
            ("2024, 5, 1",),
            ("2024, 5, 13",),
            ("2024, 6, 3",),
            ("2024, 6, 10",),
            ("2024, 7, 1",),
            ("2024, 7, 20",),
            ("2024, 8, 7",),
            ("2024, 8, 19",),
            ("2024, 10, 14",),
            ("2024, 11, 4",),
            ("2024, 11, 11",),
            ("2024, 12, 8",),
            ("2024, 12, 25",),
            # 2025
            ("2025, 1, 1",),
            ("2025, 1, 6",),
            ("2025, 3, 24",),
            ("2025, 4, 17",),
            ("2025, 4, 18",),
            ("2025, 5, 1",),
            ("2025, 6, 2",),
            ("2025, 6, 23",),
            ("2025, 6, 30",),
            ("2025, 7, 20",),
            ("2025, 8, 7",),
            ("2025, 8, 18",),
            ("2025, 10, 13",),
            ("2025, 11, 3",),
            ("2025, 11, 17",),
            ("2025, 12, 8",),
            ("2025, 12, 25",),
        ]
    def parse_simple_date(txt):
        return datetime.datetime.strptime(txt, "%Y, %m, %d").date()
    df = spark.createDataFrame(data, ["holy"])
    date_simple_udf = F.udf(parse_simple_date, DateType())
    df = df.withColumn("holyd", date_simple_udf(F.col("holy")))
    df = df.drop(*["holy"])
    df = df.withColumnRenamed("holyd", "fecha")
    df = df.orderBy(F.asc("holyd"))
    df = df.withColumn("festivo_id", F.monotonically_increasing_id())
    df.show()
    utility.write_parquet("dias_festivos", df)
    spark.stop()

def days_between(start_date: datetime.date, end_date: datetime.date) -> int:
    delta = end_date - start_date
    return abs(delta.days)

# Generate raw time_line
def get_dates_in_range(start_date, end_date, name="time_line"):
    spark = utility.get_spark_context()
    
    days_count = days_between(start_date, end_date) + 1

    def add_days_to_start(start, offset):
        return start + datetime.timedelta(days=int(offset))
    
    date_udf = F.udf(lambda offset: add_days_to_start(start_date, offset), DateType())

    # Create DataFrame with one row per day
    df = spark.range(0, days_count).withColumn(
        "date", date_udf(F.col("id"))
    ).orderBy(F.asc("id"))

    utility.write_parquet(name, df)

    spark.stop()

def add_time_dimensions(first_n_rows=None):
    # Initialize a Spark session
    spark = utility.get_spark_context()
    time_line = utility.read_parquet(spark, "time_line")
    time_line = time_line.alias("time_line")
    dias_festivos = utility.read_parquet(spark, "dias_festivos")
    dias_festivos = dias_festivos.alias("dias_festivos")

    time_line_2 = time_line.join(dias_festivos, F.col("time_line.date") == F.col("dias_festivos.fecha"), how="left")
    time_line_2 = time_line_2.drop("id", "dia", "mes", "year", "fecha")

    # Adding the day of the year range [1, 366]
    def day_of_year(value):
        return value.timetuple().tm_yday
    day_of_year_udf = F.udf(day_of_year, ShortType())
    time_line_2 = time_line_2.withColumn("doy", day_of_year_udf(F.col("date")))

    # Adding the month of the year range [1, 12]
    def month_of_year(value):
        return value.timetuple().tm_mon
    month_of_year_udf = F.udf(month_of_year, ShortType())
    time_line_2 = time_line_2.withColumn("mon", month_of_year_udf(F.col("date")))

    time_line_2 = time_line_2.withColumn("mont", 
        F.when(F.col("mon") == 1, "ene")
        .when(F.col("mon") == 2, "feb")
        .when(F.col("mon") == 3, "mar")
        .when(F.col("mon") == 4, "abr")
        .when(F.col("mon") == 5, "may")
        .when(F.col("mon") == 6, "jun")
        .when(F.col("mon") == 7, "jul")
        .when(F.col("mon") == 8, "ago")
        .when(F.col("mon") == 9, "sep")
        .when(F.col("mon") == 10, "oct")
        .when(F.col("mon") == 11, "nov")
        .when(F.col("mon") == 12, "dic")
        .otherwise("")
    )

    # Adding the day of month range [1, 31]
    def day_of_month(value):
        return value.timetuple().tm_mday
    day_of_month_udf = F.udf(day_of_month, ShortType())
    time_line_2 = time_line_2.withColumn("dom", day_of_month_udf(F.col("date")))

    # Adding the day of week range [0, 6]; Monday is 0
    def day_of_week(value):
        return value.timetuple().tm_wday
    day_of_week_udf = F.udf(day_of_week, ShortType())
    time_line_2 = time_line_2.withColumn("dow", day_of_week_udf(F.col("date")))

    time_line_2 = time_line_2.withColumn("dowt", 
        F.when(F.col("dow") == 0, "lun")
        .when(F.col("dow") == 1, "mar")
        .when(F.col("dow") == 2, "mie")
        .when(F.col("dow") == 3, "jue")
        .when(F.col("dow") == 4, "vie")
        .when(F.col("dow") == 5, "sab")
        .when(F.col("dow") == 6, "dom")
        .otherwise("")
    )

    # Adding the week of year range [1, 52]
    def week_of_year(value):
        return value.isocalendar()[1]
    week_of_year_udf = F.udf(week_of_year, ShortType())
    time_line_2 = time_line_2.withColumn("woy", week_of_year_udf(F.col("date")))

    # Adding the weekend 
    def is_weekend(value):
        return 1 if value == 5 or value == 6 else 0
    is_weekend_udf = F.udf(is_weekend, ShortType())
    time_line_2 = time_line_2.withColumn("wend", is_weekend_udf(F.col("dow")))

    # Adding holidays
    def not_is_null(value):
        return 1 if value != None else 0
    not_is_null_udf = F.udf(not_is_null, ByteType())
    time_line_2 = time_line_2.withColumn("hol", not_is_null_udf(F.col("festivo_id")))
    time_line_2 = time_line_2.drop("festivo_id")

    # Adding banks days off
    def bank_on(dow, hol):
        return 0 if dow == 5 or dow == 6 or hol == 1 else 1
    bank_on_udf = F.udf(bank_on, ShortType())
    time_line_2 = time_line_2.withColumn("bank", bank_on_udf(F.col("dow"), F.col("hol")))

    # Adding payment days
    def pay_day(dom):
        return 1 if dom == 28 or dom == 13 else 0
    pay_day_udf = F.udf(pay_day, ShortType())
    time_line_2 = time_line_2.withColumn("pay", pay_day_udf(F.col("dom")))

    # Adding payment id
    window_spec_payment = Window.orderBy(F.asc("date")).rowsBetween(Window.unboundedPreceding, 0)
    time_line_2 = time_line_2.withColumn(
        "payid_", F.sum("pay").over(window_spec_payment)
    )
    time_line_2 = time_line_2.withColumn("payid", F.col("payid_") + 1)
    time_line_2 = time_line_2.drop("payid_")
    # make the AND with banks
    # Adding banks_payment_id days off
    def bank_payid_on(bank, payid):
        return payid if bank == 1 else 0
    bank_payid_on_udf = F.udf(bank_payid_on, ShortType())
    time_line_2 = time_line_2.withColumn("paybank_", bank_payid_on_udf(F.col("bank"), F.col("payid")))
    # Compute minimum of each group
    window_spec_pay_max = Window.partitionBy("payid").orderBy(F.asc("date")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    time_line_2 = time_line_2.withColumn("paybank", F.max("paybank_").over(window_spec_pay_max))
    time_line_2 = time_line_2.drop("paybank_")
    # Shift paybank
    window_date_spec = Window.orderBy(F.asc("date"))
    time_line_2 = time_line_2.withColumn("paybank_shift", F.lag("paybank", 1).over(window_date_spec))
    time_line_2 = time_line_2.withColumn(
        "paybank_change",
        F.when(F.col("paybank") != F.col("paybank_shift"), 1).otherwise(0)
    )
    time_line_2 = time_line_2.drop("paybank_shift")
    # Make the and with the bank
    time_line_2 = time_line_2.withColumn("pay_off", bank_payid_on_udf(F.col("bank"), F.col("paybank_change")))
    time_line_2 = time_line_2.drop("payid", "paybank", "paybank_change")
    def invert_1_0(val):
        return 1 if val == 0 else 0
    invert_1_0_udf = F.udf(invert_1_0, ShortType())
    time_line_2 = time_line_2.withColumn("pay2", invert_1_0_udf(F.col("pay_off")))

    def compute_consecutive_id(df, column_name):
        window_spec = Window.orderBy(F.asc("date"))
        name_shift=f"{column_name}_shift"
        name_change=f"{column_name}_change"
        name_group=f"{column_name}_group"
        name_sum=f"{column_name}_sum"
        df_shifted = df.withColumn(name_shift, F.lag(column_name, 1).over(window_spec))
        df_with_flag = df_shifted.withColumn(
            name_change,
            F.when(F.col(column_name) != F.col(name_shift), 1).otherwise(0)
        )
        window_spec_unbounded = window_spec.rowsBetween(Window.unboundedPreceding, 0)
        df_with_change_id = df_with_flag.withColumn(
            name_group, F.sum(name_change).over(window_spec_unbounded)
        )

        window_spec = Window.partitionBy(name_group).orderBy(F.asc("date")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        df_with_cumsum = df_with_change_id.withColumn(name_sum, F.sum(column_name).over(window_spec))
        
        return df_with_cumsum.drop(name_shift, name_change, name_group)

    time_line_2 = compute_consecutive_id(time_line_2, "bank")
    time_line_2 = compute_consecutive_id(time_line_2, "pay2")

    time_line_2 = time_line_2.withColumn("fun", invert_1_0_udf(F.col("bank")))
    time_line_2 = compute_consecutive_id(time_line_2, "fun")

    time_line_2 = time_line_2.withColumnRenamed("bank", "labor")
    time_line_2 = time_line_2.withColumnRenamed("bank_sum", "labor_id")
    time_line_2 = time_line_2.withColumnRenamed("pay_off", "pay15")
    time_line_2 = time_line_2.withColumnRenamed("pay2_sum", "pay15_id")
    time_line_2 = time_line_2.withColumnRenamed("fun_sum", "fun_id")
    time_line_2 = time_line_2.drop("pay", "pay2")

    # montly payed
    def compute_montly_pay(pay15, dom):
        return 0 if dom > 10 and dom < 20 else pay15
    compute_montly_pay_udf = F.udf(compute_montly_pay, ShortType())
    time_line_2 = time_line_2.withColumn("pay30", compute_montly_pay_udf(F.col("pay15"), F.col("dom")))

    time_line_2 = time_line_2.withColumn("pay30_", invert_1_0_udf(F.col("pay30")))
    time_line_2 = compute_consecutive_id(time_line_2, "pay30_")
    time_line_2 = time_line_2.withColumnRenamed("pay30__sum", "pay30_id")

    # Reorder dataset
    #time_line_2 = time_line_2.select("date", "doy","mon","dom","dow","woy","wend","hol","dowt", "labor","labor_id", "fun", "fun_id","pay15","pay15_id", "pay30", "pay30_id")
    time_line_2 = time_line_2.select("date","mont","dowt","labor_id","fun_id","pay15_id","pay30_id")

    # time_line_2.show(128)

    # Limit last N days
    if (first_n_rows is not None):
        time_line_2 = time_line_2.orderBy(F.desc("date")).limit(first_n_rows).orderBy(F.asc("date"))

    utility.write_parquet("time_line_2", time_line_2)

    spark.stop()