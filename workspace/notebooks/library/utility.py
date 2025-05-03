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

def channel_analysis():
    spark = get_spark_context()
    df = read_parquet(spark, "base_renamed_dates")
    #df.printSchema()

    only_date_start = df.select("date_start")
    only_date_start.show()

    spark.stop()

def days_between(start_date: datetime.date, end_date: datetime.date) -> int:
    delta = end_date - start_date
    return abs(delta.days)

# Generate raw time_line
def get_dates_in_range(start_date, end_date, name="time_line"):
    spark = get_spark_context()
    
    days_count = days_between(start_date, end_date) + 1

    def add_days_to_start(start, offset):
        return start + datetime.timedelta(days=int(offset))
    
    date_udf = F.udf(lambda offset: add_days_to_start(start_date, offset), DateType())

    # Create DataFrame with one row per day
    df = spark.range(0, days_count).withColumn(
        "date", date_udf(F.col("id"))
    ).orderBy(F.asc("id"))

    write_parquet(name, df)

    spark.stop()

def add_time_dimensions(first_n_rows=None):
    # Initialize a Spark session
    spark = get_spark_context()
    time_line = read_parquet(spark, "time_line")
    time_line = time_line.alias("time_line")
    dias_festivos = read_parquet(spark, "dias_festivos")
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

    write_parquet("time_line_2", time_line_2)

    spark.stop()