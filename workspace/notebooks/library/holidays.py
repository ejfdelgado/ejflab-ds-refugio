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
    df = df.orderBy(F.asc("holyd"))
    df.show()
    utility.write_parquet("dias_festivos", df)
    spark.stop()