from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
import numpy as np

def run():
    spark = SparkSession.builder.appName("CorrelationAnalysis").getOrCreate()
    data = [
        (1, 10.0, 20.0, 30.0),
        (2, 20.0, 25.0, 40.0),
        (3, 30.0, 30.0, 50.0),
        (4, 40.0, 35.0, 60.0),
    ]
    columns = ["id", "feature1", "feature2", "feature3"]

    df = spark.createDataFrame(data, columns)

    feature_cols = ["feature1", "feature2", "feature3"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_vector = assembler.transform(df).select("features")

    correlation_result = Correlation.corr(df_vector, "features", method="pearson")
    correlation_matrix = correlation_result.collect()[0]["pearson({})".format("features")].values

    matrix_size = len(feature_cols)
    corr_array = np.array(correlation_matrix).reshape(matrix_size, matrix_size)

    threshold = 0.8
    for i in range(matrix_size):
        for j in range(i + 1, matrix_size):
            corr_val = corr_array[i][j]
            if abs(corr_val) > threshold:
                print(f"⚠️ Strong correlation between {feature_cols[i]} and {feature_cols[j]}: {corr_val:.2f}")

