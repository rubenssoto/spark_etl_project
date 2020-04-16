from pyspark.sql import SparkSession


def start_spark(spark_session_name: str):
    spark = SparkSession.builder.appName(spark_session_name).getOrCreate()

    return spark