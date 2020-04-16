def read_csv(spark, filepath: str, header: bool):
    df = spark.read.csv(filepath, header=header)

    return df