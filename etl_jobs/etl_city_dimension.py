from pyspark.sql.functions import *
from pyspark.sql import Window

def tr_create_city_dim(df):

    df_dim = df.select(col('county').alias('CityName')).drop_duplicates()
    window = Window.orderBy(df_dim.CityName)
    df_dim_final = df_dim.withColumn('CityId', row_number().over(window))

    return df_dim_final