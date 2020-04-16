from pyspark.sql.functions import *
from pyspark.sql import Window

def tr_create_state_dim(df):

    df_dim = df.select(col('state').alias('StateName')).drop_duplicates()
    window = Window.orderBy(df_dim.StateName)
    df_dim_final = df_dim.withColumn('StateId', row_number().over(window))

    return df_dim_final