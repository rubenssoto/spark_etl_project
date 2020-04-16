from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import IntegerType


def tr_join_state_city_covid_fact(df, df_state, df_city):
    df_join_city = df.join(df_city, df.county == df_city.CityName).select(df.date, df.state, df.cases, df.deaths,
                                                                          df_city.CityId)
    df_join_state = df_join_city.join(df_state, df.state == df_state.StateName).select(df.date, df.cases, df.deaths,
                                                                                       df_state.StateId,
                                                                                       df_join_city.CityId)

    return df_join_state


def tr_group_covid_fact(df):
    df_grouped = df.groupBy('StateId', 'CityId').agg(sum('cases').alias('Cases'), sum('deaths').alias('Deaths'))
    df_cast = df_grouped.select('StateId', 'CityId', col('Cases').cast(IntegerType()),
                                col('Deaths').cast(IntegerType()))

    return df_cast