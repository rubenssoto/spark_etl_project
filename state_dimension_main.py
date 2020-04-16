from etl_jobs.etl_state_dimension import *
from utilitys.file_readers import read_csv
from utilitys.file_writes import write_parquet
from configs.start import start_spark

def main():

    spark = start_spark('Covid_State_Dimension')
    data = read_csv(spark, 'us-counties.txt', True)
    transformed_data = tr_create_state_dim(data)
    write_parquet(transformed_data, 'StateDim.parquet')


if __name__ == '__main__':
    main()
