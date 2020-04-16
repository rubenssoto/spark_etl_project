def write_parquet(df, filepath: str):
    df.write.parquet(filepath)