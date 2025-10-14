from pyspark.sql import DataFrame 

from pyspark.sql.functions import current_timestamp, col


def add_processed_timestamp(df: DataFrame) -> DataFrame:
    """
    Adds a 'processed_timestamp column to the DataFrame with the current_timestamp

    Parameters: 
    df (DataFrame): The input DataFrame

    Returns:
    DataFrame with an additional 'current_timestamp' column
    """
    return df.withColumn("processed_timestamp", current_timestamp())


def add_file_name(df: DataFrame) -> DataFrame:
    """
    Adds a 'source_file_name column to the DataFrame with the file name

    Parameters: 
    df (DataFrame): The input DataFrame

    Returns:
    DataFrame with an additional 'source_file_name' column
    """
    return df.withColumn("source_file_name", col("_metadata.file_name"))


