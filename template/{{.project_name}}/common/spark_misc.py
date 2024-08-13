from pyspark.errors.exceptions import captured

import datetime
import logging
import os

logger = logging.getLogger(f'py4j.{__name__}')

def _read_table(sc, path, format):
    """
    Reads a table from the specified path.

    This function attempts to read a table from the specified path using the given format.
    If the path exists and the table is successfully read, it returns the DataFrame.
    Otherwise, it logs a warning and returns None.

    Parameters:
    - sc (SparkContext): The SparkContext object.
    - path (str): The path from which to read the table.
    - format (str): The format of the table.

    Returns:
    - DataFrame or None: The DataFrame containing the table, or None if the table cannot be read.
    """
    try:
        logger.info(f'Verifying Path Exists: {path}')
        return sc.read.format(format).load(path).select("*", "_metadata")
    except captured.AnalysisException as exc:
        logger.warning(f'Reading Table Excpetion {exc}')
        return None

def _extract_tables(sc, path, hot_paths, format):
    """
    Extracts tables from the specified paths and registers them as Spark tables.

    This function iterates over the hot paths, reads tables from each path using the specified format,
    and merges them into a single DataFrame. It then registers this DataFrame as a temporary Spark table.

    Parameters:
    - sc (SparkContext): The SparkContext object.
    - path (str): The base path containing the tables.
    - hot_paths (list): List of paths to extract tables from.
    - format (str): The format of the tables.

    """
    df = None
    for p in hot_paths:
        subset_df = _read_table(sc, p, format)
        if not df and subset_df:
            df = subset_df
        elif subset_df:
            df.union(subset_df)

    if df:
        logger.info(f'Registering Spark Table {os.path.split(path)[-1]}')
        df.createOrReplaceTempView(os.path.split(path)[-1])
