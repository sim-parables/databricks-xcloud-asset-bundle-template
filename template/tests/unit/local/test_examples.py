import argparse
import pyspark
import pytest
import os

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv_path', help="CSV Path", type= str)
    return parser.parse_known_args()

@pytest.mark.local
@pytest.mark.duckdb
@pytest.mark.examples
@pytest.mark.usefixtures('duckdb_session')
def test_holdings_duckdb(duckdb_session):
    """
    Test case for evaluating DuckDB HTTPS query

    Read the Holdings CSV via HTTPFS and summarize the
    highest share price by ticker name.
    """
    args, unknown_args = _parse_args()
    rs = duckdb_session.sql(f"""
        SELECT ticker, MAX(shares) max_share_price
        FROM READ_CSV('{args.csv_path}')
        GROUP BY ticker;
    """)

    df = rs.df()
    assert True



@pytest.mark.local
@pytest.mark.spark
@pytest.mark.examples
@pytest.mark.usefixtures('spark')
def test_holdings_spark(spark):
    """
    Test case for evaluating PySpark HTTP query

    Read the Holdings CSV via HTTPFS and summarize the
    highest share price by ticker name.
    """
    args, unknown_args = _parse_args()
    spark.sparkContext.addFile(args.csv_path)

    df = spark.read.csv(f'file://{pyspark.SparkFiles.get(os.path.split(args.csv_path)[-1])}', header=True)
    df.createOrReplaceTempView('data')
    rs = spark.sql(f"""
        SELECT ticker, MAX(shares) max_share_price
        FROM data
        GROUP BY ticker;
    """)

    assert rs.count() > 0

