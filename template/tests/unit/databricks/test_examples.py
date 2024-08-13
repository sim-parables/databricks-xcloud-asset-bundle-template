import argparse
import pytest

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv_path', help="CSV Path", type= str)
    return parser.parse_known_args()

@pytest.mark.databricks
@pytest.mark.spark
@pytest.mark.examples
@pytest.mark.usefixtures('spark')
def test_holdings_spark(spark):
    """
    Test case for evaluating PySpark query with Unity Catalog Volume Files

    Read the Holdings CSV from Unity Catalog and summarize the
    highest share price by ticker name.
    """
    args, unknown_args = _parse_args()
    df = spark.read.format('csv').load(args.csv_path, header=True)
    df.createOrReplaceTempView('data')
    
    rs = spark.sql(f"""
        SELECT ticker, MAX(shares) max_share_price
        FROM data
        GROUP BY ticker;
    """)

    assert rs.count() > 0