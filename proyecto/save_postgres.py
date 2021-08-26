import sys

from pyspark.sql import SparkSession
import findspark
findspark.init()


def create_spark_session():
    return (
        SparkSession
        .builder
        .appName("Stocks:Stream:ETL")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


if __name__ == "__main__":

    spark = create_spark_session()

    
    json = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", 'kafka:9092')
        .option("subscribe", 'cryptostocks')
        .load()
    )

    stocks = stocks_json.select("content.*")

    print(stocks)