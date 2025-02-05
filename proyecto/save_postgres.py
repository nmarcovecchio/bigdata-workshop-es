import sys

'''
psql -U workshop
'''

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    DoubleType,
    StructType,
    StructField,
    TimestampType,
    IntegerType
)

def define_write_to_postgres(table_name):

    def write_to_postgres(df, epochId):
        print(f"Bacth (epochId): {epochId}")
        return (
            df.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres/workshop")
            .option("dbtable", f"workshop.{table_name}")
            .option("user", "workshop")
            .option("password", "w0rkzh0p")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
    return write_to_postgres

def stream_to_postgres(stocks, output_table="cryptostocks"):
    wstocks = (
        stocks
        #.withWatermark("timestamp", "60 seconds")
        .select("ticker", "datetime", "close","open", "high", "volume", "macd_12_26_9", "macdh_12_26_9", "macds_12_26_9", "rsi_14")
    )

    write_to_postgres_fn = define_write_to_postgres("cryptostocks")

    query = (
        wstocks
        .writeStream
        .foreachBatch(write_to_postgres_fn)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    return query

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Stocks:Save_postgres")
        .config("spark.driver.memory", "512m")
        .config("spark.driver.cores", "1")
        .config("spark.executor.memory", "512m")
        .config("spark.executor.cores", "1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    return spark

#spark-submit   --master 'spark://master:7077'   --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --jars /app/postgresql-42.1.4.jar   --total-executor-cores 1   /proyecto/save_postgres.py
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

    json.printSchema()

    schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("datetime", TimestampType(), False),
        #StructField("Datetime", StringType(), False),
        StructField("close", DoubleType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("volume", IntegerType(), False),
        StructField("macd_12_26_9", DoubleType(), False),
        StructField("macdh_12_26_9", DoubleType(), False),
        StructField("macds_12_26_9", DoubleType(), False),
        StructField("rsi_14", DoubleType(), False),
    ])

    json_options = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss"}
    stocks_json = json.select(
        F.from_json(F.col("value").cast("string"), schema, json_options).alias("content")
    )

    stocks_json.printSchema()
    stocks = stocks_json.select("content.*")

    query3 = stream_to_postgres(stocks)
    query3.awaitTermination()