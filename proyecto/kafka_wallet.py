from kafka import KafkaConsumer
import json
import findspark
from datetime import datetime

'''
spark-submit   --master 'spark://master:7077'   --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 \
--jars /app/postgresql-42.1.4.jar   --total-executor-cores 1   /proyecto/kafka_wallet.py
'''

findspark.add_jars('/app/postgresql-42.1.4.jar')
findspark.init()

#import spark.implicits._

from pyspark.sql.types import (
    StringType,
    DoubleType,
    StructType,
    StructField,
    TimestampType,
)


from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("pyspark-Savepostgres")
    .config("spark.driver.memory", "512m")
    .config("spark.driver.cores", "1")
    .config("spark.executor.memory", "512m")
    .config("spark.executor.cores", "1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)


def write_postgres(df, table_name):



    (df.write 
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres/workshop")
            .option("dbtable", f"workshop.{table_name}")
            .option("user", "workshop")
            .option("password", "w0rkzh0p")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save())

def generate_df(data):
    schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("datetime", TimestampType(), False),
        StructField("result", DoubleType(), False),
        StructField("tipo", StringType(), False),
    ])
    df = spark.createDataFrame(data,schema)
    return df

consumer = KafkaConsumer('cryptosignal',bootstrap_servers='kafka:9092',
value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.poll()


estado = 'LIQUIDO'
precio_compra=0
precio_venta=0
ganancia = 0

wait = 0
for msg in consumer:
    msg = msg.value
    print(msg)
    
    #Por si me llega una señal de compra y venta a la vez.
    #Compro, y espero por lo menos 2 ciclos mas para ver si vendo.
    if (wait):
        wait-=1

    if (msg['Signal']=='Buy' and estado =='LIQUIDO'):
        wait = 2
        precio_compra = msg['Price']
        estado = 'COMPRADO'
        print('Compro a '+str(precio_compra))
        date = datetime.strptime(msg['Datetime'], '%Y-%m-%dT%H:%M:%S%z')
        df = generate_df([{'ticker':msg['Ticker'], 'result':0.0, 'datetime':date, 'tipo':'Buy'}])
        write_postgres(df, 'wallet')
    
    if (msg['Signal']=='Sell' and estado =='COMPRADO' and wait==0):
        precio_venta = msg['Price']
        ganancia += precio_venta - precio_compra 
        print('He comprado a  '+str(precio_compra))
        print('Vendo a  '+str(precio_venta))
        print('Gano' + str(ganancia))
        estado = 'LIQUIDO'
        print(ganancia)
        date = datetime.strptime(msg['Datetime'], '%Y-%m-%dT%H:%M:%S%z')
        df = generate_df([{'ticker':msg['Ticker'], 'result':(precio_venta*100.0/precio_compra)-100.0, 'datetime':date, 'tipo':'Sell'}])
        write_postgres(df, 'wallet')

        # SQL SAVE WALLET BUY & SELLS.