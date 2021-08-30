import yfinance as yf
import time
import pandas_ta as ta
import pandas as pd
import datetime 
from kafka import KafkaProducer
import json
from datetime import datetime
from pyspark.sql.functions import to_timestamp

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
'''
Para ver si esta corriendo OK.
docker exec -it kafka bash

/opt/kafka_2.11-0.10.1.0/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic cryptostocks --from-beginning
'''

def send_cryptosignal(producer, signal, topic, ticker, price, date):
    producer.send(topic, {'Ticker':ticker,
                            'Signal':signal,
                            'Price':price,
                            'Datetime':date}
    )              

from datetime import datetime


def send_data(producer,df,topic, ticker):
    for index, row in df.iterrows():
        print((row['Datetime'].isoformat()))
        producer.send(topic, {'Ticker':ticker,
                                'Datetime':(row['Datetime'].isoformat())[:-6],
                                'Close':row['Close'],
                                'Open':row['Open'],
                                'High':row['High'],
                                'Volume':row['Volume'],
                                'Low':row['Low'],
                                'MACD_12_26_9':row['MACD_12_26_9'],
                                'MACDh_12_26_9':row['MACDh_12_26_9'],
                                'MACDs_12_26_9':row['MACDs_12_26_9'],
                                'RSI_14':row['RSI_14']}
        )                   

def analize_signal(df, ticker, producer):
    print(df)
    #MACDH<0 La señal 9 se encuentra por debajo.
    #MACDH>0 Punto de compra.
    for i in range(len(df)):
        if(df['MACDh_12_26_9'].iloc[i]>0):
            send_cryptosignal(producer,'Buy','cryptosignal',ticker, df['Close'].iloc[i], df['Datetime'].iloc[i].isoformat())
                        
    # RSI > 70 sobrecompra.
    # RSI > 30 sobreventa
        if(df['RSI_14'].iloc[i]>70):
            send_cryptosignal(producer,'Sell','cryptosignal',ticker, df['Close'].iloc[i], df['Datetime'].iloc[i].isoformat())

if __name__ == '__main__':

    broker = 'kafka:9092'
    topic = 'cryptostocks'
    
    last_date = None

    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
        #Obtenemos los ultimos precios de las ultimas 24 horas, con un intervalo de 1 minuto
        ticker = 'BTC-USD'
        df = yf.Ticker('BTC-USD').history(period='24h',interval='1m')[['Close', 'Open', 'High', 'Volume', 'Low']]#.reset_index()
        df.ta.macd(close='close', fast=12, slow=26, signal=9, append=True)
        df.ta.rsi(close='close',timeperiod=14, append=True)
        #df = df.withColumn("Datetime", to_timestamp("Datetime"))

        #Primera vez que entra al loop obtiene las ultimas 24 horas, y genera señales con esta data
        if last_date == None:
            last_date = df.index[-1]
            df = df.reset_index()
            #print(type(df['Datetime'].iloc[-1]))

            send_data(producer,df,'cryptostocks',ticker)
            analize_signal(df, ticker, producer)
            
        #Hay nueva data disponible? genera señales en tiempo real.
        else:
            new_data = df.loc[df.index > last_date]
            if (len(new_data)>1):
                last_date = new_data.index[-1]
                print(new_data)
                new_data = new_data.reset_index()
                send_data(producer,new_data,'cryptostocks',ticker)
                analize_signal(new_data, ticker, producer)

        time.sleep(1)


    