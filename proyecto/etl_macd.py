import yfinance as yf
import time
import pandas_ta as ta
import pandas as pd
import datetime 
from kafka import KafkaProducer
import json

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


if __name__ == '__main__':

    broker = 'kafka:9092'
    topic = 'cryptostocks'
    
    last_date = None

    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
        #Obtenemos los ultimos precios de las ultimas 24 horas, con un intervalo de 1 minuto
        df = yf.Ticker('BTC-USD').history(period='24h',interval='1m')[['Close', 'Open', 'High', 'Volume', 'Low']]#.reset_index()
        df.ta.macd(close='close', fast=12, slow=26, signal=9, append=True)
        df.ta.rsi(close='close',timeperiod=14, append=True)

        #Primera vez que entra al loop
        if last_date == None:
            print(df)
            producer.send('cryptostocks', (df.reset_index()).to_json(orient = 'columns'))
            last_date = df.index[-1]
        
        #Hay nueva data disponible?
        else:
            new_data = df.loc[df.index > last_date]
            if (len(new_data)>1):
                last_date = df.index[-1]
                print(new_data)
                producer.send('cryptostocks', (new_data.reset_index()).to_json(orient = 'columns'))
                #Envio señal insertar en DB.
                if(new_data['MACDh_12_26_9'].iloc[-1]>0):
                    print('Compro a '+str(new_data['Close'].iloc[-1]))
                    #Envio señal de compra.
                    
                # > 70 sobrecompra.
                # > 30 sobreventa
                if(new_data['RSI_14'].iloc[-1]>70):
                    print('Vendo a '+str(new_data['Close'].iloc[-1]))
                    #Envio señal de venta.

        time.sleep(1)


    