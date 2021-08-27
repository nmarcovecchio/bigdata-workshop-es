from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('cryptosignal',bootstrap_servers='kafka:9092',
value_deserializer=lambda m: json.loads(m.decode('utf-8')))

estado = 'LIQUIDO'
precio_compra=0
precio_venta=0
ganancia = 0

for msg in consumer:
    msg = msg.value
    print(msg)
    if (msg['Signal']=='Buy' and estado =='LIQUIDO'):
        precio_compra = msg['Price']
        estado = 'COMPRADO'
        print('Compro a '+str(precio_compra))
    
    if (msg['Signal']=='Sell' and estado =='COMPRADO'):
        precio_venta = msg['Price']
        ganancia += precio_venta - precio_compra 
        print('He comprado a  '+str(precio_compra))
        print('Vendo a  '+str(precio_venta))
        print('Gano' + str(ganancia))
        estado = 'LIQUIDO'
        print(ganancia)

        # SQL SAVE WALLET BUY & SELLS.