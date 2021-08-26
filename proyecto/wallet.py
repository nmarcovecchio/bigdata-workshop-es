estado = 'COMPRADO'
estado = 'LIQUIDO'

if signal == 'Compra' and estado == 'LIQUIDO':
    sql_insert_compra(precio)

if signal == 'Venta' and estado == 'COMPRADO':
    sql_inseret_venta(precio)
