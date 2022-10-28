from help_func import convert_to_date


async def fbo_order_params(order: dict, api_key: str, pool) -> None:
    try:
        await pool.execute('''INSERT INTO orders_table (order_id, created_at, 
                           region, warehouse_name, api_id, mp_id) 
                           VALUES ($1, $2, $3, $4, $5, $6)''', order['gNumber'],
                           convert_to_date(order['date']), order['oblast'],
                           order['warehouseName'], api_key, 3
                           )
        await pool.execute('''INSERT INTO goods_in_orders_table (order_id, sku, offer_id, price)
                           VALUES ($1, $2, $3, $4)''', order['gNumber'], str(order['nmId']),
                           order['supplierArticle'], order['totalPrice']
                           )
    except (Exception) as E:
        print(f'Error in send FBO orders WB in DB: {E}')


async def fbs_order_params(order: dict, api_key: str, pool) -> None:
    try:
        await pool.execute(
            '''INSERT INTO orders_table (order_id, created_at, city, delivery_type,
            warehouse_id, api_id, mp_id) VALUES ($1, $2, $3, $4, $5, $6, $7)''',
            order['orderId'], convert_to_date(order['dateCreated']), 
            order['deliveryAddressDetails']['city'], order['deliveryType'],
            order['wbWhId'], api_key, 3
        )
        await pool.execute(
            '''INSERT INTO goods_in_orders_table (order_id, sku) 
            VALUES ($1, $2)''', order['orderId'], order['chrtId']
        )
    except Exception as E:
        print(f'Error in send FBS orders WB in DB: {E}')


if __name__ == '__main__':
    pass
