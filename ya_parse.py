from help_func import convert_to_date, convert_to_date_ya
from pprint import pprint


async def set_order_ya(order: dict, api_id: str, pool):
    try:
        await pool.execute(
            '''INSERT INTO orders_table (order_id, status, created_at, in_process_at, 
            city, payment_type_group_name, warehouse_id, warehouse_name, api_id, mp_id) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)''', str(order['id']),
            order['status'], convert_to_date_ya(order['creationDate']), convert_to_date(order['statusUpdateDate']),
            order['deliveryRegion']['name'], order['paymentType'],
            str(order['items'][0]['warehouse']['id']), order['items'][0]['warehouse']['name'],
            api_id, 2
        )
    except Exception as E:
        print(f'Error in send ORDER to Data Base Yandex: {E}')


async def set_good_ya(order: dict, pool): 
    try:
        orders = order['items']
        for _ in orders:
            await pool.execute(
                '''INSERT INTO goods_in_orders_table (order_id, sku, 
                unit_name, quantity, offer_id, price, unit_price) 
                VALUES ($1, $2, $3, $4, $5, $6, $7)''', str(order['id']),
                str(_['marketSku']), _['offerName'], _['count'],
                _['shopSku'], _['prices'][0]['total'],
                _['prices'][0]['costPerItem']
            )
    except Exception as E:
        print(f'Error in send GOOD to Data Base Yandex: {E}')


if __name__ == '__main__':
    pass
