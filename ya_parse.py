import requests
from help_func import convert_to_date, connections
import asyncio
import asyncpg


def parse_ya_order(campaign_id: str, token: str, client_id: str, order: list) -> dict:
    url = f'https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/stats/orders.json'
    ya_headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        'Authorization': f'OAuth oauth_token={token}, oauth_client_id={client_id}',
    }
    data = {
        'orders': order,
    }
    response = requests.post(url, headers=ya_headers, json=data)
    return response.json()


async def set_order_ya(order: dict, api_id: str, pool):
    try:
        await pool.execute(
            '''INSERT INTO orders_table (order_id, status, created_at, 
            city, payment_type_group_name, api_id, mp_id) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)''', order['id'], 
            order['status'], order['creationDate'], 
            order['delivery']['region']['name'], order['paymentType'], 
            api_id, 2
        )
    except Exception as E:
        print(f'Error in send ORDER to Data Base Yandex: {E}')


async def set_good_ya(order: dict, pool): 
    try:
        order_prices = order['items'][0]['price']
        if len(order_prices) >= 1:
            for order_price in order_prices:
                if order_price['type'] == 'BUYER':
                    await pool.execute(
                        '''INSERT INTO goods_in_orders_table (order_id, sku, 
                        unit_name, quantity, offer_id, price, unit_price) 
                        VALUES ($1, $2, $3, $4, $5, $6, $7)''', order['id'],
                        order['items'][0]['marketSku'], 
                        order['items'][0]['offerName'], order['items'][0]['count'],
                        order['items'][0]['shopSku'], order_price['total'], 
                        order_price['costPerItem']
                    )
    except Exception as E:
        print(f'Error in send GOOD to Data Base Yandex: {E}')


if __name__ == '__main__':
    pass
