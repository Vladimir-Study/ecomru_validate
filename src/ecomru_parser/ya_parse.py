import requests
from psycopg2 import Error
from help_func import convert_to_date, connections


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


def ya_orders_params(order: dict, api_id: str) -> None:
    try:
        ya_conn = connections()
        with ya_conn:
            with ya_conn.cursor() as select:
                select.execute(
                    f"INSERT INTO orders_table (order_id, status, created_at, in_process_at,"
                    f"city, payment_type_group_name, warehouse_id, warehouse_name, api_id, mp_id) "
                    f"VALUES ('{order['id']}', '{order['status']}', "
                    f"'{order['creationDate']}', '{convert_to_date(order['statusUpdateDate'])}', "
                    f"'{order['deliveryRegion']['name']}', '{order['paymentType']}', '{order['items'][0]['warehouse']['id']}', "
                    f"'{order['items'][0]['warehouse']['name']}', '{api_id}', 2)"
                )
                ya_conn.commit()
                order_prices = order['items'][0]['prices']
                if len(order_prices) >= 1:
                    for order_price in order_prices:
                        if order_price['type'] == 'BUYER':
                            select.execute(
                                f"INSERT INTO goods_in_orders_table (order_id, sku, unit_name, quantity, "
                                f"offer_id, price, unit_price)"
                                f"VALUES ('{order['id']}', '{order['items'][0]['marketSku']}', '{order['items'][0]['offerName']}', "
                                f"{order['items'][0]['count']}, '{order['items'][0]['shopSku']}', {order_price['total']}, "
                                f"{order_price['costPerItem']})"
                            )
                            ya_conn.commit()
    except (Exception, Error) as E:
        print(f'Error: {E}')


if __name__ == '__main__':
    pass