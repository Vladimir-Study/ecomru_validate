from dotenv import load_dotenv
from pprint import pprint
from psycopg2 import Error
import psycopg2
import requests
import os
import json
load_dotenv()


def connections():
    conn = psycopg2.connect(
        host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
        port='6432',
        dbname='market_db',
        user=os.environ['DB_LOGIN'],
        password=os.environ['DB_PASSWORD'],
        target_session_attrs='read-write',
        sslmode='verify-full'
    )
    return conn


def parse_fbo_return(client_id: str, api_key: str, limit: int = 10) -> dict:
    url = 'https://api-seller.ozon.ru/v2/returns/company/fbo'
    ozon_headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                    '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        'Client-Id': client_id,
        'Api-Key': api_key,
    }
    data = {
        "limit": limit,
        "offset": 0,
    }
    response = requests.post(url, json=data, headers=ozon_headers)
    return response.json()


def fbo_send_to_return_table(order: dict) -> None:
    try:
        conn = connections()
        with conn:
            with conn.cursor() as select:
                select.execute(f"SELECT * FROM return_table WHERE return_id = '{order['id']}'")
                order_in_table = select.fetchone()
                if order_in_table is None:
                    select.execute(
                        f"INSERT INTO return_table (return_id, posting_number, sku, status, return_reason_name, " 
                        f"return_date, waiting_for_seller_date_time, is_opened, moving_to_place_name) "
                        f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (order['id'], order['posting_number'], order['sku'],
                        order['status_name'], order['return_reason_name'], order['accepted_from_customer_moment'],
                        order['returned_to_ozon_moment'], order['is_opened'], order['dst_place_name'])
                    )
                    conn.commit()
    except (Exception, Error) as E:
        print(f"Error in send to return table fbo: {E}")

if __name__ == '__main__':
    # response = parse_fbo_return(os.environ['OZON_CLIENT_ID'], os.environ['OZON_API_KEY'])
    # pprint(response)
    pass