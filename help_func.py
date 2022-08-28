from datetime import datetime, timedelta
import psycopg2
import os
from psycopg2 import Error
from pprint import pprint
from dotenv import load_dotenv
import asyncio
import csv
load_dotenv()


def connections():
    conn = psycopg2.connect(
        host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
        port='6432',
        dbname='market_db',
        user=os.environ['DB_LOGIN'],
        password=os.environ['DB_PASSWORD'],
        target_session_attrs='read-write',
        sslmode='require'
    )
    return conn


def params_date():
    date_now = datetime.now().isoformat(sep='T', timespec='milliseconds') + 'Z'
    date_to = datetime.now()
    delta = timedelta(weeks=4)
    date_to = date_to - delta
    date_to = date_to.isoformat(sep='T', timespec='milliseconds') + 'Z'
    date_dict = {'date_now': date_now, 'date_to': date_to}
    return date_dict


def convert_to_date(str_date: str):
    date = str_date[0:19]
    date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
    return date


def account_data(mp_id: int): # 1- ozon, 2-yandex , 3- WB
    try:
        conn = connections()
        return_dict = {}
        with conn:
            with conn.cursor() as select:
                text_select = f"SELECT * FROM account_list WHERE mp_id = {mp_id}"
                select.execute(text_select)
                select_result_db = select.fetchall()
                if mp_id == 1:
                    for line in select_result_db:
                        if line[9] == 'Active':
                            if line[6] in [values['api_key'] for values in return_dict.values()]:
                                continue
                            else:
                                return_dict[line[0]] = {'client_id_api': line[5], 'api_key': line[6]}
                    return return_dict
                elif mp_id == 2:
                    for line in select_result_db:
                        if line[9] == 'Active':
                            if line[6] in [values['client_id'] for values in return_dict.values()]:
                                continue
                            else:
                                return_dict[line[0]] = {'token': line[5], 'client_id': line[6], 'campaign_id': line[7]}
                    return return_dict
                elif mp_id == 3:
                    for line in select_result_db:
                        if line[9] == 'Active' and line[12] == 'Active':
                            if line[6] in [values['key'] for values in return_dict.values()]:
                                continue
                            else:
                                return_dict[line[0]] = {'api_key': line[5], 'key': line[6]}
                        elif line[9] != 'Active' and line[12] == 'Active':
                            if line[6] in [values['key'] for values in return_dict.values()]:
                                continue
                            else:
                                return_dict[line[0]] = {'key': line[6]}
                        elif line[9] == 'Active' and line[12] != 'Active':
                            if line[5] in [values['api_key'] for values in return_dict.values()]:
                                continue
                            else:
                                return_dict[line[0]] = {'api_key': line[5]}
                    return return_dict
    except (Exception, Error) as E:
        print(f'Error {E}')


feidnames_return_ozon = ['id', 'clearing_id', 'posting_number', 'product_id',
        'sku', 'status', 'returns_keeping_cost', 'return_reason_name',
        'return_date', 'quantity', 'product_name', 'price', 
        'waiting_for_seller_date_time', 'returned_to_seller_date_time', 
        'last_free_waiting_day', 'is_opened', 'place_id', 'commision_percent',
        'commisions', 'price_without_commision', 'is_moving', 
        'moving_to_place_name', 'waiting_for_seller_days', 'picking_amount',
        'accepted_from_customer_moment', 'picking_tag', 'status_name', 
        'returned_to_ozon_moment', 'current_place_name', 'dst_place_name',
        'company_id']

if __name__ == '__main__':
    #res = account_data(3)
    #pprint(res)
    pass
