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
        host=os.environ['PG_HOST'],
        port=os.environ['PG_PORT'],
        dbname=os.environ['PG_DB'],
        user=os.environ['PG_USER'],
        password=os.environ['PG_PASSWORD'],
        target_session_attrs=os.environ['TARGET_SESSION_ATTRS'],
        sslmode=os.environ['SSLMODE']
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


def convert_to_date_ya(str_date: str):
    date = datetime.strptime(str_date, '%Y-%m-%d')
    return date


def account_data(mp_id: int):  # 1- ozon, 2-yandex , 3- WBFBS, 15 - WBFBO
    try:
        conn = connections()
        return_dict = {}
        with conn:
            with conn.cursor() as select:
                select_get_account_list = f"SELECT id FROM account_list WHERE mp_id = {mp_id} AND status_1 = 'Active'"
                select.execute(select_get_account_list)
                account_id_list = select.fetchall()
                for account_id in account_id_list:
                    select_get_account_data = f"SELECT sa.attribute_name, asd.attribute_value " \
                                              f"FROM account_list al join account_service_data asd " \
                                              f"on al.id = asd.account_id join  service_attr sa on " \
                                              f"asd.attribute_id = sa.id where al.id = {account_id[0]};"
                    select.execute(select_get_account_data)
                    data_account = select.fetchall()
                    for data in data_account:
                        if account_id[0] not in return_dict.keys():
                            return_dict[account_id[0]] = {data[0]: data[1]}
                        return_dict[account_id[0]] = {**return_dict[account_id[0]], **{data[0]: data[1]}}
                return return_dict
    except (Exception, Error) as E:
        print(f'Error in get account data: {E}')


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
    date = params_date()
    pprint(date)