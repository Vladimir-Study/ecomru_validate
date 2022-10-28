from help_func import params_date, account_data, connections
from mp_parser import parse_ozon_fbo, parse_ya, parse_wb_fbs, parse_wb_fbo
from ozon_parse import order_params, goods_in_order_params
from ya_parse import set_order_ya, set_good_ya
from wb_parse import fbs_order_params, fbo_order_params
import datetime
import asyncpg
from status_update import get_status_on_db
from dotenv import load_dotenv
import os
import asyncio
import json

date = params_date()
load_dotenv()


async def send_to_db_ya() -> None:
    try:
        date_now = datetime.datetime.today()
        delta = datetime.timedelta(weeks=4)
        date_to = date_now - delta 
        date_now = date_now.strftime('%d-%m-%Y')
        date_to = date_to.strftime('%d-%m-%Y')
        ya_market_account_list = account_data(2)
        for params in ya_market_account_list.values():
            ya_market_orders = await parse_ya(
                    params['campaign_id'],
                    params['token'],
                    params['client_id'],
                    date_now,
                    date_to
            )
        '''
        async with asyncpg.create_pool(
                host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
                port='6432',
                database='market_db',
                user=os.environ['DB_LOGIN'],
                password=os.environ['DB_PASSWORD'],
                ssl='require'
        ) as pool:
            with open('orders_ya.json', 'r', encoding='utf-8') as file:
                return_orders = json.load(file)
                print(len(return_orders))
                for client_id, orders in return_orders.items():
                    tasks = []
                    pended = 0
                    chunk = 1000
                    for order in orders:
                        task = asyncio.create_task(set_order_ya(order, client_id, pool))
                        tasks.append(task)
                        task = asyncio.create_task(set_good_ya(order, pool))
                        tasks.append(task)
                        if len(tasks) == chunk or pended == len(orders) or len(orders) < chunk:
                            print(f'Numbers of orders for recordins: {len(tasks)}')
                            await asyncio.gather(*tasks)
                            pended += len(tasks)
                            tasks = []
            if os.path.isfile('orders_ozon_fbo.json'):
                os.remove('orders_ozon_fbo.json')
                print('File remove!')
        '''
    except Exception as E:
        print(f'Errors Yandex Market in main file: {E}')


async def send_to_db_wb() -> None:
    try:
        wb_account_list = account_data(3)
        for wb_account in wb_account_list.values():
            if 'api_key' in wb_account:
                await parse_wb_fbs(wb_account['api_key'], date['date_to'], date['date_now'])
            if 'key' in wb_account:
                goods_list = await parse_wb_fbo(wb_account['key'], date['date_to'])
        async with asyncpg.create_pool(
                host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
                port='6432',
                database='market_db',
                user=os.environ['DB_LOGIN'],
                password=os.environ['DB_PASSWORD'],
                ssl='require'
        ) as pool:
            file_name = ['orders_wb_fbo', 'orders_wb_fbs']
            for file in file_name:
                print(file)
                with open(f'{file}.json', 'r', encoding='utf-8') as file:
                    return_orders = json.load(file)
                    print(len(return_orders))
                    for client_id, orders in return_orders.items():
                        print(orders)
                        tasks = []
                        pended = 0
                        chunk = 1000
                        for order in orders:
                            if file == 'orders_wb_fbs':
                                print('FBS')
                                task = asyncio.create_task(fbs_order_params(order, client_id, pool))
                            else:
                                print('FBO')
                                task = asyncio.create_task(fbo_order_params(order, client_id, pool))
                            tasks.append(task)
                            if len(tasks) == chunk or pended == len(orders) or len(orders) < chunk:
                                print(f'Numbers of orders for recordins: {len(tasks)}')
                                await asyncio.gather(*tasks)
                                pended += len(tasks)
                                tasks = []
                if os.path.isfile(f'{file}.json'):
                    os.remove(f'{file}.json')
                    print('File remove!')
    except Exception as E:
        print(f'Errors Wildberries in main file: {E}')


async def send_to_db_ozon_fbo() -> None:
    try:
        account_ozon = account_data(1)
        # асинхронный парсинг возврата заказов Озон FBO
        for data_account in account_ozon.values():
            await parse_ozon_fbo(data_account['client_id_api'],
                                 data_account['api_key'],
                                 date['date_now'],
                                 date['date_to'])
            print(f"End parse {data_account['api_key']}")
        async with asyncpg.create_pool(
                host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
                port='6432',
                database='market_db',
                user=os.environ['DB_LOGIN'],
                password=os.environ['DB_PASSWORD'],
                ssl='require'
        ) as pool:
            with open('orders_ozon_fbo.json', 'r', encoding='utf-8') as file:
                return_orders = json.load(file)
                chunk = 1000
                for api_id, orders in return_orders.items():
                    print(len(orders))
                    tasks = []
                    pended = 0
                    for order in orders:
                        task = asyncio.create_task(goods_in_order_params(order, pool))
                        print(task)
                        tasks.append(task)
                        task = asyncio.create_task(order_params(order, api_id, pool))
                        tasks.append(task)
                        if len(tasks) == chunk or pended == len(orders) or len(orders) < chunk:
                            print(f'Numbers of orders for recordins: {len(tasks)}')
                            await asyncio.gather(*tasks)
                            pended += len(tasks)
                            tasks = []
            async with pool.acquire() as conn:
                conn.execute("DELETE FROM orders_table WHERE ctid IN "
                           "(SELECT ctid FROM (SELECT *, ctid, row_number() OVER "
                           "(PARTITION BY order_id, status ORDER BY id DESC) FROM "
                           "orders_table)s WHERE row_number >= 2)"
                           )
                print('Deletion of duplicates in the table of goods is completed')
                conn.execute("DELETE FROM goods_in_orders_table WHERE ctid IN (SELECT ctid FROM"
                           "(SELECT *, ctid, row_number() OVER (PARTITION BY order_id, sku ORDER BY id DESC) "
                           "FROM goods_in_orders_table)s WHERE row_number >= 2)"
                           )
                print('Deletion of duplicates in the table of orders is completed')
            if os.path.isfile('orders_ozon_fbo.json'):
                os.remove('orders_ozon_fbo.json')
                print('File remove!')
    except Exception as E:
        print(f'Errors Ozon FBO in main file: {E}')


def call_funcs_send_to_db() -> None:
    send_to_db_wb()
    send_to_db_ya()
    send_to_db_ozon_fbo()


def removing_duplicates_orders() -> None:
    conn = connections()
    try:
        with conn:
            with conn.cursor() as select:
                select.execute("DELETE FROM orders_table WHERE ctid IN "
                               "(SELECT ctid FROM (SELECT *, ctid, row_number() OVER "
                               "(PARTITION BY order_id, status ORDER BY id DESC) FROM "
                               "orders_table)s WHERE row_number >= 2)"
                               )
                conn.commit()
                print('Deletion of duplicates in the table of goods is completed')
    except (Exception, Error) as E:
        print(f"Error removing duplicates in goods table: {E}")


def removing_duplicates_goods_in_order() -> None:
    conn = connections()
    try:
        with conn:
            with conn.cursor() as select:
                select.execute("DELETE FROM goods_in_orders_table WHERE ctid IN (SELECT ctid FROM"
                               "(SELECT *, ctid, row_number() OVER (PARTITION BY order_id, sku ORDER BY id DESC) "
                               "FROM goods_in_orders_table)s WHERE row_number >= 2)")
                conn.commit()
                print('Deletion of duplicates in the table of orders is completed')
    except (Exception, Error) as E:
        print(f"Error removing duplicates in orders table: {E}")


if __name__ == '__main__':
    # call_funcs_send_to_db()
    # removing_duplicates_orders()
    # removing_duplicates_goods_in_order()
    asyncio.get_event_loop().run_until_complete(send_to_db_wb())
