from help_func import params_date, account_data, connections
from return_fbo import get_chunk, fbo_send_to_return_table
from ya_parse import parse_ya_order, ya_orders_params
from wb_parse import fbs_order_params, fbo_order_params
from mp_parser import MarketParser
from psycopg2 import Error
import asyncpg
from status_update import get_status_on_db
from return_fbs import get_chunk_fbs, get_satus_order, fbs_send_to_return_table
from dotenv import load_dotenv
import os
import asyncio
import json

date = params_date()
load_dotenv()


def send_to_db_ya() -> None:
    try:
        ya_market_account_list = account_data(2)
        ya_market = MarketParser()
        for params in ya_market_account_list.values():
            ya_market_orders = ya_market.parse_ya(
                params['campaign_id'],
                params['token'],
                params['client_id']
            )
            if 'errors' in ya_market_orders.keys():
                print(ya_market_orders['errors'][0]['message'])
            elif len(ya_market_orders['orders']) == 0:
                print('Numbers of orders in 0')
            else:
                dict_number_order = [order['id'] for order in ya_market_orders['orders']]
                ya_market_order = parse_ya_order(
                    params['campaign_id'],
                    params['token'],
                    params['client_id'],
                    order=dict_number_order
                )
                for index in range(len(dict_number_order)):
                    print(f"Sending to DataBase order number: {dict_number_order[index]}")
                    old_status = get_status_on_db(dict_number_order[index])
                    status_now = ya_market_order['result']['orders'][index]['status']
                    if old_status is None or old_status != status_now:
                        ya_orders_params(ya_market_order['result']['orders'][index], params['token'])
                        print('Write')
                    elif old_status == status_now:
                        print('Rewrite')
                        continue
    except Exception as E:
        print(f'Errors Yandex Market in main file: {E}')


def send_to_db_wb() -> None:
    try:
        wb_account_list = account_data(3)
        wb = MarketParser()
        for wb_account in wb_account_list.values():
            if 'api_key' in wb_account:
                goods_list = wb.parse_wb_fbs(wb_account['api_key'], date['date_to'], date['date_now'])
                for goods in goods_list['orders']:
                    fbs_order_params(goods, wb_account['api_key'])
            if 'key' in wb_account:
                goods_list = wb.parse_wb_fbo(wb_account['key'], date['date_to'])
                for goods in goods_list:
                    fbo_order_params(goods, wb_account['key'])
    except Exception as E:
        print(f'Errors Wildberries in main file: {E}')


def send_to_db_ozon_fbo() -> None:
    try:
        ozon = MarketParser()
        account_ozon = account_data(1)
        '''
        conn = connections()
        for data_account in account_ozon.values():
            # получение заказов Озон FBO
            ozon_fbo = ozon.parse_ozon_fbo(data_account['client_id_api'],
                                           data_account['api_key'],
                                           date['date_now'],
                                           date['date_to']
                                           )
            # парсинг вазвратов Озон FBO
            response = parse_fbo_return(data_account['client_id_api'], data_account['api_key'])
            for order in tqdm(response['returns']):
                fbo_send_to_return_table(order)
            # парсинг заказов Озон FBO
            for order in ozon_fbo['result']:
                old_status = get_status_on_db(order['order_id'])
                status_now = order['status']
                if old_status is None or old_status != status_now:
                    order_params(order, data_account['client_id_api'])
                    goods_in_order_params(order)
                    print('rewrite')
                elif old_status == status_now:
                    print('continue')
                    continue
        '''
        # асинхронный парсинг возврата заказов Озон FBS
        async def returns_orders(account_list: dict) -> None:
            await get_chunk(account_list)
            await get_chunk_fbs(account_list)
            async with asyncpg.create_pool(
                    host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
                    port='6432',
                    database='market_db',
                    user=os.environ['DB_LOGIN'],
                    password=os.environ['DB_PASSWORD'],
                    ssl='require'
            ) as pool:
                status_in_db = await get_satus_order(pool)
                with open('returns_ozon.json', 'r', encoding='utf-8') as file:
                    return_orders = json.load(file)
                    tasks = []
                    chunk = 100
                    for order in return_orders['returns']:
                        if len(order) == 11:
                            task = await fbo_send_to_return_table(order, status_in_db, pool)
                            tasks.append(task)
                        elif len(order) == 26:
                            task = await fbs_send_to_return_table(order, status_in_db, pool)
                            tasks.append(task)
                    while len(tasks) != 0:
                        print(f'Numbers of orders for recordins: {len(tasks)}')
                        chunk_tasks = tasks[:chunk]
                        await asyncio.gather(*chunk_tasks)
                        tasks = tasks[chunk:]
                    return_orders['returns'].clear()
                    with open('returns_ozon.json', 'w', encoding='utf-8') as outfile:
                        json.dump(return_orders, outfile, ensure_ascii=False, indent=4)
                async with pool.acquire() as conn:
                    conn.execute('DELETE FROM return_table WHERE ctid IN (SELECT ctid FROM(SELECT *, ctid, '
                                 'row_number() OVER (PARTITION BY return_id ORDER BY id DESC) FROM return_table)s '
                                 'WHERE row_number >= 2)')
        asyncio.get_event_loop().run_until_complete(returns_orders(account_ozon))
    except (Exception, Error) as E:
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
    send_to_db_ozon_fbo()
