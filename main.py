from help_func import params_date, account_data, connections
from mp_parser import parse_ozon_fbo, parse_ya, parse_wb_fbs, parse_wb_fbo
from ozon_parse import order_params, goods_in_order_params
from ya_parse import set_order_ya, set_good_ya
from wb_parse import fbs_order_params, fbo_order_params
import datetime
import asyncpg, threading
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
        print(ya_market_account_list)
        for params in ya_market_account_list.values():
            await parse_ya(
                    params['campaigns_id'],
                    params['client_id_api'],
                    params['api_key'],
                    date_now,
                    date_to
            )
        async with asyncpg.create_pool(
                host=os.environ['PG_HOST'],
                port=os.environ['PG_PORT'],
                database=os.environ['PG_DB'],
                user=os.environ['PG_USER'],
                password=os.environ['PG_PASSWORD'],
                ssl=os.environ['SSLMODE']
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
            if os.path.isfile('orders_ya.json'):
                os.remove('orders_ya.json')
                print('File remove!')
        return "END PARSE YANDEX"
    except Exception as E:
        print(f'Errors Yandex Market in main file: {E}')


async def send_to_db_wb() -> None:
    try:
        wb_account_list = account_data(3)
        for wb_account in wb_account_list.values():
            if 'client_id_api' in wb_account:
                await parse_wb_fbs(wb_account['client_id_api'], date['date_to'], date['date_now'])
            if 'api_key' in wb_account:
                await parse_wb_fbo(wb_account['api_key'], date['date_to'])
        async with asyncpg.create_pool(
                host=os.environ['PG_HOST'],
                port=os.environ['PG_PORT'],
                database=os.environ['PG_DB'],
                user=os.environ['PG_USER'],
                password=os.environ['PG_PASSWORD'],
                ssl=os.environ['SSLMODE']
        ) as pool:
            file_name = ['orders_wb_fbo', 'orders_wb_fbs']
            for file in file_name:
                if not os.path.isfile(f'{file}.json'):
                    print(file)
                    continue
                with open(f'{file}.json', 'r', encoding='utf-8') as file:
                    return_orders = json.load(file)
                    print(len(return_orders))
                    for client_id, orders in return_orders.items():
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
        return "END PARSE WB"
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
                host=os.environ['PG_HOST'],
                port=os.environ['PG_PORT'],
                database=os.environ['PG_DB'],
                user=os.environ['PG_USER'],
                password=os.environ['PG_PASSWORD'],
                ssl=os.environ['SSLMODE']
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
                        tasks.append(task)
                        task = asyncio.create_task(order_params(order, api_id, pool))
                        tasks.append(task)
                        if len(tasks) == chunk or pended == len(orders) or len(orders) < chunk:
                            print(f'Numbers of orders for recordins: {len(tasks)}')
                            await asyncio.gather(*tasks)
                            pended += len(tasks)
                            tasks = []
            if os.path.isfile('orders_ozon_fbo.json'):
                os.remove('orders_ozon_fbo.json')
                print('File remove!')
        return "END PARSE OZON"
    except Exception as E:
        print(f'Errors Ozon FBO in main file: {E}')


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


async def main(loop_list: list):
    result = []
    future_one = asyncio.run_coroutine_threadsafe(send_to_db_ya(), loop_list[0])
    result.append(future_one)
    future_two = asyncio.run_coroutine_threadsafe(send_to_db_wb(), loop_list[1])
    result.append(future_two)
    future_three = asyncio.run_coroutine_threadsafe(send_to_db_ozon_fbo(), loop_list[2])
    result.append(future_three)
    try:
        print('RESULT:')
        for future in result:
            res = future.result()
            print(res)
        loop_list[0].call_soon_threadsafe(loop_list[0].stop)
        loop_list[1].call_soon_threadsafe(loop_list[1].stop)
        loop_list[2].call_soon_threadsafe(loop_list[2].stop)
    except Exception as E:
        print(f"Exception in main func")


if __name__ == '__main__':
    loop_list = []
    new_loop_one = asyncio.new_event_loop()
    loop_list.append(new_loop_one)
    new_loop_two = asyncio.new_event_loop()
    loop_list.append(new_loop_two)
    new_loop_three = asyncio.new_event_loop()
    loop_list.append(new_loop_three)
    thread_one = threading.Thread(target=new_loop_one.run_forever)
    thread_two = threading.Thread(target=new_loop_two.run_forever)
    thread_three = threading.Thread(target=new_loop_three.run_forever)
    thread_one.start()
    thread_two.start()
    thread_three.start()
    asyncio.run(main(loop_list))
    # call_funcs_send_to_db()
    # removing_duplicates_orders()
    # removing_duplicates_goods_in_order()
    # asyncio.get_event_loop().run_until_complete(send_to_db_wb())
