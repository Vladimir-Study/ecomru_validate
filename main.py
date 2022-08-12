from help_func import params_date, account_data, connections
from return_fbo import parse_fbo_return, fbo_send_to_return_table
from ya_parse import parse_ya_order, ya_orders_params
from wb_parse import fbs_order_params, fbo_order_params
from ozon_parse import order_params, goods_in_order_params
from mp_parser import MarketParser
from psycopg2 import Error
from pprint import pprint
from status_update import get_status_on_db 
from return_fbs import main_fbs 
from tqdm import tqdm
import asyncio 


date = params_date()


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
        conn = connections()
        for data_account in account_ozon.values():
            #получение заказов Озон FBO
            ozon_fbo = ozon.parse_ozon_fbo(data_account['client_id_api'],
                                           data_account['api_key'],
                                           date['date_now'],
                                           date['date_to']
                                           )
            #парсинг вазвратов Озон FBO
            response = parse_fbo_return(data_account['client_id_api'], data_account['api_key'])
            for order in tqdm(response['returns']):
                fbo_send_to_return_table(order)
            #парсинг заказов Озон FBO
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
        # асинхронный парсинг возврата заказов Озон FBS        
        asyncio.run(main_fbs(account_ozon))
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
    #call_funcs_send_to_db()
    #removing_duplicates_orders()
    #removing_duplicates_goods_in_order()
    send_to_db_ozon_fbo()
