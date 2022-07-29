from pprint import pprint

from help_func import params_date, account_data
from ya_parse import parse_ya_order, ya_orders_params
from wb_parse import fbs_order_params, fbo_order_params
from ozon_parse import order_params, goods_in_order_params
from ecomru_parser.parser import MarketParser
from tqdm import tqdm
import schedule

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
                    ya_orders_params(ya_market_order['result']['orders'][index], params['token'])
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
    ozon = MarketParser()
    account_ozon = account_data(1)
    for data_account in account_ozon.values():
        ozon_fbo = ozon.parse_ozon_fbo(data_account['client_id_api'],
                                       data_account['api_key'],
                                       date['date_now'],
                                       date['date_to']
                                       )
        order_params(ozon_fbo, data_account['client_id_api'])
        goods_in_order_params(ozon_fbo)


if __name__ == '__main__':
    pass