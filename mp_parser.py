import requests
import asyncio 
import aiohttp
from help_func import params_date
import json
import os
from datetime import datetime


async def write_to_file(file_name: str, client_id: str, orders) -> None:
    len_orders = len(orders)
    if not os.path.isfile(f'{file_name}.json'):
        print('Not file')
        with open(f'{file_name}.json', 'w', encoding='utf-8') as outfile:
            api_orders = {client_id: orders}
            print(f'Create {len_orders}')
            json.dump(api_orders, outfile, ensure_ascii=False, indent=4)
    else:
        print('File is create')
        with open(f'{file_name}.json', 'r', encoding='utf-8') as file:
            returns = json.load(file)
            if client_id not in returns.keys():
                with open(f'{file_name}.json', 'w', encoding='utf-8') as outfile:
                    api_orders = {client_id: orders}
                    returns = {**returns, **api_orders}
                    print(f'Write {len_orders}')
                    json.dump(returns, outfile, ensure_ascii=False, indent=4)
            else:
                add_orders = [*returns[client_id], *orders]
                api_returns = {client_id: add_orders}
                returns = {**returns, **api_returns}
                print(f'Write {len(returns)}')
                with open('orders_ya.json', 'w', encoding='utf-8') as outfile:
                    json.dump(returns, outfile, ensure_ascii=False, indent=4)


headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
    'Content-Type': 'application/json'
}


async def parse_ozon_fbo(
        client_id: str, 
        api_key:str, 
        date_now, 
        date_to, 
        limit: int = 1000):
    url = 'https://api-seller.ozon.ru/v2/posting/fbo/list'
    ozon_headers = {
        'Client-Id': client_id,
        'Api-Key': api_key,
    }
    ozon_headers = {**headers, **ozon_headers}
    async with aiohttp.ClientSession() as session:
        try:
            response_count = 0
            len_orders = 1000
            parse_count = 0
            while response_count != 10 and len_orders == 1000:
                print('Start while')
                offset = 0
                print(f'Offset: {offset}')
                data = {
                    "dir": "asc",
                    "filter": {
                        "since": date_to,
                        "status": "",
                        "to": date_now
                    },
                    "limit": limit,
                    "offset": offset,
                    "translit": True,
                    "with": {
                        "analytics_data": True,
                        "financial_data": True
                    }
                }
                async with session.post(url, headers=ozon_headers, json=data) as response:
                    orders = await response.json()
                    if response.status == 200:
                        print('Status code 200!')
                        len_orders = len(orders['result'])
                        offset += len_orders 
                        print(offset)
                        parse_count += len_orders 
                        print(f"Number of ofders for parsing: {parse_count}")
                        await write_to_file('orders_ozon_fbo', api_key, orders['result'])
                        '''
                        if not os.path.isfile('orders_ozon_fbo.json'):
                            print('Not file')
                            with open('orders_ozon_fbo.json', 'w', encoding='utf-8') as outfile:
                                api_orders = {api_key: orders['result']}
                                print(f'Create {len_orders}')
                                json.dump(api_orders, outfile, ensure_ascii=False, indent=4)
                        else:
                            print('File is create')
                            with open('orders_ozon_fbo.json', 'r', encoding='utf-8') as file:
                                returns = json.load(file)
                                if api_key not in returns.keys():
                                    with open('orders_ozon_fbo.json', 'w', encoding='utf-8') as outfile:
                                        api_orders = {client_id: orders['result']}
                                        returns = {**returns, **api_orders}
                                        print(f'Create {len_orders}')
                                        json.dump(api_orders, outfile, ensure_ascii=False, indent=4)
                                else:
                                    api_returns = {api_key: returns}
                                    returns = {**returns, **api_orders}
                                    returns = [*returns[api_key], *orders['result']]
                                    print(f'Write {len(returns)}')
                                    with open('orders_ozon_fbo.json', 'w', encoding='utf-8') as outfile:
                                        json.dump(api_returns, outfile, ensure_ascii=False, indent=4)
                        '''
                    else:
                        response_count += 1
        except Exception as E:
            print(f'Errors in get orders OZON FBO: {E}')


async def parse_ozon_fbs(
        client_id: str, 
        api_key:str, 
        date_now, 
        date_to, 
        limit: int = 1000,
        offset: int = 0):
    url = 'https://api-seller.ozon.ru/v3/posting/fbs/list'
    ozon_headers = {
        'Client-Id': client_id,
        'Api-Key': api_key,
    }
    ozon_headers = {**headers, **ozon_headers}
    async with aiohttp.ClientSession() as session:
        try:
            response_count = 0
            len_orders = 1000
            while response_count != 10 and len_orders == 1000:
                data = {
                    "dir": "ASC",
                    "filter": {
                        "since": date_to,
                        "status": "awaiting_packaging",
                        "to": date_now
                    },
                    "limit": limit,
                    "offset": offset,
                    "translit": True,
                    "with": {
                        "analytics_data": True,
                        "financial_data": True
                    }
                }
                async with session.post(url, headers=ozon_headers, json=data) as response:
                    orders = await response.json()
                    if response.status == 200:
                        offset += 1
                        len_orders = len(orders['result']['postings'])
                        print(f"Number of ofders for parsing: {len(orders['result']['postings'])}")
                        if not os.path.isfile('orders_ozon_fbs.json'):
                            with open('orders_ozon_fbs.json', 'w', encoding='utf-8') as outfile:
                                set_orders = orders['result']['postings']
                                json.dump(set_orders, outfile, ensure_ascii=False, indent=4)
                        else:
                            with open('orders_ozon_fbs.json', 'r', encoding='utf-8') as file:
                                returns = json.load(file)
                                set_orders = orders['result']['postings']
                                returns = [*returns, *set_orders]
                                with open('orders_ozon_fbs.json', 'w', encoding='utf-8') as outfile:
                                    json.dump(returns, outfile, ensure_ascii=False, indent=4)
                    else:
                        response_count += 1
        except Exception as E:
            print(f'Errors in get orders OZON FBS {E}')


async def parse_wb_fbs(
        api_key: str, 
        date_start: str, 
        date_end: str, 
        take: int = 1000):
    url = 'https://suppliers-api.wildberries.ru/api/v2/orders'
    wb_headers = {
        'accept': 'application / json',
        'Authorization': api_key,
    }
    wb_headers = {**headers, **wb_headers}
    async with aiohttp.ClientSession() as session:
        try:
            response_count = 0
            len_orders = 1000
            skip = 0
            while response_count != 10 and len_orders == 1000:
                params = {
                    'date_start': date_start,
                    'date_end': date_end,
                    'take': take,
                    'skip': skip
                }
                async with session.get(url, headers=wb_headers, params=params) as response:
                    orders = await response.json()
                    if response.status == 200:
                        len_orders = len(orders['orders'])
                        skip += len_orders
                        print(f"Number of ofders for parsing: {len_orders}")
                        await write_to_file('orders_wb_fbs', api_key, orders['orders'])
                    else:
                        response_count += 1
        except Exception as E:
            print(f'Errors in get orders WB FBS {E}')


async def parse_wb_fbo(
        key: str, 
        date_from: str, 
        flags: int = 0):
    url = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/orders'
    async with aiohttp.ClientSession() as session:
        try:
            response_count = 0
            flag = True
            while response_count != 10 and flag != False:
                params = {
                    'key': key,
                    'dateFrom': date_from,
                    'flag': flags
                    }
                async with session.get(url, headers=headers, params=params) as response:
                    orders = await response.json()
                    if response.status == 200:
                        flag = False
                        len_orders = len(orders)
                        print(f"Number of ofders for parsing: {len_orders}")
                        await write_to_file('orders_wb_fbo', key, orders)
                    else:
                        response_count += 1
        except Exception as E:
            print(f'Errors in get orders WB FBO {E}')
            

async def parse_ya(
        campaign_id: str,
        token: str, 
        client_id: str,
        fromDate: str,
        toDate: str,
        ):
    url = f'https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/orders.json'
    print(campaign_id)
    ya_headers = {
        'Authorization': f'OAuth oauth_token={token}, oauth_client_id={client_id}',
        'Accept': 'application/json',
    }
    ya_headers = {**headers, **ya_headers}
    async with aiohttp.ClientSession() as session:
        try:
            response_count = 0
            page = 1
            current_page = 10
            while response_count != 10 and page <= current_page:
                params = {
                    'fromDate': fromDate,
                    'toDate': toDate,
                    'page': page 
                        } 
                async with session.get(url, headers=ya_headers, params=params) as response:
                    orders = await response.json()
                    if 'errors' in orders.keys():
                        print(orders['errors'][0]['message'])
                    elif len(orders['orders']) == 0:
                        print('Numbers of orders in 0')
                    if response.status == 200:
                        current_page = orders['pager']['pagesCount'] 
                        page = orders['pager']['currentPage'] + 1
                        print(f"Number of ofders for parsing: {len(orders['orders'])}")
                        await write_to_file('orders_ya', client_id, orders['orders'])
                    else:
                        response_count += 1
        except Exception as E:
            print(f'Errors in get orders Yandex {E}')


if __name__ == '__main__':
    pass
