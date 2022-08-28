from dotenv import load_dotenv
from pprint import pprint
from psycopg2 import Error
from help_func import account_data
from mp_parser import MarketParser
import threading
import psycopg2
import requests
import os
import json
import asyncio
import asyncpg
import aiohttp
from datetime import datetime

load_dotenv()


async def parse_fbo_return_in_json(
        client_id: str,
        api_key: str,
        limit: int = 100,
):
    url = 'https://api-seller.ozon.ru/v2/returns/company/fbo'
    ozon_headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        'Client-Id': client_id,
        'Api-Key': api_key,
    }
    data = {
        "limit": limit,
        "offset": 0,
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url,
                                    json=data,
                                    headers=ozon_headers
                                    ) as response:
                orders = await response.json()
                print(f"Number of ofders for parsing: {len(orders['returns'])}")
                with open('returns_ozon.json', 'r', encoding='utf-8') as file:
                    returns = json.load(file)
                    for _ in orders['returns']:
                        returns['returns'].append(_)
                    with open('returns_ozon.json', 'w', encoding='utf-8') as outfile:
                        json.dump(returns, outfile, ensure_ascii=False, indent=4)
        except Exception as E:
            print(f'Exception get return orders ozon FBO : {E}')


async def make_request_insert_in_db_fbo(pool, row):
    insert = await pool.execute('''INSERT INTO return_table (return_id, 
    posting_number, sku, status, return_reason_name, return_date, 
    waiting_for_seller_date_time, is_opened, moving_to_place_name) VALUES 
    ($1, $2, $3, $4, $5, $6, $7, $8, $9)''', str(row['id']), row['posting_number'],
                                str(row['sku']), row['status_name'], row['return_reason_name'],
                                None if row['accepted_from_customer_moment'] == '' or 'null' else datetime.strptime(
                                    row['accepted_from_customer_moment'][:19], '%Y-%m-%dT%H:%M:%S'),
                                None if row['returned_to_ozon_moment'] == '' or 'null' else datetime.strptime(
                                    row['returned_to_ozon_moment'][:19], '%Y-%m-%dT%H:%M:%S'),
                                str(row['is_opened']), row['dst_place_name']
                                )
    return insert


async def make_request_update_in_db_fbo(pool, row):
    update = await pool.execute('''UPDATE return_table SET status = $1 
            WHERE return_id = $2''', row['status_name'], str(row['id']))
    return update


async def fbo_send_to_return_table(return_order: dict, status_in_db, pool) -> None:
    try:
        if return_order['id'] not in status_in_db.keys():
            task = asyncio.create_task(make_request_insert_in_db_fbo(pool, return_order))
        elif return_order['id'] in status_in_db.keys() and \
                status_in_db['status'] != return_order['status_name']:
            task = asyncio.create_task(make_request_update_in_db_fbo(pool, return_order))
        return task
    except (Exception) as E:
        print(f"Error in send to return table fbo: {E}")


async def get_chunk(accounts_list: dict) -> None:
    tasks = []
    chunk = 10
    count = 0
    for key, account in accounts_list.items():
        count += 1
        task = asyncio.create_task(parse_fbo_return_in_json(
            account['client_id_api'],
            account['api_key'],
            100
        ))
        tasks.append(task)
        if len(tasks) == chunk or count == len(accounts_list):
            await asyncio.gather(*tasks)
            tasks = []


if __name__ == '__main__':
    # ozon = MarketParser()
    # account_ozon = account_data(1)
    # asyncio.get_event_loop().run_until_complete(
    #     main_fbo(account_ozon)
    # )
    pass
