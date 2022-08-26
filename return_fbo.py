from dotenv import load_dotenv
from pprint import pprint
from psycopg2 import Error
from help_func import write_csv, add_in_csv, feidnames_return_ozon, account_data
from mp_parser import MarketParser
import threading
import psycopg2
import requests
import os
import json
import asyncio
import asyncpg
import aiohttp
import aiofiles
from aiocsv import AsyncDictReader
from datetime import datetime
load_dotenv()


async def parse_fbo_return_in_csv(
        client_id: str, 
        api_key: str, 
        limit: int = 100,
        ) -> dict:
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
            async with session.post( url, 
                    json=data, 
                    headers=ozon_headers
                    ) as response:
                orders = await response.json()
                print(f"Number of ofders for parsing: {len(orders['returns'])}")
                await add_in_csv(feidnames_return_ozon, 'return_orders_ozon', orders['returns'])
        except Exception as E:
            print(f'Exception get return orders ozon FBO : {E}')


async def make_request_insert_in_db_fbo(pool, row):
    insert = await pool.execute('''INSERT INTO return_table (return_id, 
    posting_number, sku, status, return_reason_name, return_date, 
    waiting_for_seller_date_time, is_opened, moving_to_place_name) VALUES 
    ($1, $2, $3, $4, $5, $6, $7, $8, $9)''', row['id'], row['posting_number'],
    row['sku'], row['status_name'], row['return_reason_name'],
    None if row['accepted_from_customer_moment'] == '' else datetime.strptime(
    row['accepted_from_customer_moment'][:19], '%Y-%m-%dT%H:%M:%S'),
    None if row['returned_to_ozon_moment'] == '' else datetime.strptime(
    row['returned_to_ozon_moment'][:19], '%Y-%m-%dT%H:%M:%S'),
    row['is_opened'], row['dst_place_name']
    )
    return insert


async def make_request_update_in_db_fbo(pool, row):
    update = await pool.execute('''UPDATE return_table SET status = $1 
            WHERE return_id = $2''', row['status_name'], str(int(row['id'])))
    return update



async def fbo_send_to_return_table(file_name_csv) -> None:
    #try:
    async with asyncpg.create_pool(
        host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
        port='6432',
        database='market_db',
        user=os.environ['DB_LOGIN'],
        password=os.environ['DB_PASSWORD'],
        ssl='verify-full'
            ) as pool:
        async with aiofiles.open( f'{file_name_csv}.csv', 
                mode='r', 
                encoding='utf-8'
                ) as file:
            data_in_db = await pool.fetch(
                    '''SELECT return_id, 
                    status FROM return_table'''
                    )
            status_in_db = {}
            tasks = []
            chunk = 10
            for _ in data_in_db:
                status_in_db[_['return_id']] = _['status']
            async for row in AsyncDictReader(file, delimiter=';'):
                print(int(row['id']))
                '''
                id = str(int(row['id']))
                print(id)
                if id not in status_in_db.keys():
                    task = asyncio.create_task(make_request_insert_in_db_fbo(pool, row))
                    tasks.append(task)
                elif id in status_in_db.keys() and status_in_db[id] != row['status_name']:
                    task = asyncio.create_task(make_request_update_in_db_fbo(pool, row))
                    tasks.append(task)
                else:
                    continue
            while len(tasks) != 0:
                print(f'Numbers of orders for recordins: {len(tasks)}')
                chunk_tasks = tasks[:chunk]
                await asyncio.gather(*chunk_tasks)
                tasks = tasks[chunk:]
                '''
    #except (Exception) as E:
    #    print(f"Error in send to return table fbo: {E}")


async def get_chunk(accounts_list: dict) -> None:
    tasks = []
    chunk = 10
    count = 0
    for key, account in accounts_list.items():
        count += 1
        task = asyncio.create_task(parse_fbo_return_in_csv(
            account['client_id_api'],
            account['api_key'],
            100 
            ))        
        tasks.append(task)
        if len(tasks) == chunk or count == len(accounts_list):
            await asyncio.gather(*tasks)


async def main_fbo(accounts_list: dict, file_name_csv: str) -> None:
    await get_chunk(accounts_list)
    await fbo_send_to_return_table(file_name_csv)


if __name__ == '__main__':
    ozon = MarketParser()
    account_ozon = account_data(1)
    asyncio.get_event_loop().run_until_complete(
            main_fbo(account_ozon, 'return_orders_ozon')
            )
