from datetime import datetime
from pprint import pprint
import asyncio
import os
import asyncpg
import aiohttp
from dotenv import load_dotenv
from time import time
load_dotenv()


async def parse_fbs_return(limit: int, account_dict: dict):
    return_orders = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        chunk = 3
        count = 0
        for account in account_dict.values():
            count += 1
            tasks.append(asyncio.create_task(make_request_in_site(
                session,
                account['client_id_api'],
                account['api_key'],
                limit
            )))
            if len(tasks) == chunk or count == len(account_dict):
                await_res = await asyncio.gather(*tasks)
                for _ in await_res:
                    for i in _['result']['returns']:
                        return_orders.append(i)
                tasks = []
    return return_orders


async def make_request_in_site(session, client_id: str, api_key: str, limit: int):
    URL = 'https://api-seller.ozon.ru/v2/returns/company/fbs'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        'Client-Id': client_id,
        'Api-Key': api_key,
    }
    data = {
        "limit": limit,
        "offset": 0,
    }
    async with session.post(URL, json=data, headers=headers) as response:
        return await response.json()


async def make_request_insert_in_db(pool, order):
    await pool.execute('''INSERT INTO return_table VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
                                $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)''', str(order['id']),
                                str(order['clearing_id']), order['posting_number'], str(order['product_id']),
                                str(order['sku']), order['status'], order['returns_keeping_cost'],
                                order['return_reason_name'], 
                                None if order['return_date'] == None else datetime.strptime(
                                    order['return_date'][:19], '%Y-%m-%dT%H:%M:%S'),
                                order['quantity'],order['product_name'], order['price'], 
                                None if order['waiting_for_seller_date_time'] == None else datetime.strptime(
                                    order['waiting_for_seller_date_time'][:19], '%Y-%m-%dT%H:%M:%S'),
                                None if order['returned_to_seller_date_time'] == None else datetime.strptime(
                                    order['returned_to_seller_date_time'][:19], '%Y-%m-%dT%H:%M:%S'),
                                None if order['last_free_waiting_day'] == None else datetime.strptime(
                                    order['last_free_waiting_day'][:19], '%Y-%m-%dT%H:%M:%S'),
                                str(order['is_opened']), str(order['place_id']), order['commission_percent'],
                                order['commission'], order['price_without_commission'], str(order['is_moving']),
                                order['moving_to_place_name'], order['waiting_for_seller_days'],
                                str(order['picking_amount']), str(order['accepted_from_customer_moment']),
                                str(order['picking_tag']))


async def make_request_update_in_db(pool, order):
    await pool.execute('''UPDATE SET return_table status = $1''', order['status'])


async def get_satus_order(pool):
    records_status = {}
    async with pool.acquire() as conn:
        records = await conn.fetch('''SELECT return_id, status FROM return_table''')
        for record in records:
            records_status[record['return_id']] = record['status']
        return records_status    


async def fbs_send_to_return_table(accounts):
    try:
        start = time()
        async with asyncpg.create_pool(
                host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
                port='6432',
                database='market_db',
                user=os.environ['DB_LOGIN'],
                password=os.environ['DB_PASSWORD'],
                ssl='verify-full'
        ) as pool:
            return_orders = await parse_fbs_return(10, accounts)
            status_orders = await get_satus_order(pool)
            tasks = []
            for order in return_orders:
                if order['id'] in status_orders.keys() and order['status'] != status_orders[order['id']]:
                    tasks.append(asyncio.create_task(make_request_update_in_db(pool, order)))
                    print('tasks + 1')
                elif order['id'] in status_orders.keys() and order['status'] == status_orders[order['id']]:
                    print('continue')
                    continue
                else:
                    tasks.append(asyncio.create_task(make_request_insert_in_db(pool, order)))
                    print('tasks ++1')
            await asyncio.gather(*tasks)
            print(len(tasks))
            print('{:.2f}'.format(time()-start))
    except Exception as E:
        print(E)


async def main_fbs(accounts):            
    await fbs_send_to_return_table(accounts)


if __name__ == '__main__':
    pass

