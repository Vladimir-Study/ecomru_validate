import asyncio
import os
import asyncpg
import aiohttp
from dotenv import load_dotenv
from time import time
load_dotenv()


async def parse_fbs_return(limit: int, account_dict: dict):
    start = time()
    async with aiohttp.ClientSession() as session:
        tasks = []
        chunk = 3
        count = 0
        for account in account_dict.values():
            count += 1
            tasks.append(asyncio.create_task(make_request_in_site(
                session,
                account['client_id'],
                account['api_key'],
                limit
            )))
            if len(tasks) == chunk or count == len(account_dict):
                await_res = await asyncio.gather(*tasks)
                for _ in await_res:
                    print(len(_['result']['returns']))
                tasks = []
                print(count)
        print('{:.2f}'.format(time()-start))


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


async def make_request_in_db(conn):
    QUERY = '''SELECT return_id FROM return_table'''
    await conn.fetch(QUERY)


async def fbs_send_to_return_table():
    async with asyncpg.create_pool(
            host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
            port='6432',
            database='market_db',
            user=os.environ['DB_LOGIN'],
            password=os.environ['DB_PASSWORD'],
            ssl='verify-full'
    ) as pool:
        # async with pool.acquire() as conn:
        tasks = []
        chunk = 10
        count = 0
        start = time()
        for x in range(44):
            count += 1
            tasks.append(asyncio.create_task(make_request_in_db(pool)))
            if len(tasks) == chunk or count == 44:
                await asyncio.gather(*tasks)
                tasks = []
                print(count)
        print('{:.2f}'.format(time()-start))


async def async_work_with_db(async_fanc):


async def main():
    # await fbs_send_to_return_table()
    account = {1: {'client_id': '155597', 'api_key': 'b5e85d8a-e803-433c-ac2f-23bc2c8f8ae6'},
               2: {'client_id': '43083', 'api_key': 'f7c1af71-cd07-4b9a-9abe-fdfdf940db4f'},
               3: {'client_id': '101143', 'api_key': '9c65a425-9b98-42f5-ab0b-c97ee93bb7e9'}
               }
    await parse_fbs_return(10, account)



asyncio.get_event_loop().run_until_complete(main())