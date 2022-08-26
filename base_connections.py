from dotenv import load_dotenv
import asyncio
import asyncpg
import aiohttp
import os 
from return_fbo import test_con
load_dotenv()


async def connections():
    async with asyncpg.create_pool(
        host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
        port='6432',
        database='market_db',
        user=os.environ['DB_LOGIN'],
        password=os.environ['DB_PASSWORD'],
        ssl='verify-full'
            ) as pool:
        await test_con(pool)

asyncio.run(connections())        
