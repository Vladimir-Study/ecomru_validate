from help_func import params_date, convert_to_date, account_data, connections
from pprint import pprint
import asyncio 
import aiohttp
import json
import asyncpg
import os
from dotenv import load_dotenv
load_dotenv()


async def order_params(order, api_id, pool):
    try:
        await pool.execute(
        '''INSERT INTO orders_table VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, 
            $22, $23, $24, $25, $26, $27, $28, $29)''', str(order['order_id']), 
            order['order_number'], order['posting_number'],order['status'], 
            str(order['cancel_reason_id']), convert_to_date(order['created_at']), 
            convert_to_date(order['in_process_at']), 
            order['analytics_data']['region'], 
            order['analytics_data']['city'], 
            order['analytics_data']['delivery_type'], 
            str(order['analytics_data']['is_premium']), 
            order['analytics_data']['payment_type_group_name'], 
            str(order['analytics_data']['warehouse_id']), 
            order['analytics_data']['warehouse_name'], 
            str(order['analytics_data']['is_legal']), 
            order['financial_data']['posting_services']['marketplace_service_item_fulfillment'], 
            order['financial_data']['posting_services']['marketplace_service_item_pickup'], 
            order['financial_data']['posting_services']['marketplace_service_item_dropoff_pvz'], 
            order['financial_data']['posting_services']['marketplace_service_item_dropoff_sc'], 
            order['financial_data']['posting_services']['marketplace_service_item_dropoff_ff'], 
            order['financial_data']['posting_services']['marketplace_service_item_direct_flow_trans'], 
            order['financial_data']['posting_services']['marketplace_service_item_return_flow_trans'], 
            order['financial_data']['posting_services']['marketplace_service_item_deliv_to_customer'], 
            order['financial_data']['posting_services']['marketplace_service_item_return_not_deliv_to_customer'], 
            order['financial_data']['posting_services']['marketplace_service_item_return_part_goods_customer'], 
            order['financial_data']['posting_services']['marketplace_service_item_return_after_deliv_to_customer'], 
            str(order['additional_data']), api_id, 1
        )
    except Exception as e:
        print(f'Error ozon orders {e}')


async def goods_in_order_params(order, pool):
    try:
        for n in range(len(order['products'])):
            actions = [line for line in order['financial_data']['products'][n]['actions']]
            await pool.execute(
                '''INSERT INTO goods_in_orders_table (order_id, sku, quantity, 
                offer_id, price, commission_amount, commission_percent, 
                payout, product_id, old_price, unit_price, 
                total_discount_value, total_discount_percent, actions, 
                picking, picking_quantity, client_price, 
                marketplace_service_item_fulfillment, 
                marketplace_service_item_pickup, 
                marketplace_service_item_dropoff_pvz, 
                marketplace_service_item_dropoff_sc, 
                marketplace_service_item_dropoff_ff, 
                marketplace_service_item_direct_flow_trans, 
                marketplace_service_item_return_flow_trans, 
                marketplace_service_item_deliv_to_customer, 
                marketplace_service_item_return_not_deliv_to_customer, 
                marketplace_service_item_return_part_goods_customer, 
                marketplace_service_item_return_after_deliv_to_customer, 
                unit_name) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, 
                $23, $24, $25, $26, $27, $28, $29)''', str(order['order_id']), 
                str(order['products'][n]['sku']), order['products'][n]['quantity'], 
                order['products'][n]['offer_id'], float(order['products'][n]['price']), 
                order['financial_data']['products'][n]['commission_amount'], 
                order['financial_data']['products'][n]['commission_percent'], 
                order['financial_data']['products'][n]['payout'], 
                str(order['financial_data']['products'][n]['product_id']), 
                order['financial_data']['products'][n]['old_price'], 10, 
                order['financial_data']['products'][n]['total_discount_value'], 
                order['financial_data']['products'][n]['total_discount_percent'], 
                actions[0], order['financial_data']['products'][n]['picking'], 
                order['financial_data']['products'][n]['quantity'], 
                0 if order['financial_data']['products'][n]['client_price'] == '' 
                else float(order['financial_data']['products'][n]['client_price']), 
                order['financial_data']['products'][n]['item_services']['marketplace_service_item_fulfillment'], 
                order['financial_data']['products'][n]['item_services']['marketplace_service_item_pickup'], 
                order['financial_data']['products'][n]['item_services']['marketplace_service_item_dropoff_pvz'], 
                order['financial_data']['products'][n]['item_services']['marketplace_service_item_dropoff_sc'], 
                order['financial_data']['products'][n]['item_services']['marketplace_service_item_dropoff_ff'], 
                order['financial_data']['products'][n]['item_services']['marketplace_service_item_direct_flow_trans'], 
                order['financial_data']['products'][n]['item_services']['marketplace_service_item_return_flow_trans'], 
                order['financial_data']['products'][n]['item_services']['marketplace_service_item_deliv_to_customer'], 
                order['financial_data']['products'][n]['item_services']['marketplace_service_item_return_not_deliv_to_customer'], 
                order['financial_data']['products'][n]['item_services']['marketplace_service_item_return_part_goods_customer'], 
                order['financial_data']['products'][n]['item_services']['marketplace_service_item_return_after_deliv_to_customer'], 
                order['products'][n]['name']
            )
    except Exception as e:
        print(f'Error goods in order: {e}')


headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
    'Content-Type': 'application/json'
}


async def set_to_db() -> None:
    async with asyncpg.create_pool(
                    host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
                    port='6432',
                    database='market_db',
                    user=os.environ['DB_LOGIN'],
                    password=os.environ['DB_PASSWORD'],
                    ssl='require'
            ) as pool:   
        with open('orders_ozon_fbo.json', 'r', encoding='utf-8') as file:
            orders = json.load(file)
            tasks = []
            chunk = 1000
            for order in orders:
                tasks.append(await order_params(order, pool))
                tasks.append(await goods_in_order_params(order, pool))
            print(tasks)
            while len(tasks) != 0:
                print(f'Numbers of orders for recordins: {len(tasks)}')
                chunk_tasks = tasks[:chunk]
                await asyncio.gather(*chunk_tasks)
                tasks = tasks[chunk:]
            os.remote('')


if __name__ == '__main__':
    account = {
    '1': {'client_id_api': '155597',
    'api_key': 'b5e85d8a-e803-433c-ac2f-23bc2c8f8ae6'},
    '2': {'client_id_api': '43083',
    'api_key': 'f7c1af71-cd07-4b9a-9abe-fdfdf940db4f'},
    '3': {'client_id_api': '35291',
    'api_key': '1421edc2-106f-4ec1-9fce-6f3d6f3dd6d1'},
    }
    for key, val in account.items():
        asyncio.get_event_loop().run_until_complete(set_to_db())

