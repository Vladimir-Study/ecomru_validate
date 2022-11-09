from help_func import params_date, convert_to_date, account_data, connections
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
        print(f'Error OZON orders: {e}')


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
        print(f'Error goods in order OZON: {e}')


if __name__ == '__main__':
    pass
