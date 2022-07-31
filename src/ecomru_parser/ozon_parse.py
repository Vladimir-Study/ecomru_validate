from help_func import params_date, convert_to_date, account_data, connections
from mp_parser import MarketParser
from psycopg2 import Error

date = params_date()
ozon = MarketParser()
account_ozon = account_data(1)
ozon_fbo = ozon.parse_ozon_fbo(account_ozon[107]['client_id_api'], account_ozon[107]['api_key'], date['date_now'],
                               "2021-09-01T00:00:00.000Z")

def order_params(response, api_id):
    try:
        conn = connections()
        with conn:
            with conn.cursor() as select:
                select.execute(f"SELECT order_id FROM orders_table;")
                orders = select.fetchall()
                list_order = []
                for order in orders:
                    list_order.append(order[0])
                for order in response['result']:
                    if order['order_id'] not in list_order:
                        select.execute(
                            f"INSERT INTO orders_table VALUES ('{order['order_id']}', "
                            f"'{order['order_number']}', '{order['posting_number']}', "
                            f"'{order['status']}', '{order['cancel_reason_id']}', "
                            f"'{convert_to_date(order['created_at'])}', "
                            f"'{convert_to_date(order['in_process_at'])}', "
                            f"'{order['analytics_data']['region']}', "
                            f"'{order['analytics_data']['city']}', "
                            f"'{order['analytics_data']['delivery_type']}', "
                            f"'{order['analytics_data']['is_premium']}', "
                            f"'{order['analytics_data']['payment_type_group_name']}', "
                            f"'{order['analytics_data']['warehouse_id']}', "
                            f"'{order['analytics_data']['warehouse_name']}', "
                            f"'{order['analytics_data']['is_legal']}', "
                            f"{order['financial_data']['posting_services']['marketplace_service_item_fulfillment']}, "
                            f"{order['financial_data']['posting_services']['marketplace_service_item_pickup']}, "
                            f"{order['financial_data']['posting_services']['marketplace_service_item_dropoff_pvz']}, "
                            f"{order['financial_data']['posting_services']['marketplace_service_item_dropoff_sc']}, "
                            f"{order['financial_data']['posting_services']['marketplace_service_item_dropoff_ff']}, "
                            f"{order['financial_data']['posting_services']['marketplace_service_item_direct_flow_trans']}, "
                            f"{order['financial_data']['posting_services']['marketplace_service_item_return_flow_trans']}, "
                            f"{order['financial_data']['posting_services']['marketplace_service_item_deliv_to_customer']}, "
                            f"{order['financial_data']['posting_services']['marketplace_service_item_return_not_deliv_to_customer']}, "
                            f"{order['financial_data']['posting_services']['marketplace_service_item_return_part_goods_customer']}, "
                            f"{order['financial_data']['posting_services']['marketplace_service_item_return_after_deliv_to_customer']}, "
                            f"'{order['additional_data']}', '{api_id}', 1);"
                        )
                        conn.commit()
    except (Exception, Error) as e:
        print(f'Error {e}')


def goods_in_order_params(response):
    try:
        conn = connections()
        with conn:
            with conn.cursor() as select:
                for order in response['result']:
                    actions = []
                    for n in range(len(order['products'])):
                        for line in order['financial_data']['products'][n]['actions']:
                            actions.append(line)
                        select.execute(
                            f"INSERT INTO goods_in_orders_table (order_id, sku, quantity, "
                            f"offer_id, price, commission_amount, commission_percent, "
                            f"payout, product_id, old_price, unit_price, total_discount_value, "
                            f"total_discount_percent, actions, picking, picking_quantity, "
                            f"client_price, marketplace_service_item_fulfillment, "
                            f"marketplace_service_item_pickup, marketplace_service_item_dropoff_pvz, "
                            f"marketplace_service_item_dropoff_sc, marketplace_service_item_dropoff_ff, "
                            f"marketplace_service_item_direct_flow_trans, marketplace_service_item_return_flow_trans, "
                            f"marketplace_service_item_deliv_to_customer, "
                            f"marketplace_service_item_return_not_deliv_to_customer, "
                            f"marketplace_service_item_return_part_goods_customer, "
                            f"marketplace_service_item_return_after_deliv_to_customer, "
                            f"unit_name) "
                            f"VALUES ('{order['order_id']}', "
                            f"'{order['products'][n]['sku']}', {order['products'][n]['quantity']}, "
                            f"'{order['products'][n]['offer_id']}', {order['products'][n]['price']}, "
                            f"{order['financial_data']['products'][n]['commission_amount']}, "
                            f"{order['financial_data']['products'][n]['commission_percent']}, "
                            f"{order['financial_data']['products'][n]['payout']}, "
                            f"'{order['financial_data']['products'][n]['product_id']}', "
                            f"{order['financial_data']['products'][n]['old_price']}, "
                            f"10, "
                            f"{order['financial_data']['products'][n]['total_discount_value']}, "
                            f"{order['financial_data']['products'][n]['total_discount_percent']}, "
                            f"'{actions[0]}', "
                            f"'{order['financial_data']['products'][n]['picking']}', "
                            f"{order['financial_data']['products'][n]['quantity']}, "
                            f"{0 if order['financial_data']['products'][n]['client_price'] == '' else float(order['financial_data']['products'][n]['client_price'])}, "
                            f"{order['financial_data']['products'][n]['item_services']['marketplace_service_item_fulfillment']}, "
                            f"{order['financial_data']['products'][n]['item_services']['marketplace_service_item_pickup']}, "
                            f"{order['financial_data']['products'][n]['item_services']['marketplace_service_item_dropoff_pvz']}, "
                            f"{order['financial_data']['products'][n]['item_services']['marketplace_service_item_dropoff_sc']}, "
                            f"{order['financial_data']['products'][n]['item_services']['marketplace_service_item_dropoff_ff']}, "
                            f"{order['financial_data']['products'][n]['item_services']['marketplace_service_item_direct_flow_trans']}, "
                            f"{order['financial_data']['products'][n]['item_services']['marketplace_service_item_return_flow_trans']}, "
                            f"{order['financial_data']['products'][n]['item_services']['marketplace_service_item_deliv_to_customer']}, "
                            f"{order['financial_data']['products'][n]['item_services']['marketplace_service_item_return_not_deliv_to_customer']}, "
                            f"{order['financial_data']['products'][n]['item_services']['marketplace_service_item_return_part_goods_customer']}, "
                            f"{order['financial_data']['products'][n]['item_services']['marketplace_service_item_return_after_deliv_to_customer']}, "
                            f"'{order['products'][n]['name']}')"
                        )
                        conn.commit()
    except (Exception, Error) as e:
        print(f'Error {e}')


if __name__ == '__main__':
    # order_params(ozon_fbs, account_ozon[107]['api_key'])
    for order in ozon_fbo['result']:
        print(order)
    # pass
