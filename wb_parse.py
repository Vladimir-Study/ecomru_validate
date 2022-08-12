from psycopg2 import Error
from help_func import convert_to_date, connections


def fbo_order_params(fbo_order: dict, api_key: str) -> None:
    try:
        fbo_conn = connections()
        with fbo_conn:
            with fbo_conn.cursor() as fbo_select:
                fbo_select.execute(f"INSERT INTO orders_table (order_id, created_at, "
                                   f"region, warehouse_name, api_id, mp_id) "
                                   f"VALUES ('{fbo_order['gNumber']}', '{convert_to_date(fbo_order['date'])}', "
                                   f"'{fbo_order['oblast']}', '{fbo_order['warehouseName']}', '{api_key}', 3)"
                                   )
                fbo_conn.commit()
                fbo_select.execute(f"INSERT INTO goods_in_orders_table (order_id, sku, offer_id, price) "
                                   f"VALUES ('{fbo_order['gNumber']}' , '{fbo_order['nmId']}', "
                                   f"'{fbo_order['supplierArticle']}', {fbo_order['totalPrice']})"
                                   )
                fbo_conn.commit()
    except (Exception, Error) as E:
        print(f'Error: {E}')


def fbs_order_params(fbs_order: dict, api_key: str) -> None:
    try:
        fbs_conn = connections()
        with fbs_conn:
            with fbs_conn.cursor() as fbs_select:
                fbs_select.execute(
                    f"INSERT INTO orders_table (order_id, created_at, city, delivery_type, "
                    f"warehouse_id, api_id, mp_id) "
                    f"VALUES ('{fbs_order['orderId']}', '{convert_to_date(fbs_order['dateCreated'])}', "
                    f"'{fbs_order['deliveryAddressDetails']['city']}', '{fbs_order['deliveryType']}', '{fbs_order['wbWhId']}', "
                    f"'{api_key}', 3)"
                )
                fbs_conn.commit()
                fbs_select.execute(
                    f"INSERT INTO goods_in_orders_table (order_id, sku) "
                    f"VALUES ('{fbs_order['orderId']}', {fbs_order['chrtId']})"
                )
                fbs_conn.commit()
    except (Exception, Error) as E:
        print(f'Error: {E}')


if __name__ == '__main__':
    pass