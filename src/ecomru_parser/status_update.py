from psycopg2 import Error
from pprint import pprint
from help_func import connections


def get_status_on_db(order_id: str) -> str:
    try:
        conn = connections()
        with conn:
            with conn.cursor() as select:
                select.execute(f"SELECT * FROM orders_table WHERE order_id = '{order_id}';")
                res = select.fetchone()
                if res is not None:
                    return res[3]
                return res
    except (Exception, Error) as E:
        print(f'Error status get: {E}')


if __name__ == '__main__':
    #print(get_status_on_db('749937544'))
    pass 
