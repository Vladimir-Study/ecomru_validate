from pprint import pprint

from flask import Flask
from flask_restful import Api, Resource
import psycopg2
from validate_account.validate_account import read_account


def connections(auth_path: str):
    account = read_account(auth_path)
    conn = psycopg2.connect(
        host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
        port='6432',
        dbname='market_db',
        user=account[0],
        password=account[1],
        target_session_attrs='read-write',
        sslmode='verify-full'
    )
    return conn


app = Flask(__name__)
api = Api()


class EcomruUsers(Resource):

    def get(self, id):
        try:
            conn = connections('../login.txt')
            with conn:
                with conn.cursor() as select:
                    select.execute(f"SELECT * FROM account_list WHERE id = {id};")
                    line_table = select.fetchone()
            if line_table is not None:
                return {
                    'id': id,
                    'status_1': line_table[9],
                    'status_2': line_table[12],
                }
            return {
                'id': 'Not found'
            }
        except Exception as e:
            return {
                'Exception': e,
            }

api.add_resource(EcomruUsers, '/account/status/<int:id>')
api.init_app(app)


if __name__ == '__main__':
    app.run(debug=True, port=5000, host='127.0.0.1')
