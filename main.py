import requests
import psycopg2
import re
import datetime


# Класс принимает API tokenи проверяет валидность аккаунта,
# каждый метод возвращает True если аккаунт валиден
# mp_id: 1- Ozon, 2-
class ValidateAccount():

    # Ready
    def validate_ozon(self, client_id: str, api_key: str) -> bool:
        url = 'https://api-seller.ozon.ru/v1/warehouse/list'
        headers = {
            'Client-Id': client_id,
            'Api-Key': api_key,
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'Content-Type': 'application/json',
        }
        try:
            response = requests.post(url, headers=headers)
            if response.status_code == 200:
                return True
            return False
        except:
            return False

    def access_token(self, client_secret: str, client_id: str):
        url = 'https://performance.ozon.ru/api/client/token'
        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials"
        }
        response = requests.post(url, data=data)
        res = response.content.decode()
        pattern = r'["]{1}'
        access_token = re.split(pattern, res)
        return access_token[3]

    # Ready
    def validate_ozon_performance(self, client_secret: str, client_id: str) -> bool:
        access_token = self.access_token(client_secret, client_id)
        access_token = f'Bearer {access_token}'
        url = "https://performance.ozon.ru:443/api/client/campaign"
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'Authorization': access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return True
            return False
        except:
            return False

    # Ready
    def validate_wildberries(self, token: str) -> bool:
        url = 'https://suppliers-api.wildberries.ru/api/v2/warehouses'
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'Authorization': token,
            'accept': 'application/json'
        }

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return True
            return False
        except:
            return False

    # Ready
    def validate_wbstatistic(self, token: str) -> bool:
        url = f'https://suppliers-stats.wildberries.ru/api/v1/supplier/incomes'
        date = datetime.date.today()
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        }
        params = {
            'dateFrom': date,
            'key': token,
        }
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                return True
            return False
        except:
            return False

    # Ready
    def validate_yandex(self, token: str, client_id: str) -> bool:
        url = 'https://api.partner.market.yandex.ru/v2/campaigns.json'
        headers = {
            'Content-Type': 'application/json',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
            'Authorization': f'OAuth oauth_token={token}, oauth_client_id={client_id}',
        }
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return True
            return False
        except:
            return False


def read_account(file_path: str):
    with open(file_path, 'r', encoding='utf-8') as file:
        login = file.readline().strip()
        password = file.readline().strip()
        account = [login, password]
        return account

def mp_index(column_name: list):
    for name in column_name:
        for key, val in name.items():
            if key == 'mp_id':
                return val

def mp_request_index(mp_id, column_name):
    list_index = []
    if mp_id == 1:
        for name in column_name:
            for key, val in name.items():
                if key in ['client_secret_performance', 'client_id_performance']:
                    list_index.append(val)


if __name__ == '__main__':
    account = read_account('login.txt')
    conn = psycopg2.connect(
        host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
        port='6432',
        dbname='market_db',
        user=account[0],
        password=account[1],
        target_session_attrs='read-write',
        sslmode='verify-full'
    )
    select = conn.cursor()
    select.execute("select column_name,data_type from information_schema.columns "
                   "where table_name = 'account_list';")
    colum_name = select.fetchall()
    name_colum = []
    for index in range(len(colum_name)):
        name_index = {colum_name[index][0]: index}
        name_colum.append(name_index)
    # select.execute("SELECT * FROM account_list")
    # lines_table = select.fetchall()
    # for line in lines_table:
    #     print(line)
    print(mp_index(name_colum))
    conn.close()
