import requests
import psycopg2
import re


# Класс принимает API tokenи проверяет валидность аккаунта,
# каждый метод возвращает True если аккаунт валиден
# mp_id: 1- Ozon, 2-
class ValidateAccount():

    #Ready
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

    #?
    def validate_wildberries(self, token: str) -> bool:
        url = 'https://suppliers-api.wildberries.ru/api/v2/warehouses'
        headers = {
            'key': token,
            'accept': 'application/json'
        }

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == '200':
                return True
            return False
        except:
            return False

    #?
    def validate_yandex(self, campaign_id: str) -> bool:
        url = f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/outlets.json"
        headers = {
            'Content-Type': 'application/json'
        }
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == '200':
                return True
        except:
            return False


def read_account(file_path: str):
    with open(file_path, 'r', encoding='utf-8') as file:
        login = file.readline().strip()
        password = file.readline().strip()
        account = [login, password]
        return account


if __name__ == '__main__':
    # account = read_account('login.txt')
    ozon = ValidateAccount()
    res = ozon.validate_ozon('43083', 'f7c1af71-cd07-4b9a-9abe-fdfdf940db4f')
    print(res)
    '''
    conn = psycopg2.connect(
        host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
        port='6432',
        dbname='market_db',
        user=account[0],
        password=account[1],
        target_session_attrs='read-write',
        sslmode='verify-full'
    )
    q = conn.cursor()
    q.execute('SELECT * FROM account_list WHERE id = 1;')
    res = q.fetchone()
    print(res)
    conn.close()
    '''