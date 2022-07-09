import requests
import psycopg2


# Класс принимает API tokenи проверяет валидность аккаунта,
# каждый метод возвращает True если аккаунт валиден
class ValidateAccount():

    def __init__(self, token: str):
        self.token = token

    def validate_ozon(self, client_id: str) -> bool:
        url = 'api-seller.ozon.ru/v1/actions'
        headers = {
            'Client-Id': client_id,
            'Api-Key': self.token,
        }

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == '200':
                return True
        except:
            return False

    def validate_wildberries(self) -> bool:
        url = 'https://suppliers-api.wildberries.ru/api/v2/warehouses'
        headers = {
            'key': self.token,
            'accept': 'application/json'
        }

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == '200':
                return True
        except:
            return False

    def validate_yandex(self, campaign_id: str) -> bool:
        url = f"https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/outlets.json"
        headers = {
            'Content-Type': 'application/json'
        }
        try:
            response = requests.get(url)
            if response.status_code == '200':
                return True
        except:
            return False


if __name__ == '__main__':
    conn = psycopg2.connect(
    host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
    port='6432',
    dbname='market_db',
    user='vpavlov',
    password='Qazwsx123Qaz',
    target_session_attrs='read-write',
    sslmode='verify-full'
    )
    q = conn.cursor()
    q.execute('SELECT * FROM account_list')
    res = q.fetchone()
    print(res)
    conn.close()