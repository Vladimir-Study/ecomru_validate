import requests


class MarketParser():
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'Content-Type': 'application/json'
    }

    def parse_ozon_fbo(self, client_id, api_key, date_now, date_to):
        url = 'https://api-seller.ozon.ru/v2/posting/fbo/list'
        ozon_headers = {
            'Client-Id': client_id,
            'Api-Key': api_key,
        }
        ozon_headers = {**self.headers, **ozon_headers}
        data = {
            "dir": "asc",
            "filter": {
                "since": date_to,
                "status": "",
                "to": date_now
            },
            "limit": 100,
            "offset": 0,
            "translit": True,
            "with": {
                "analytics_data": True,
                "financial_data": True
            }
        }
        response = requests.post(url, json=data, headers=ozon_headers)
        return response.json()

    def parse_ozon_fbs(self, client_id, api_key, date_now, date_to):
        url = 'https://api-seller.ozon.ru/v3/posting/fbs/list'
        ozon_headers = {
            'Client-Id': client_id,
            'Api-Key': api_key,
        }
        ozon_headers = {**self.headers, **ozon_headers}
        data = {
            "dir": "ASC",
            "filter": {
                "since": date_to,
                "status": "awaiting_packaging",
                "to": date_now
            },
            "limit": 100,
            "offset": 0,
            "translit": True,
            "with": {
                "analytics_data": True,
                "financial_data": True
            }
        }
        response = requests.post(url, json=data, headers=ozon_headers)
        return response.json()

    def parse_wb_fbs(self, api_key: str, date_start: str, date_end: str, take: int = 100, skip: int = 0):
        url = 'https://suppliers-api.wildberries.ru/api/v2/orders'
        wb_headers = {
            'accept': 'application / json',
            'Authorization': api_key,
        }
        params = {
            'date_start': date_start,
            'date_end': date_end,
            'take': take,
            'skip': skip
        }
        wb_headers = {**self.headers, **wb_headers}
        response = requests.get(url, headers=wb_headers, params=params)
        return response.json()

    def parse_wb_fbo(self, key: str, date_from: str, flag: int = 0):
        url = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/orders'
        params = {
            'key': key,
            'dateFrom': date_from,
            'flag': flag
        }
        response = requests.get(url, headers=self.headers, params=params)
        return response.json()

    def parse_ya(self, campaign_id: str, token: str, client_id: str):
        url = f'https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/orders.json'
        ya_headers = {
            'Authorization': f'OAuth oauth_token={token}, oauth_client_id={client_id}',
        }
        ya_headers = {**self.headers, **ya_headers}
        response = requests.get(url, headers=ya_headers)
        return response.json()


if __name__ == '__name__':
    pass
