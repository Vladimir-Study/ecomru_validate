from pprint import pprint

import requests
import os
from dotenv import load_dotenv

load_dotenv()


class MarketParser():
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'Content-Type': 'application/json'
    }

    def parse_ozon_fbo(self, client_id, api_key):
        url = 'https://api-seller.ozon.ru/v2/posting/fbo/list'
        ozon_headers = {
            'Client-Id': client_id,
            'Api-Key': api_key,
        }
        ozon_headers = {**self.headers, **ozon_headers}
        data = {
            "dir": "asc",
            "filter": {
                "since": "2021-09-01T00:00:00.000Z",
                "status": "",
                "to": "2021-11-17T10:44:12.828Z"
            },
            "limit": 5,
            "offset": 0,
            "translit": True,
            "with": {
                "analytics_data": True,
                "financial_data": True
            }
        }
        response = requests.post(url, json=data, headers=ozon_headers)
        return response.json()

    def parse_ozon_fbs(self, client_id, api_key):
        url = 'https://api-seller.ozon.ru/v3/posting/fbs/list'
        ozon_headers = {
            'Client-Id': client_id,
            'Api-Key': api_key,
        }
        ozon_headers = {**self.headers, **ozon_headers}
        data = {
            "dir": "ASC",
            "filter": {
                "since": "2021-11-01T00:00:00.000Z",
                "status": "awaiting_packaging",
                "to": "2021-12-01T23:59:59.000Z"
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

    def parse_wb_fbs(self, api_key: str, date_start: str, take: int, skip: int = 0):
        url = 'https://suppliers-api.wildberries.ru/api/v2/orders'
        wb_headers = {
            'accept': 'application / json',
            'Authorization': api_key,
        }
        params = {
            'date_start': date_start,
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

    def parse_ya(self, campaign_id: str, date_from: str, token: str, client_id: str):
        url = f'https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/stats/orders.json'
        ya_headers = {
            'Authorization': f'OAuth oauth_token={token}, oauth_client_id={client_id}',
        }
        data = {
            'dateFrom': date_from,
        }
        ya_headers = {**self.headers, **ya_headers}
        response = requests.post(url, headers=ya_headers, json=data)
        return response.json()

wb = MarketParser()
pprint(wb.parse_ya(os.environ['CAMPAIGN_ID'], '2020-11-01', os.environ['YA_TOKEN'], os.environ['YA_CLIENT_ID']))