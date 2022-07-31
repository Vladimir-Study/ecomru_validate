import requests
import os
from dotenv import load_dotenv


load_dotenv()


url = 'https://api-seller.ozon.ru/v2/posting/fbo/list'
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
    'Content-Type': 'application/json'
}
response = requests.post(url, headers=headers)

print(response.json())