from pprint import pprint
import requests
import json

url = 'http://127.0.0.1:5000/account/delete'
params = {
    'Content-Type': 'application/json'
}
response = requests.delete(url, data=json.dumps({"id": 223}), headers=params)

print(response.text)