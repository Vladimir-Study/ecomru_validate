from pprint import pprint
import requests

url = 'http://127.0.0.1:5000/account/status/255'
response = requests.get(url)

pprint(response.json())