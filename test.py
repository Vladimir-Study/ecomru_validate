import json
from pprint import pprint

res = [
    {'name': 'Anna',
     'age': 23},
    {'name': 'Petr',
     'age': 21},
    {'name': 'Leo',
     'age': 43}
]
with open('returns_ozon.json', 'r', encoding='utf-8') as file:
    test = json.load(file)
    for _ in res:
        test['returns'].append(_)
    with open('returns_ozon.json', 'w', encoding='utf-8') as outfile:
        json.dump(test, outfile, indent=4, ensure_ascii=False)

with open('returns_ozon.json', 'r', encoding='utf-8') as file:
    test = json.load(file)
    test['returns'].clear()
    with open('returns_ozon.json', 'w', encoding='utf-8') as outfile:
        json.dump(test, outfile, indent=4, ensure_ascii=False)

