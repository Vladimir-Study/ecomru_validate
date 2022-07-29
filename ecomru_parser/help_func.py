from datetime import datetime, timedelta
from pprint import pprint

from API_account.API import connections
from psycopg2 import Error


def params_date():
    date_now = datetime.now().isoformat(sep='T', timespec='milliseconds') + 'Z'
    date_to = datetime.now()
    delta = timedelta(weeks=4)
    date_to = date_to - delta
    date_to = date_to.isoformat(sep='T', timespec='milliseconds') + 'Z'
    date_dict = {'date_now': date_now, 'date_to': date_to}
    return date_dict


def convert_to_date(str_date: str):
    date = str_date[0:19]
    date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
    return date


def account_data(mp_id: int): # 1- ozon, 2-yandex , 3- WB
    try:
        conn = connections()
        _return_dict = {}
        with conn:
            with conn.cursor() as select:
                text_select = f"SELECT * FROM account_list WHERE mp_id = {mp_id}"
                select.execute(text_select)
                result_select = select.fetchall()
    except (Exception, Error) as E:
        print(f'Error {E}')
    if mp_id == 1:
        for line in result_select:
            if line[9] == 'Active':
                _return_dict[line[0]] = {'client_id_api': line[5], 'api_key': line[6]}
        return _return_dict
    elif mp_id == 2:
        for line in result_select:
            if line[9] == 'Active':
                _return_dict[line[0]] = {'token': line[5], 'client_id': line[6], 'campaign_id': line[7]}
        return _return_dict
    elif mp_id == 3:
        for line in result_select:
            if line[9] == 'Active' and line[12] == 'Active':
                _return_dict[line[0]] = {'api_key': line[5], 'key': line[6]}
            elif line[9] != 'Active' and line[12] == 'Active':
                _return_dict[line[0]] = {'key': line[6]}
            elif line[9] == 'Active' and line[12] != 'Active':
                _return_dict[line[0]] = {'api_key': line[5]}
        return _return_dict


if __name__ == '__main__':
    # res = account_data(2)
    # pprint(res)
    pass