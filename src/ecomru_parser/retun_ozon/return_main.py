from return_fbo import parse_fbo_return, fbo_send_to_return_table
from pprint import pprint
from tqdm import tqdm
import os


def return_send_to_db() -> None:
    response = parse_fbo_return(os.environ['OZON_CLIENT_ID'], os.environ['OZON_API_KEY'])
    for order in tqdm(response['returns']):
        fbo_send_to_return_table(order)

    
if __name__ == '__main__':
    return_send_to_db()