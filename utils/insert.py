import random
import sys
sys.path.append('/root/mongodb_pumper/utils')
sys.path.append('/root/mongodb_pumper')
from string import ascii_letters
import datetime
import concurrent.futures
from utils.connect import connect
from utils.memory import get_number_of_rows_from_size

def create_random_doc():
    toggle = random.choice([True, False])
    task_number = str(random.randint(1, sys.maxsize))
    random_string = ''.join(random.choices(ascii_letters, k=10))
    random_texts = ''.join(random.choices(ascii_letters, k=200))
    random_bytes = ''.join(random.choices(ascii_letters, k=400))
    random_string_list = [''.join(random.choices(ascii_letters, k=400)), ''.join(random.choices(ascii_letters, k=400)), ''.join(random.choices(ascii_letters, k=400))]
    random_num_list = [str(random.randint(1, sys.maxsize)),str(random.randint(1, sys.maxsize)),str(random.randint(1, sys.maxsize))]
    binary_list = [''.join(random.choices(ascii_letters, k=400)), ''.join(random.choices(ascii_letters, k=400)), ''.join(random.choices(ascii_letters, k=400))]
    random_map = {"Name": {"S": ''.join(random.choices(ascii_letters, k=10))}, "Age": {"N": str(random.randint(1, sys.maxsize))}}
    random_list = [ {"S": ''.join(random.choices(ascii_letters, k=10))} , {"S": ''.join(random.choices(ascii_letters, k=200))}, {"N":  str(random.randint(1, sys.maxsize))}]
    item = {
        "username": {'S':random_string},
        "last_name": {'S':random_texts},
        "num_attr": {'N':task_number},
        "byte_attr": {'B':random_bytes},
        "string_list_attr": {'SS':random_string_list},
        "num_list_attr": {'NS': random_num_list},
        "binary_list_attr": {'BS': binary_list},
        "map_attr": {'M': random_map},
        "list_attr": {"L": random_list},
        "toggle": {'BOOL':toggle}
    }
    return item


def create_random_docs(batch_size):
    docs = []
    for i in range(batch_size):
        docs.append(create_random_doc())
    return docs

def process_batch(table_name='auto-table-1'):
    client = connect()
    item = create_random_doc()
    client.put_item(TableName=table_name, Item=item,)

if __name__ == '__main__':
    process_batch()

