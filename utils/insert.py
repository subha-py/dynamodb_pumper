import random
import sys
sys.path.append('/root/mongodb_pumper/utils')
sys.path.append('/root/mongodb_pumper')
from string import ascii_letters
import datetime
import concurrent.futures
from utils.connect import connect
from utils.table import get_table
from utils.memory import get_number_of_rows_from_size

def create_random_doc():
    toggle = random.choice([True, False])
    task_number = random.randint(1, sys.maxsize)
    random_string = ''.join(random.choices(ascii_letters, k=10))
    random_texts = ''.join(random.choices(ascii_letters, k=200))
    random_bytes = ''.join(random.choices(ascii_letters, k=400)).encode(encoding='ascii')
    random_string_list = [''.join(random.choices(ascii_letters, k=400)), ''.join(random.choices(ascii_letters, k=400)), ''.join(random.choices(ascii_letters, k=400))]
    random_num_list = [random.randint(1, sys.maxsize),random.randint(1, sys.maxsize),random.randint(1, sys.maxsize)]
    binary_list = [''.join(random.choices(ascii_letters, k=400)), ''.join(random.choices(ascii_letters, k=400)), ''.join(random.choices(ascii_letters, k=400))]
    random_map = {"Name": ''.join(random.choices(ascii_letters, k=10)), "Age": random.randint(1, sys.maxsize)}
    random_list = [''.join(random.choices(ascii_letters, k=200)) , ''.join(random.choices(ascii_letters, k=200)),random.randint(1, sys.maxsize)]
    item = {
        "username": random_string,
        "last_name": random_texts,
        "num_attr": task_number,
        "byte_attr": random_bytes,
        "string_list_attr":random_string_list,
        "num_list_attr": random_num_list,
        "binary_list_attr": binary_list,
        "map_attr": random_map,
        "list_attr": random_list,
        "toggle":toggle
    }
    return item


def create_random_docs(batch_size):
    docs = []
    for i in range(batch_size):
        docs.append(create_random_doc())
    return docs

def process_batch(table_name='auto-table-1'):
    table = get_table(table_name)
    with table.batch_writer() as batch:
        for i in range(50):
            batch.put_item(Item=create_random_doc())

if __name__ == '__main__':
    process_batch()

