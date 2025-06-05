import random
import sys
sys.path.append('/root/dynamodb_pumper/utils')
sys.path.append('/root/dynamodb_pumper')
from string import ascii_letters

import concurrent.futures
from utils.table import get_table

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

def process_batch(batch_number,total_batches, table_name, batch_size=1000):
    print(f'{batch_number}//{total_batches}: starting')
    table = get_table(table_name)
    with table.batch_writer() as batch:
        for i in range(batch_size):
            batch.put_item(Item=create_random_doc())
    print(f'{batch_number}//{total_batches}: batch executed')
def pump_data(threads=64, table_name='auto-table-1'):
    future_to_batch = {}
    number_of_batches = 1000000
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        for batch_number in range(1,number_of_batches+1):
            arg = (batch_number,number_of_batches, table_name)
            future_to_batch[executor.submit(process_batch, *arg)] = batch_number
    result = []
    for future in concurrent.futures.as_completed(future_to_batch):
        batch_number = future_to_batch[future]
        try:
            res = future.result()
            if not res:
                result.append(batch_number)
        except Exception as exc:
            print("%r generated an exception: %s" % (batch_number, exc))
    return result

if __name__ == '__main__':
    pump_data()