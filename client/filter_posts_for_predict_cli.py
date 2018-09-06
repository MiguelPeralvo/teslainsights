import argparse
from ..inference import message_utils
from ..inference import db_utils

import gc
import json
import sys
import os
import time
sys.excepthook = sys.__excepthook__  # See https://groups.io/g/insync/topic/13778827?p=,,,20,0,0,0::recentpostdate%2Fsticky,,,20,2,0,13778827

import traceback
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_record_for_prediction(record):
    record['msgType'] = record.get('msgType', '') + '-sentiment-request'
    record['data']['text'] = record['data']['body']
    record['data'].pop('body')
    return record


def filter_input(input_data_file_path, batch_size, sleep_ms, processed_posts):
    if input_data_file_path:
        input_handle = open(input_data_file_path, 'r')
    else:
        input_handle = sys.stdin

    for input_msgs in message_utils.read_json_input(batch_size, input_handle, sleep_ms):
        posts_to_inspect = {}
        # print(input_msgs)
        for record in input_msgs:
            if record['msgType'] == 'stocktwit':
                # TODO: Generalise to extract fields for other message types
                key = (record['msgType'], record['data']['id'])

                if key not in processed_posts:
                    processed_posts.add(key)
                    posts_to_inspect[key] = record

        yield posts_to_inspect


if __name__ == '__main__':
    # At some stage, we may need to use some LRU/or distributed data structure, but not yet.
    processed_posts = set({})
    ssh_username = os.getenv('AUTOMLPREDICTOR_DB_SSH_USER')
    ssh_password = os.getenv('AUTOMLPREDICTOR_DB_SSH_PASSWORD')

    db_host = os.getenv('AUTOMLPREDICTOR_DB_SERVER_IP', '127.0.0.1')
    db_user = 'root'
    db_password = os.getenv('AUTOMLPREDICTOR_DB_SQL_PASSWORD')
    db_port = 3306
    db = 'automlpredictor_db_dashboard'

    parser = argparse.ArgumentParser()
    parser.add_argument('-bs', '--batch_size', help='Number of records per read.', type=int, default=5)
    parser.add_argument('-db', '--database_name', help='Database where to store the data', type=str, default='automlpredictor_db_dashboard')
    parser.add_argument(
        '-idf', '--input_data_file_path', help='Path for the data file. If not specified, we\'ll read the data from stdin',
        type=str, required=False
    )
    parser.add_argument('-s', '--sleep_ms', help='Sleep in millisecs', type=int, default=1000)
    parser.add_argument('-ssh', '--ssh', help='Use ssh', action='store_true', required=False)

    args = parser.parse_args()
    input_data_file_path = args.input_data_file_path
    batch_size = int(args.batch_size)
    db = str(args.database_name)
    sleep_ms = int(args.sleep_ms)
    use_ssh = args.ssh  # True

    while True:

        for batch in filter_input(input_data_file_path, batch_size, sleep_ms, processed_posts):
            # Now we'll check in the db if we already processed them (several hundred per batch tops).
            ids = ', '.join(list(map(lambda x: f"('{x[0]}', {x[1]})", batch.keys())))
            old_ids_sql = f'SELECT post_type, post_id FROM analysis_posts_sentiment WHERE (post_type, post_id) IN ({ids})'

            try:
                df_old_ids = db_utils.query(use_ssh, old_ids_sql, db_host, db_user, db_password, db_port, db, ssh_username, ssh_password)

                for _, row in df_old_ids.iterrows():
                    batch.pop((row['post_type'], row['post_id']))
            except:
                logger.error(f'Exception associated to query {old_ids_sql}: {traceback.format_exc()}')

            for key, record in batch.items():
                print(json.dumps(transform_record_for_prediction(record)))


            del batch
            gc.collect()
            logger.info(f'Main loop: Going to sleep for {sleep_ms} milliseconds.')
            time.sleep(sleep_ms / 1000)




