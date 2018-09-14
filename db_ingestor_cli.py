import argparse
import gc
from inference import message_utils
from inference import db_utils
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
import sys
sys.excepthook = sys.__excepthook__ # See https://groups.io/g/insync/topic/13778827?p=,,,20,0,0,0::recentpostdate%2Fsticky,,,20,2,0,13778827

import traceback
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_record_for_prediction(record):
    record['msgType'] = record.get('msgType', '') + '-sentiment-request'
    record['data']['text'] = record['data']['body']
    record['data'].pop('body')
    return record


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-bs', '--batch_size', help='Number of records per read, between commits.', type=int, default=5)
    parser.add_argument('-db', '--database_name', help='Database where to store the data', type=str, default='automlpredictor_db_dashboard')
    parser.add_argument(
        '-idf', '--input_data_file_path', help='Path for the data file. If not specified, we\'ll read the data from stdin',
        type=str, required=False
    )
    parser.add_argument('-s', '--sleep_ms', help='Sleep in millisecs', type=int, default=5000)
    parser.add_argument('-ssh', '--ssh', help='Use ssh', action='store_true', required=False)

    args = parser.parse_args()
    batch_size = int(args.batch_size)
    database_name = str(args.database_name)
    sleep_ms = int(args.sleep_ms)
    input_data_file_path = args.input_data_file_path
    ssh = args.ssh  # True
    # db = 'automlpredictor_db_dashboard'
    db_host = os.getenv('AUTOMLPREDICTOR_DB_SERVER_IP', '127.0.0.1')
    db_user = os.getenv('AUTOMLPREDICTOR_DB_SQL_USER', 'root')
    db_password = os.getenv('AUTOMLPREDICTOR_DB_SQL_PASSWORD')
    db_port = 3306
    ssh_username = os.getenv('AUTOMLPREDICTOR_DB_SSH_USER')
    ssh_password = os.getenv('AUTOMLPREDICTOR_DB_SSH_PASSWORD')

    if input_data_file_path:
        input_handle = open(input_data_file_path, 'r')
    else:
        input_handle = sys.stdin

    Base = automap_base()
    engine, ssh_server = db_utils.reconnect_db(ssh, db_host, database_name, db_user, db_password, db_port, ssh_username, ssh_password, 'utf8mb4')
    Base.prepare(engine, reflect=True)

    for input_msgs in message_utils.read_json_input(batch_size, input_handle, sleep_ms):
        # try:
        logger.info(f'Ingesting {len(input_msgs)} messages')
        logger.info(f'Ingesting {input_msgs} messages')

        try:
            session = Session(engine)
            db_utils.operate_msgs_into_db(Base, session, input_msgs, session.merge)
            session.close()
        except:
            logger.error(f'An error occurred while managing session {session}: {traceback.format_exc()}')

        del input_msgs
        del session
        gc.collect()

    if ssh:
        ssh_server.stop()

    if input_handle is not sys.stdin:
        input_handle.close()





