import sys

sys.excepthook = sys.__excepthook__  # See https://groups.io/g/insync/topic/13778827?p=,,,20,0,0,0::recentpostdate%2Fsticky,,,20,2,0,13778827
from dateutil import parser as date_parser
import mysql.connector as sql
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from typing import Dict, Iterable, List, Tuple

import traceback
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def query(use_ssh, q, db_host, db_user, db_password, db_port, db, ssh_username, ssh_password, charset='utf8mb4'):

    if use_ssh:
        with SSHTunnelForwarder(
                ssh_address_or_host=(db_host, 22),
                ssh_password=ssh_password,
                ssh_username=ssh_username,
                remote_bind_address=('127.0.0.1', db_port)
        ) as server:
            conn = sql.connect(host='127.0.0.1',
                               port=server.local_bind_port,
                               user=db_user,
                               passwd=db_password,
                               db=db,
                               charset=charset)
            response = pd.read_sql_query(q, conn)
            conn.close()
            return response
    else:
        conn = sql.connect(host=db_host,
                           port=db_port,
                           user=db_user,
                           passwd=db_password,
                           db=db,
                           charset=charset)
        response = pd.read_sql_query(q, conn)
        conn.close()
        return response

def update(use_ssh, queries, db_host, db_user, db_password, db_port, db, ssh_username, ssh_password, charset='utf8mb4'):

    if use_ssh:
        with SSHTunnelForwarder(
                ssh_address_or_host=(db_host, 22),
                ssh_password=ssh_password,
                ssh_username=ssh_username,
                remote_bind_address=('127.0.0.1', db_port)
        ) as server:
            conn = sql.connect(host='127.0.0.1',
                               port=server.local_bind_port,
                               user=db_user,
                               passwd=db_password,
                               db=db,
                               charset=charset)
            cursor = conn.cursor()

            for q in queries:
                cursor.execute(q)

            conn.commit()
            conn.close()

    else:
        conn = sql.connect(host=db_host,
                           port=db_port,
                           user=db_user,
                           passwd=db_password,
                           db=db,
                           charset=charset)
        cursor = conn.cursor()

        for q in queries:
            cursor.execute(q)

        conn.commit()
        conn.close()


def reconnect_db(ssh, db_host, database_name, db_user, db_password, db_port, ssh_username, ssh_password, charset='utf8mb4'):
    ssh_server = None

    if ssh:
        ssh_server = SSHTunnelForwarder(
            ssh_address_or_host=(db_host, 22),
            ssh_password=ssh_password,
            ssh_username=ssh_username,
            remote_bind_address=('127.0.0.1', db_port))

        ssh_server.start()
        local_bind_port = ssh_server.local_bind_port
        engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@127.0.0.1:{local_bind_port}/{database_name}?charset={charset}')

    else:
        engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{database_name}?charset={charset}')

    return engine, ssh_server


def map_msgs_to_db_objects(classes, msgs: Iterable[Dict[str, object]]) -> Iterable[object]:

    # TODO: Generalise to other posts types
    def map_stocktwits_sentiment(msg: Dict[str, object]) -> Dict[str, object]:
        user_name = msg['data']['user']['username']
        id = msg['data']['id']
        created_at_epoch_ms = date_parser.parse(msg['data']['created_at']).timestamp()*1000

        return {
            'post_type': msg['msgType'].replace('-sentiment-response', ''),
            'post_id': id,
            'body': msg['data']['text'],
            'impact': 1,
            'link': f'https://stocktwits.com/{user_name}/message/{id}',
            'user_name': user_name,
            'created_at_epoch_ms': created_at_epoch_ms,
            'sentiment_ml_model': msg['predictions']['values']['bull_sentiment'],
            'sentiment_vader_normalized': msg['predictions']['values']['vader_sentiment'],
            'sentiment_mixed': msg['predictions']['values']['bull_vader_sentiment'],
            'client_received_epoch_ms': msg['clientReceivedTsMs'],
        }

    def map_twitter_sentiment(msg: Dict[str, object]) -> Dict[str, object]:
        user_name = msg['data']['user']['screen_name']
        id = msg['data']['id']
        created_at_epoch_ms = date_parser.parse(msg['data']['created_at']).timestamp()*1000

        return {
            'post_type': msg['msgType'].replace('-sentiment-response', ''),
            'post_id': id,
            'body': msg['data']['text'],
            'impact': 1,
            'link': f'https://twitter.com/{user_name}/status/{id}',
            'user_name': user_name,
            'created_at_epoch_ms': created_at_epoch_ms,
            'sentiment_ml_model': msg['predictions']['values']['bull_sentiment'],
            'sentiment_vader_normalized': msg['predictions']['values']['vader_sentiment'],
            'sentiment_mixed': msg['predictions']['values']['bull_vader_sentiment'],
            'client_received_epoch_ms': msg['clientReceivedTsMs'],
        }

    def filter_msgs(msgs):
        return [
            msg for msg in msgs if msg['msgType'] in
            ['stocktwit-sentiment-response', 'twitter-user-sentiment-response', 'twitter-topic-sentiment-response']
        ]

    mapped_objects = []

    msg_type_classes = {
        'stocktwit-sentiment-response': classes.analysis_posts_sentiment,
        'twitter-user-sentiment-response': classes.analysis_posts_sentiment,
        'twitter-topic-sentiment-response': classes.analysis_posts_sentiment,
    }


    mappers = {
        'stocktwit-sentiment-response': map_stocktwits_sentiment,
        'twitter-user-sentiment-response': map_twitter_sentiment,
        'twitter-topic-sentiment-response': map_twitter_sentiment,
    }


    # TODO: Use more functional approach
    for msg in filter_msgs(msgs):
        # Map from dictionary to class associated to table (ORM)
        mapper = mappers[msg['msgType']]# map_stocktwits_sentiment(msg)
        mapped_obj = mapper(msg)
        logger.info(f'Mapped object into {mapped_obj} message')

        if mapped_obj:
            obj_class = msg_type_classes[msg['msgType']]
            mapped_objects.append(obj_class(**mapped_obj))

    return mapped_objects


def operate_msgs_into_db(base, session, msgs, operate):

    try:
        mapped_objects = map_msgs_to_db_objects(base.classes, msgs)
        logger.info(f'{len(mapped_objects)} rows ready for commit into db.')
        inserted_instances = 0

        for instance in mapped_objects:
            try:
                inserted_instances += 1
                # session.add(instance)
                # session.merge(instance)
                operate(instance)
            except:
                session.rollback()
                logger.error(f'An error occurred: {traceback.format_exc()} for instance {instance}')

        session.commit()
        logger.info(f'Committed to db: {inserted_instances} rows.')

    except Exception:
        session.rollback()
        logger.error(f'An error occurred: {traceback.format_exc()}')
