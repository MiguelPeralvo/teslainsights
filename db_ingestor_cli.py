import argparse
import gc
from inference import message_utils
from inference import db_utils
from lru import LRU
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
import sys
import time
from typing import Dict, Iterable, List, Tuple
sys.excepthook = sys.__excepthook__ # See https://groups.io/g/insync/topic/13778827?p=,,,20,0,0,0::recentpostdate%2Fsticky,,,20,2,0,13778827

import traceback
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_relevant_posts(use_ssh, db_host, db_user, db_password, db_port, database_name, ssh_username, ssh_password):
    relevant_posts_sql = f'''

    SELECT post_type, message_id, MAX(interaction_total) as interaction_total, MAX(likes_total) as likes_total FROM
    (
    SELECT 'stocktwit' as post_type, message_id, conversation_replies as interaction_total, likes_total FROM data_stocktwits_posts_rt
    WHERE message_id IN (SELECT post_id FROM analysis_posts_sentiment 
    WHERE created_at_epoch_ms >=(SELECT UNIX_TIMESTAMP(NOW())*1000-(12*3600*1000)) AND post_type = 'stocktwit')

    UNION

    SELECT 'twitter-user' as post_type, tweet_id as message_id, retweet_count as interaction_total, favorite_count as likes_total FROM data_twitter_users_rt
    WHERE tweet_id IN (SELECT post_id FROM analysis_posts_sentiment 
    WHERE created_at_epoch_ms >=(SELECT UNIX_TIMESTAMP(NOW())*1000-(12*3600*1000)) AND post_type = 'twitter-user')

    UNION

    SELECT 'twitter-topic' as post_type, tweet_id as message_id, retweet_count as interaction_total, favorite_count as likes_total FROM data_twitter_topics_rt
    WHERE tweet_id IN (SELECT post_id FROM analysis_posts_sentiment 
    WHERE created_at_epoch_ms >=(SELECT UNIX_TIMESTAMP(NOW())*1000-(12*3600*1000)) AND post_type = 'twitter-topic')
    ) impact 
    GROUP BY post_type, message_id;

  '''

    df_relevant_posts = db_utils.query(use_ssh, relevant_posts_sql, db_host, db_user, db_password, db_port, database_name, ssh_username, ssh_password)

    gc.collect()

    return df_relevant_posts


def update_impact_in_db(posts_to_update: List[Tuple[int, float]], use_ssh, db_host, db_user, db_password, db_port, database_name, ssh_username, ssh_password):
    sql_for_update = [
        f"UPDATE analysis_posts_sentiment SET impact={float(impact)} WHERE post_id={post_id} AND post_type='{post_type}';\n\n" for (post_id, impact, post_type)
        in posts_to_update
    ]

    logger.info(f'Running updates: {len(sql_for_update)}')
    db_utils.update(use_ssh, sql_for_update, db_host, db_user, db_password, db_port, database_name, ssh_username, ssh_password)
    logger.info(f'Completed updates: {len(sql_for_update)}')


def insert_current_global_sentiment_in_db(use_ssh, db_host, db_user, db_password, db_port, database_name, ssh_username, ssh_password):
    sql_for_insert_stocktwits = """
        INSERT INTO analysis_global_sentiment(sentiment_type, sentiment_seconds_back, created_at_epoch_ms, sentiment_absolute)
        (SELECT 'stocktwits', 12*3600, UNIX_TIMESTAMP(NOW())*1000, AVG(impact*(sentiment_mixed-0.5))
        FROM analysis_posts_sentiment 
        WHERE created_at_epoch_ms >=(SELECT UNIX_TIMESTAMP(NOW())*1000-(12*3600*1000)) AND post_type IN ('stocktwit'));       
    """

    sql_for_insert_twitter = """
        INSERT INTO analysis_global_sentiment(sentiment_type, sentiment_seconds_back, created_at_epoch_ms, sentiment_absolute)
        (SELECT 'twitter', 12*3600, UNIX_TIMESTAMP(NOW())*1000, AVG(impact*(sentiment_mixed-0.5))
        FROM analysis_posts_sentiment
        WHERE created_at_epoch_ms >=(SELECT UNIX_TIMESTAMP(NOW())*1000-(12*3600*1000)) AND post_type IN ('twitter-topic', 'twitter-user'));
    """

    sql_for_insert_social_teslamonitor = """
        INSERT INTO analysis_global_sentiment(sentiment_type, sentiment_seconds_back, created_at_epoch_ms, sentiment_absolute)
        (
            SELECT 'social_teslamonitor', sentiment_seconds_back, UNIX_TIMESTAMP(NOW())*1000, 
            50*AVG(0.5+sentiment_absolute)
            FROM analysis_global_sentiment
            WHERE created_at_epoch_ms >=(SELECT UNIX_TIMESTAMP(NOW())*1000-(sentiment_seconds_back*1000))
            AND sentiment_type IN ('twitter', 'stocktwits')
            GROUP BY sentiment_seconds_back
        );
    """

    # Stocktweets ranges between 0 and 100.
    # Stockfluence ranges between 0 and 200.

    sql_for_insert_social_external_ensemble = """
        INSERT INTO analysis_global_sentiment(sentiment_type, sentiment_seconds_back, created_at_epoch_ms, sentiment_absolute)
        (
            SELECT 'social_external_ensemble', 12*3600, UNIX_TIMESTAMP(NOW())*1000, 
            AVG(sentiment_absolute)
            FROM 
            (
            
                SELECT client_received_ts_ms, sentiment_percent as sentiment_absolute
                FROM data_stocktwits_sentiment_rt
                WHERE client_received_ts_ms >=(SELECT UNIX_TIMESTAMP(NOW())*1000-(12*3600*1000))
                
                UNION
                
                SELECT client_received_ts_ms, 0.5*sentiment_score as sentiment_absolute
                FROM data_stockfluence_rt_sentiment
                WHERE client_received_ts_ms >=(SELECT UNIX_TIMESTAMP(NOW())*1000-(12*3600*1000))                
            
            ) AS sentiment_ensemble
            
        );
    """

    sql_for_insert_news_external_ensemble = """
        INSERT INTO analysis_global_sentiment(sentiment_type, sentiment_seconds_back, created_at_epoch_ms, sentiment_absolute)
        (
            SELECT 'news_external_ensemble', 12*3600, UNIX_TIMESTAMP(NOW())*1000, 
            AVG(sentiment_absolute)
            FROM 
            (

                SELECT client_received_ts_ms, news_sentiment_score as sentiment_absolute
                FROM data_benzinga_sentiment_rt
                WHERE client_received_ts_ms >=(SELECT UNIX_TIMESTAMP(NOW())*1000-(12*3600*1000))

                UNION

                SELECT client_received_ts_ms, current_buzz as sentiment_absolute
                FROM data_tipranks_news_sentiment_rt
                WHERE client_received_ts_ms >=(SELECT UNIX_TIMESTAMP(NOW())*1000-(12*3600*1000))                

            ) AS sentiment_ensemble

        );
    """

    sql_for_insert_global_external_ensemble = """
        INSERT INTO analysis_global_sentiment(sentiment_type, sentiment_seconds_back, created_at_epoch_ms, sentiment_absolute)
        (
            SELECT 'global_external_ensemble', 12*3600, UNIX_TIMESTAMP(NOW())*1000, 
            AVG(sentiment_absolute)
            FROM analysis_global_sentiment
            WHERE created_at_epoch_ms >= (SELECT UNIX_TIMESTAMP(NOW())*1000-(12*3600*1000))
            AND sentiment_type in ('news_external_ensemble', 'social_external_ensemble')
        );
    """

    sql_for_insert = [
        sql_for_insert_stocktwits, sql_for_insert_twitter, sql_for_insert_social_teslamonitor,
        sql_for_insert_social_external_ensemble, sql_for_insert_news_external_ensemble,
        sql_for_insert_global_external_ensemble
    ]

    logger.info(f'Running inserts: {sql_for_insert}')

    db_utils.update(
        use_ssh, sql_for_insert,
        db_host, db_user, db_password, db_port, database_name, ssh_username, ssh_password
    )

    logger.info(f'Completed inserts: {sql_for_insert}')


def compute_impact(post):
    msg_type = post['post_type']

    if msg_type in ['twitter-topic', 'twitter-user']:
        return 1 + post.get('likes_total', 0) + post.get('interaction_total', 0) * 3
    else:
        return 1 + post.get('likes_total', 0) + post.get('interaction_total', 0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-bs', '--batch_size', help='Number of records per read, between commits.', type=int, default=5)
    parser.add_argument('-db', '--database_name', help='Database where to store the data', type=str, default='automlpredictor_db_dashboard')
    parser.add_argument(
        '-idf', '--input_data_file_path', help='Path for the data file. If not specified, we\'ll read the data from stdin',
        type=str, required=False
    )
    parser.add_argument('-s', '--sleep_ms', help='Sleep in millisecs', type=int, default=50)
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

    processed_posts = LRU(50000)

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

        try:
            session = Session(engine)
            db_utils.operate_msgs_into_db(Base, session, input_msgs, session.merge)
            session.close()

            df_relevant_posts = get_relevant_posts(ssh, db_host, db_user, db_password, db_port, database_name, ssh_username, ssh_password)
            # print(df_relevant_posts)
            relevant_posts = df_relevant_posts.to_dict('records')
            posts_to_update = []

            for relevant_post in relevant_posts:
                # message_id, conversation_replies, likes_total
                current_id = relevant_post['message_id']
                values = {}
                current_post_prev_version = processed_posts.get(current_id, None)

                if relevant_post != current_post_prev_version:
                    processed_posts[current_id] = relevant_post
                    posts_to_update.append((current_id, compute_impact(relevant_post), relevant_post['post_type']))

            if len(posts_to_update) > 0:
                update_impact_in_db(posts_to_update, ssh, db_host, db_user, db_password, db_port, database_name, ssh_username, ssh_password)
                insert_current_global_sentiment_in_db(ssh, db_host, db_user, db_password, db_port, database_name, ssh_username, ssh_password)

        except:
            logger.error(f'An error occurred while managing session {session}: {traceback.format_exc()}')

        del input_msgs
        del session
        gc.collect()
        logger.info(f'Main loop: Going to sleep for {sleep_ms} milliseconds.')
        time.sleep(sleep_ms / 1000)

    if ssh:
        ssh_server.stop()

    if input_handle is not sys.stdin:
        input_handle.close()





