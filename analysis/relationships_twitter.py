import argparse
import json
import networkx as nx
from time import sleep
from collections import namedtuple
import os
from twython import Twython
import traceback
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--relationship', help='Relationship: self/friend/follower', type=str, default='self', choices={'self', 'friend', 'follower'})
    parser.add_argument('-s', '--sentiment', help='Sentiment: bulls/bears', type=str, default='bears', choices={'musk', 'bulls', 'bears'})
    parser.add_argument('-sp', '--sleep_between_pages_ms',
                        help='Sleep between pages to be requested (once gone through all the items to be requested) in millisecs', type=int, default=60000)
    # parser.add_argument('-i', '--since_id', help='Returns results with an ID greater than (that is, more recent than) the specified ID.', type=int, default=995748957413429250)
    args = parser.parse_args()
    relationship = str(args.relationship)
    sentiment = str(args.sentiment)
    sleep_between_pages_ms = int(args.sleep_between_pages_ms)
    # since_id = int(args.since_id)

    consumer_key = os.getenv('TWITTER_CONSUMER_KEY')
    consumer_secret = os.getenv('TWITTER_CONSUMER_SECRET')

    # OAUTH2
    twitter = Twython(consumer_key, consumer_secret, oauth_version=2)
    access_token = twitter.obtain_access_token()

    twitter = Twython(consumer_key, access_token=access_token)

    TwitterList = namedtuple('TwitterList', ['list_id', 'list_name', 'list_uri'])

    musk_lists = [
        TwitterList(1014546986647851010, 'Elon', '/futuresemantics/lists/elon'),
    ]

    bulls_lists = [
        TwitterList(977204527764877317, 'tesla.geeks', '/c4chaos/lists/tesla-geeks'),
        TwitterList(1014227248541589504, 'Protesla-bulls', '/futuresemantics/lists/protesla-bulls'),
        TwitterList(1014213842266714112, 'tesla-vip', '/futuresemantics/lists/tesla-vip'),
    ]

    bears_lists = [
        TwitterList(140675384, 'tesla-haters', '/Harbles/lists/tesla-haters'),
        TwitterList(1014541206641086464, 'teslafudspreaders', '/futuresemantics/lists/teslafudspreaders'),
    ]


    if relationship == 'friend':
        relationship_method = twitter.get_friends_list
    elif relationship == 'follower':
        relationship_method = twitter.get_followers_list

    if sentiment == 'musk':
        lists_to_scan = musk_lists
    elif sentiment == 'bulls':
        lists_to_scan = bulls_lists
    elif sentiment == 'bears':
        lists_to_scan = bears_lists

    related_users = {}

    for twitter_list in lists_to_scan:
        try:
            # get_followers_ids
            list_users = twitter.cursor(twitter.get_list_members, list_id=twitter_list.list_id, return_pages=True, count=100)

            for page in list_users:
                logger.info(f"Getting page for list {twitter_list.list_id}  {twitter_list.list_name}")

                for user in page:

                    if relationship=='self':
                        related_users[user['id']] = {
                            'id': user['id'],
                            'name': user['name'],
                            'screen_name': user['screen_name']
                        }
                    else:
                        related_users_pages = twitter.cursor(relationship_method, screen_name=user['screen_name'], return_pages=True)

                        for related_users_page in related_users_pages:
                            logger.info(f"Getting page for user {user['screen_name']}")

                            for related_user in related_users_page:
                                if related_user['id'] not in related_users:
                                    related_users[related_user['id']] = {
                                        'id': related_user['id'],
                                        'name': related_user['name'],
                                        'screen_name': related_user['screen_name']
                                    }

                            sleep(sleep_between_pages_ms / 1000)

                        sleep(sleep_between_pages_ms / 1000)

                sleep(sleep_between_pages_ms / 1000)

        except:
            logger.error(f'An error occurred: {traceback.format_exc()}')

    for user in related_users.values():
        print(json.dumps(user))

