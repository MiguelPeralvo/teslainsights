import argparse
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
    parser.add_argument('-s', '--sleep_ms', help='Sleep between cycles (once gone through all the items to be requested) in millisecs', type=int, default=60000)
    parser.add_argument('-sp', '--sleep_between_pages_ms',
                        help='Sleep between pages to be requested (once gone through all the items to be requested) in millisecs', type=int, default=15000)
    # parser.add_argument('-i', '--since_id', help='Returns results with an ID greater than (that is, more recent than) the specified ID.', type=int, default=995748957413429250)
    args = parser.parse_args()
    sleep_ms = int(args.sleep_ms)
    sleep_between_pages_ms = int(args.sleep_between_pages_ms)
    # since_id = int(args.since_id)



    consumer_key = os.getenv('TWITTER_CONSUMER_KEY')
    consumer_secret = os.getenv('TWITTER_CONSUMER_SECRET')

    # OAUTH2
    twitter = Twython(consumer_key, consumer_secret, oauth_version=2)
    access_token = twitter.obtain_access_token()

    twitter = Twython(consumer_key, access_token=access_token)

    TwitterList = namedtuple('TwitterList', ['list_id', 'list_name', 'list_uri'])

    wanted_lists = [
        TwitterList(1014227248541589504, 'Protesla-bulls', '/futuresemantics/lists/protesla-bulls'),
        TwitterList(1014541206641086464, 'teslafudspreaders', '/futuresemantics/lists/teslafudspreaders'),
        TwitterList(1014213842266714112, 'tesla-vip', '/futuresemantics/lists/tesla-vip'),
    ]

    friends = {}

    for twitter_list in wanted_lists:
        try:
            list_users = twitter.cursor(twitter.get_list_members, list_id=twitter_list.list_id, return_pages=True, count=100)
            logger.info(f"Getting the first page for list {twitter_list.list_name}")

            for page in list_users:
                for user in page:
                    user_friends = twitter.cursor(twitter.get_friends_ids, screen_name=user['screen_name'], return_pages=True)

                    for friends_page in user_friends:
                        friends_list = friends.get(user['screen_name'], [])
                        friends_list.extend(friends_page)
                        friends[user['screen_name']] = friends_list

                    sleep(sleep_between_pages_ms / 1000)

                sleep(sleep_between_pages_ms / 1000)

            print(friends)
        except:
            logger.error(f'An error occurred: {traceback.format_exc()}')
            # twitter_connection = Twython(consumer_key, access_token=access_token)

        sleep(sleep_between_pages_ms / 1000)


    print(friends)

# Create graph
# print "Adding followers relationships..."
# for user in followers:
# 	graph.add_edge(user.name,username)
#
# print "Adding following relationships..."
# for user in friends:
# 	graph.add_edge(username,user.name)
#
# # Save graph
# print ""
# print "The personal profile was analyzed succesfully."
# print ""
# print "Saving the file as "+username+"-personal-network.gexf..."
# nx.write_gexf(graph, username+"-personal-network.gexf")