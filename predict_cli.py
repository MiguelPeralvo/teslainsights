import matplotlib
matplotlib.use('TkAgg')
from inference import sentiment_inference
from inference import message_utils
import argparse
import json
from collections import namedtuple
import sys
import os
sys.excepthook = sys.__excepthook__  # See https://groups.io/g/insync/topic/13778827?p=,,,20,0,0,0::recentpostdate%2Fsticky,,,20,2,0,13778827

import traceback
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

Model = namedtuple('Model', ['stoi', 'classification_model', 'itos_file_path', 'trained_classifier_file_path'])

# TODO: Generalise at strike 3.
def load_models(
    _stocktwits_itos_file_path, _stocktwits_trained_classifier_file_path,
    _twitter_itos_file_path, _twitter_trained_classifier_file_path
):

    for path in [
        _stocktwits_itos_file_path, _stocktwits_trained_classifier_file_path,
        _twitter_itos_file_path, _twitter_trained_classifier_file_path
    ]:
        if not os.path.exists(path):
            logger.error(f'Could not find {path}')
            exit(-1)

    stocktwits_stoi, stocktwits_model = sentiment_inference.load_model(_stocktwits_itos_file_path, _stocktwits_trained_classifier_file_path)
    twitter_stoi, twitter_model = sentiment_inference.load_model(_twitter_itos_file_path, _twitter_trained_classifier_file_path)

    return {
        'stocktwit-sentiment-request': Model(stocktwits_stoi, stocktwits_model, _stocktwits_itos_file_path, _stocktwits_trained_classifier_file_path),
        'twitter-user-sentiment-request': Model(twitter_stoi, twitter_model, _twitter_itos_file_path, _twitter_trained_classifier_file_path),
        'twitter-topic-sentiment-request': Model(twitter_stoi, twitter_model, _twitter_itos_file_path, _twitter_trained_classifier_file_path),
    }


def predict_input(models, input_data_file_path, batch_size, sleep_ms):

    vader_analyzer = sentiment_inference.load_vader_analyzer()


    if input_data_file_path:
        input_handle = open(input_data_file_path, 'r')
    else:
        input_handle = sys.stdin

    for input_msgs in message_utils.read_json_input(batch_size, input_handle, sleep_ms):
        # print(input_msgs)
        for json_record in input_msgs:
            msg_type = json_record['msgType']
            stoi = models[msg_type].stoi
            model = models[msg_type].classification_model
            itos_file_path = models[msg_type].itos_file_path
            trained_classifier_file_path = models[msg_type].trained_classifier_file_path

            yield(sentiment_inference.predict_json_record(
                json_record, stoi, model, vader_analyzer, itos_file_path,
                trained_classifier_file_path, input_data_file_path)
            )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-sif', '--stocktwits_itos_file_path', help='Path for the data file. If not specified, we\'ll read the data from stdin', type=str, required=True)
    parser.add_argument('-stcf', '--stocktwits_trained_classifier_file_path', help='Path for the data file. If not specified, we\'ll read the data from stdin', type=str,
                        required=True)

    parser.add_argument('-tif', '--twitter_itos_file_path', help='Path for the data file. If not specified, we\'ll read the data from stdin', type=str, required=True)
    parser.add_argument('-ttcf', '--twitter_trained_classifier_file_path', help='Path for the data file. If not specified, we\'ll read the data from stdin', type=str,
                        required=True)

    parser.add_argument('-bs', '--batch_size', help='Number of records per read.', type=int, default=500)
    parser.add_argument('-idf', '--input_data_file_path', help='Path for the data file. If not specified, we\'ll read the data from stdin', type=str,
                        required=False)
    parser.add_argument('-s', '--sleep_ms', help='Sleep in millisecs', type=int, default=1000)

    args = parser.parse_args()
    stocktwits_itos_file_path = args.stocktwits_itos_file_path
    stocktwits_trained_classifier_file_path = args.stocktwits_trained_classifier_file_path
    twitter_itos_file_path = args.twitter_itos_file_path
    twitter_trained_classifier_file_path = args.twitter_trained_classifier_file_path
    input_data_file_path = args.input_data_file_path
    batch_size = int(args.batch_size)
    sleep_ms = int(args.sleep_ms)


    models = load_models(
        stocktwits_itos_file_path, stocktwits_trained_classifier_file_path,
        twitter_itos_file_path, twitter_trained_classifier_file_path
    )

    for json_line in predict_input(models, input_data_file_path, batch_size, sleep_ms):
        print(json.dumps(json_line))
