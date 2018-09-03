import matplotlib
matplotlib.use('TkAgg')
from inference import sentiment_inference
from inference import message_utils
import argparse
import json

import sys
import os
sys.excepthook = sys.__excepthook__  # See https://groups.io/g/insync/topic/13778827?p=,,,20,0,0,0::recentpostdate%2Fsticky,,,20,2,0,13778827

import traceback
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



def predict_input(
        itos_file_path, trained_classifier_file_path, input_data_file_path, batch_size, sleep_ms,
):
    # Check the itos file exists
    if not os.path.exists(itos_file_path):
        logger.error(f'Could not find {itos_file_path}')
        exit(-1)

    # Check the classifier file exists
    if not os.path.exists(trained_classifier_file_path):
        logger.error(f'Could not find {trained_classifier_file_path}')
        exit(-1)

    stoi, model = sentiment_inference.load_model(itos_file_path, trained_classifier_file_path)

    if input_data_file_path:
        input_handle = open(input_data_file_path, 'r')
    else:
        input_handle = sys.stdin

    for input_msgs in message_utils.read_json_input(batch_size, input_handle, sleep_ms):
        # print(input_msgs)
        for json_record in input_msgs:
            yield(sentiment_inference.predict_json_record(
                json_record, stoi, model, itos_file_path,
                trained_classifier_file_path, input_data_file_path)
            )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-if', '--itos_file_path', help='Path for the data file. If not specified, we\'ll read the data from stdin', type=str, required=True)
    parser.add_argument('-tcf', '--trained_classifier_file_path', help='Path for the data file. If not specified, we\'ll read the data from stdin', type=str,
                        required=True)
    parser.add_argument('-bs', '--batch_size', help='Number of records per read.', type=int, default=1)
    parser.add_argument('-idf', '--input_data_file_path', help='Path for the data file. If not specified, we\'ll read the data from stdin', type=str,
                        required=False)
    parser.add_argument('-s', '--sleep_ms', help='Sleep in millisecs', type=int, default=1000)

    args = parser.parse_args()
    itos_file_path = args.itos_file_path
    trained_classifier_file_path = args.trained_classifier_file_path
    input_data_file_path = args.input_data_file_path
    batch_size = int(args.batch_size)
    sleep_ms = int(args.sleep_ms)

    for json_line in predict_input(itos_file_path, trained_classifier_file_path, input_data_file_path, batch_size, sleep_ms):
        print(json.dumps(json_line))
