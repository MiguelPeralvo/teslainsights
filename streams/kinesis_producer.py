import argparse
import boto3
import json
from datetime import datetime
import calendar
import random
import sys
import time
import traceback
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# arn:aws:kinesis:eu-west-1:631645402277:stream/automlpredictor-tesla-test

def put_to_stream(kinesis_client, raw_records):
  records = []
  put_response = None

  for raw_record in raw_records:

    if raw_record and raw_record != '\n':
      # TODO: Revisit, it's good enough for the time being, because we know
      pk = str(hash(raw_record[73:200]))
      record = {'Data': raw_record, 'PartitionKey': pk}
      records.append(record)

  try:
    # logger.info(f'Records for Put request: {records}')
    put_response = kinesis_client.put_records(
      Records=records,
      StreamName=stream_name,
    )
    # logger.info(f'Put response: {put_response} for alert_records.')
  except Exception:
    logging.error(f'Transmission Failed when sending records: {records} :{traceback.format_exc()}')

  return put_response

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-bs', '--batch_size', help='Number of records per write/flush, between sleeps.', type=int, default=5)
  parser.add_argument('-p', '--print', help='Print stdin', action='store_true')
  parser.add_argument('-s', '--sleep_ms', help='Sleep in millisecs', type=int, default=0)
  parser.add_argument('-sn', '--stream_name', help='Sleep in millisecs', type=str, required=True)
  parser.add_argument('-r', '--region_name', help='AWS region name', type=str, default='eu-west-1')

  args = parser.parse_args()
  batch_size = int(args.batch_size)
  sleep_ms = int(args.sleep_ms)
  stream_name = str(args.stream_name)
  region_name = str(args.region_name)

  kinesis_client = boto3.client('kinesis', region_name=region_name)
  # response = kinesis_client.describe_stream(StreamName=stream_name)
  n_messages = 0
  raw_records = []

  while True:
      line = sys.stdin.readline().rstrip()

      if args.print:
        print(line)

      raw_records.append(line)
      n_messages += 1

      if n_messages == args.batch_size:
        # TODO: Error handling/retry of failed records
        put_response = put_to_stream(kinesis_client, raw_records)
        n_messages = 0
        raw_records = []
        time.sleep(sleep_ms/1000)