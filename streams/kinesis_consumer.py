import argparse
from botocore.exceptions import ClientError
import boto3
import json
from datetime import datetime
import time
import _thread

import traceback
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException',
					'ThrottlingException')
MAX_RETRIES = 5

# arn:aws:kinesis:eu-west-1:631645402277:stream/automlpredictor-tesla-test

def process_shard(shard, sleep_ms, shard_iterator_type, args):
	shard_id = shard['ShardId']
	logger.info(f'Reading from shard: {shard_id}')

	while True:

		if shard_iterator_type == 'AT_TIMESTAMP':
			since_ts_epoch_ms = float(args.since_ts_epoch_ms)
			shard_iterator = kinesis_client.get_shard_iterator(
				StreamName=stream_name,
				ShardId=shard_id,
				ShardIteratorType=shard_iterator_type,
				Timestamp=since_ts_epoch_ms
			)

		elif shard_iterator_type in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER']:
			starting_sequence_number = str(args.starting_sequence_number)
			shard_iterator = kinesis_client.get_shard_iterator(
				StreamName=stream_name,
				ShardId=shard_id,
				ShardIteratorType=shard_iterator_type,
				StartingSequenceNumber=starting_sequence_number,
			)
		else:
			shard_iterator = kinesis_client.get_shard_iterator(
				StreamName=stream_name,
				ShardId=shard_id,
				ShardIteratorType=shard_iterator_type,
			)

		# ShardIteratorType='LATEST')
		# Valid Values: AT_SEQUENCE_NUMBER | AFTER_SEQUENCE_NUMBER | TRIM_HORIZON | LATEST | AT_TIMESTAMP

		shard_iterator = shard_iterator['ShardIterator']
		record_response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=batch_size)
		retries = 0

		while 'NextShardIterator' in record_response:
			try:
				record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'], Limit=batch_size)

				for record in record_response['Records']:
					try:
						data_record = record['Data']

						if data_record and data_record != '\n':
							print(json.dumps(json.loads(data_record)))

					except Exception as e:
						logger.error(f'JSON parsing failed for record {data_record}: {traceback.format_exc()}')

			except ClientError as err:

				if err.response['Error']['Code'] == 'ExpiredIteratorException':
					logger.info(f'Expired iterator. Moving on.')
					break

				elif err.response['Error']['Code'] not in RETRY_EXCEPTIONS:
					raise

				if retries == MAX_RETRIES:
					logger.error(f'Max retries reached({MAX_RETRIES}), moving on.')
					break

				logger.warning(f'Slowing it down. Retries={retries}.')
				time.sleep(5 ** (retries + 2))
				retries += 1

			# except:
			# 	logging.error(f'An error occurred: {traceback.format_exc()}')

		time.sleep(sleep_ms / 1000)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-bs', '--batch_size', help='Number of records per read, between sleeps.', type=int, default=50)
	parser.add_argument('-s', '--sleep_ms', help='Sleep in millisecs', type=int, default=250)
	parser.add_argument('-ts', '--since_ts_epoch_ms', help='Read since ts epoch_ms. Mandatory for shard_iterator_type=AT_TIMESTAMP', type=float, required=False)
	parser.add_argument('-sn', '--stream_name', help='Sleep in millisecs', type=str, required=True)
	parser.add_argument('-r', '--region_name', help='AWS region name', type=str, default='eu-west-1')
	parser.add_argument('-sit', '--shard_iterator_type', help='Shard Iterator Type: ', type=str, default='LATEST',
						choices=['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER', 'TRIM_HORIZON', 'LATEST', 'AT_TIMESTAMP'])
	parser.add_argument('-sqn', '--starting_sequence_number', help='StartingSequenceNumber for AT_SEQUENCE_NUMBER/AFTER_SEQUENCE_NUMBER. '
																   'Mandatory for shard_iterator_type=[AT_SEQUENCE_NUMBER|AFTER_SEQUENCE_NUMBER]',
						type=str, required=False)

	args = parser.parse_args()
	batch_size = int(args.batch_size)
	sleep_ms = int(args.sleep_ms)

	stream_name = str(args.stream_name)
	region_name = str(args.region_name)
	shard_iterator_type = str(args.shard_iterator_type)

	kinesis_client = boto3.client('kinesis', region_name=region_name)
	response = kinesis_client.describe_stream(StreamName=stream_name)

	# TODO: KCL
	for shard in response['StreamDescription']['Shards']:
		_thread.start_new_thread(process_shard(shard, sleep_ms, shard_iterator_type, args))

	kinesis_client.close()
