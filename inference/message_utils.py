from datetime import datetime
from splitstream import splitfile
import json
import time
import traceback
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def print_event(data, source, msg_type, data_source=None, schema='automlpredictor-common', schema_version='20180514.1', entity_id=1, entity_name='TESLA'):
    delta_ts = datetime.utcnow() - datetime(1970, 1, 1)
    client_received_ts_ms = int((delta_ts.days * 24 * 60 * 60 + delta_ts.seconds) * 1000 + delta_ts.microseconds / 1000.0)

    if not data_source:
        data_source = source

    json_final = f'{{"schema":"{schema}","schemaVersion":"{schema_version}","source":"{source}","msgType":"{msg_type}",' \
                 f'"clientReceivedTsMs":{client_received_ts_ms},"entityId":{entity_id},"entityName":"{entity_name}","dataSource":"{data_source}","data":{data}}}'

    print(json_final)


# TODO: Implement timeout?
def read_json_input(batch_size, input_handle, sleep_ms):
    def read_line():
        for jsonline in splitfile(input_handle, format="json"):
            try:
                yield json.loads(jsonline)
            except Exception:
                logger.error(f'JSON parsing failed for record {jsonline}: {traceback.format_exc()}')

    while True:
        n_lines = 0
        msgs = []

        for json_msg in read_line():
            msgs.append(json_msg)
            n_lines += 1

            if n_lines == batch_size:
                yield msgs.copy()
                n_lines = 0
                msgs = []

        if n_lines > 0:
            yield msgs.copy()

        # If nothing new in the file, we sleep
        logger.info(f'read_json_input loop: Going to sleep for {sleep_ms} milliseconds.')
        time.sleep(sleep_ms / 1000)
