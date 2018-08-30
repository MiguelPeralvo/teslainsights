from datetime import datetime


def print_event(data, source, msg_type, data_source=None, schema='automlpredictor-common', schema_version='20180514.1', entity_id=1, entity_name='TESLA'):
  delta_ts = datetime.utcnow() - datetime(1970, 1, 1)
  client_received_ts_ms = int((delta_ts.days * 24 * 60 * 60 + delta_ts.seconds) * 1000 + delta_ts.microseconds / 1000.0)

  if not data_source:
    data_source = source

  json_final = f'{{"schema":"{schema}","schemaVersion":"{schema_version}","source":"{source}","msgType":"{msg_type}",' \
               f'"clientReceivedTsMs":{client_received_ts_ms},"entityId":{entity_id},"entityName":"{entity_name}","dataSource":"{data_source}","data":{data}}}'

  print(json_final)