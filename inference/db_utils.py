import sys
sys.excepthook = sys.__excepthook__ # See https://groups.io/g/insync/topic/13778827?p=,,,20,0,0,0::recentpostdate%2Fsticky,,,20,2,0,13778827
import time
import mysql.connector as sql
import pandas as pd
from sshtunnel import SSHTunnelForwarder
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
      return pd.read_sql_query(q, conn)
  else:
    conn = sql.connect(host=db_host,
                       port=db_port,
                       user=db_user,
                       passwd=db_password,
                       db=db,
                       charset=charset)
    return pd.read_sql_query(q, conn)