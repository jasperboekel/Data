import psycopg2
import os
import datetime as dt
import uuid
import itertools
import logging 
import logging.config
import json
import socket
import pandas as pd
import psycopg2.extras as extras
from sqlalchemy import create_engine
from contextlib import contextmanager
from timer import timed
from io import StringIO
from pathlib import Path

SELF_DIR = Path(__file__).resolve().parent

with open(SELF_DIR / 'logging.config') as f:
    LOG_CONFIG = json.loads(f.read())

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("data")

with open(SELF_DIR / '.common.json') as f:
    common_vars = json.loads(f.read())
if socket.gethostname() == 'postgres':
    file = '.prod.json'
else:
    file = '.dev.json'
with open(SELF_DIR / file) as f:
    env_vars = json.loads(f.read())
         
env_vars.update(common_vars)    


user = env_vars['user']
password = env_vars['password']
database = env_vars['database']
host = env_vars['host']


@contextmanager
def get_connection():
    con = None
    try:
        con = psycopg2.connect(user=user, password=password, host=host, database=database)
        yield con
    except (Exception, psycopg2.DatabaseError) as error:
        logger.exception(error)
    finally:
        con.close()

@timed
def df_to_sql(schema, table, df):    
    connect = f'postgresql+psycopg2://{user}:{password}@{host}:5432/{database}'
    engine = create_engine(connect)
    try:
        df.to_sql(name=table, schema=schema, con=engine, index=False, if_exists='append')
    except Exception as error:
        logger.exception(error)

@timed
def timescaledb_parallel_copy(schema, table, df, workers=1):
    cols = get_columns(schema, table)
    filename = str(uuid.uuid4()) + '.csv'
    try:
        df[cols].to_csv(filename, index=False)
        insert_command = f''' timescaledb-parallel-copy \
                            --connection "postgres://{user}:{password}@{host}:5432/{database}" \
                            --schema {schema} --table {table} --file {filename} \
                            --workers {workers} --reporting-period 1s --skip-header '''
        logger.debug(insert_command)
        os.system(insert_command)
    except Exception as error:
        logger.exception(error)
    finally:
        os.system(f''' rm {filename} ''')

@timed
def execute_values(con, schema, table, df, notify_message = None):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query  = "INSERT INTO %s.%s(%s) VALUES %%s" % (schema, table, cols)
    cursor = con.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        con.commit()
        if notify_message is not None:
            cursor.execute(notify_message)
            con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        logger.exception(error)
        con.rollback()
        cursor.close()
    finally:
        print("execute_values() done")
        cursor.close()
    
@timed
def copy_from_stringio(schema, table, df, notify_message = None):
    cols = get_columns(schema, table)
    buffer = StringIO()
    df[cols].to_csv(buffer, header=True, index=False)
    buffer.seek(0)
    sql = f"COPY {schema}.{table} FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    with get_connection() as con:
        cursor = con.cursor()
        try:
            cursor.copy_expert(sql, file=buffer)
            con.commit()
            if notify_message is not None:
                cursor.execute(notify_message)
                con.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.exception(error)
            con.rollback()


def get_columns(schema, table) -> list:
    query = '''
            SELECT
                column_name
            FROM information_schema.columns
            WHERE table_schema = %s
            AND table_name = %s 
            order by ordinal_position
            '''
    with get_connection() as con:
        cursor = con.cursor()
        cursor.execute(query, (schema, table))
        cols = cursor.fetchall()
    
    cols = list(itertools.chain(*cols))
    return cols


def get_latest_timestamp(schema, table, coin_id) -> dt.datetime:
    query = f''' select max(timestamp) from {schema}.{table} where coin_id = %s '''

    with get_connection() as con:
        cursor = con.cursor()
        cursor.execute(query, (coin_id,))
        timestamp = cursor.fetchone()[0]
    
    return timestamp


def get_latest_row(symbol = 'BTC'):
    query = '''select *
                from prices.coins p 
                join common.coins c using(coin_id)
                where p.abbreviation = %(symbol)s
                order by "timestamp" desc limit 1 '''
    with get_connection() as con:
        df = pd.read_sql(query, con, params={'symbol': symbol})
        
    return df


def get_historical_rows(symbol = 'BTC', past_timestamp = ''):
    query = '''select *
                from prices.coins p 
                join common.coins c using(coin_id)
                where p.abbreviation = %(symbol)s
                and p.timestamp >= %(past_timestamp)s'''
    with get_connection() as con:
        df = pd.read_sql(query, con, params={'symbol': symbol, 'past_timestamp': past_timestamp})
        
    return df


def trades_to_database(schema:str, table:str, stratname:str, list_of_trade_objects:list):
    # construct df from list of trades
    trades = [p.summary() for p in list_of_trade_objects]
    df_trades = pd.DataFrame(trades).set_index('timestamp')
    # check if the stratname and parameters already exist in the parameters table and add ID to the column


    # add the df to the results table 
    df_to_sql(schema, table, df_trades)
