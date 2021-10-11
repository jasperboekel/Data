import datetime as dt
import pandas as pd
import database as db
import logging
import logging.config
import json
from binance.client import Client
from timer import timed
from itertools import chain, islice
from datetime import timedelta
from pathlib import Path

SELF_DIR = Path(__file__).resolve().parent



with open(SELF_DIR / 'logging.config') as f:
    LOG_CONFIG = json.loads(f.read())

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("data")

with open(SELF_DIR / '.common.json') as f:
    env_vars = json.loads(f.read())

binance_api_key = env_vars['binancePublicKey']
binance_api_secret = env_vars['binancePrivateKey']
binance_client = Client(api_key=binance_api_key, api_secret=binance_api_secret)


TOMORROW = dt.datetime.today() + dt.timedelta(days=1)
COLS = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ]
FORMAT = "%d %b %Y %H:%M:%S"
COIN_IDS = {'BTCUSDT': 1,
            'ETHUSDT': 2,
            'LTCUSDT': 3,
            'BNBUSDT': 4,
            'LINKUSDT': 5}


def chunks(iterator, size=50_000):
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))


def parse_klines(data, symbol) -> pd.DataFrame:
    df = pd.DataFrame(data, columns=COLS)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
    df['coin_id'] = COIN_IDS[symbol]
    df['datasource_id'] = 1
    df['dumptime'] = dt.datetime.now()
    return df

data = {'stream': 'ethusdt@ticker', 'data': {'e': '24hrTicker', 'E': 1633981704662, 's': 'ETHUSDT', 'p': '18.26000000', 'P': '0.518', 'w': '3522.35414264', 'x': '3525.11000000', 'c': '3542.98000000', 'Q': '0.12830000', 'b': '3542.97000000', 'B': '7.66560000', 'a': '3542.98000000', 'A': '1.96410000', 'o': '3524.72000000', 'h': '3623.80000000', 'l': '3371.00000000', 'v': '546840.51980000', 'q': '1926165970.28086400', 'O': 1633895304662, 'C': 1633981704662, 'F': 630179434, 'L': 631485793, 'n': 1306360}}

@timed
def parse_multiplex_socket(data):
    df = pd.DataFrame(data['data'], index=[0])
    df = df.rename(columns={'E': 'timestamp', 'o': 'open', 'h': 'high',
                            'l': 'low', 'c': 'close', 'v': 'volume', 's': 'symbol'
                          })
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    symbol = df['symbol'].iloc[0]
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]     
    df[['quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore']] = 0
    df['coin_id'] = COIN_IDS[symbol]
    df['datasource_id'] = 1
    df['dumptime'] = dt.datetime.now()
    return df


@timed
def scrape(symbol: str, kline_size: str, start: dt.datetime, end: dt.datetime = TOMORROW) -> pd.DataFrame:
    try:
        data = binance_client.get_historical_klines(symbol, kline_size, start.strftime(FORMAT), end.strftime(FORMAT))
    except Exception as error:
        print(error)
        logger.exception(error)
        data = [] 
    df = parse_klines(data, symbol)
    logger.debug('coin = {symbol}, df.shape = {shape}'.format(symbol=symbol, shape=df.shape))
    return df


def scrape_historical(symbol: str, kline_size: str, start: dt.datetime, end: dt.datetime = TOMORROW):
    klines = binance_client.get_historical_klines_generator(symbol, kline_size, 
                                                            start.strftime(FORMAT), 
                                                            end.strftime(FORMAT))
    for batch in chunks(klines, size=250_000):
        df = parse_klines(batch, symbol)
        db.timescaledb_parallel_copy(schema='prices', table='coins', df=df)


#@task(max_retries=5, retry_delay=timedelta(seconds=3))
@timed
def scrape_scheduler(symbol: str, kline_size: str):
    print(symbol)
    print(dt.datetime.now())
    start = db.get_latest_timestamp(schema='prices', table='coins', coin_id=COIN_IDS[symbol]) + dt.timedelta(minutes=1)
    df = scrape(symbol, kline_size, start)
    if df.empty:
        raise NoDataError
    db.copy_from_stringio('prices', 'coins', df, f"NOTIFY test, 'new {symbol}';")
    print(dt.datetime.now())


class NoDataError(Exception):
    pass

