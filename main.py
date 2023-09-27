import asyncio
import aiohttp
import pandas as pd
import time
from urllib.parse import urljoin, urlencode
from datetime import datetime

from cred import api_key

size_high = size_low = 3



async def get_kline_data(api_key, symbol, interval, limit=None, futures=False, start_time=None, end_time=None):
    """
    :param api_key: (str) api key of binance account;
    :param symbol: (str) trading pair. ex: 'ETHUSDT';
    :param interval: (str) interval of candles. ex: 1m, 5m, 30m, 1h, 1d;
    :param limit: (int) number of candles (default 500, max 1000);
    :param start_time: (long) time in ms;
    :param end_time: (long) time in ms;
    :return: (pandas Data Frame) CandleStick Data;
    """
    api_key = api_key
    
    if futures:        
        BASE_URL = 'https://fapi.binance.com'
        PATH = '/fapi/v1/klines'
        
    else:        
        BASE_URL = 'https://api.binance.com'
        PATH = '/api/v1/klines'
        
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit,
        'startTime': start_time,
        'endTime' : end_time
    }
    headers = {'X-MBX-APIKEY': api_key}
    url = urljoin(BASE_URL, PATH)

    async with aiohttp.ClientSession() as session:

        async with session.get(url, headers=headers, params=params) as response:
            klines = await response.json()

    df = pd.DataFrame(klines)
    df.drop(columns=df.columns[-1], inplace=True)
    df.columns = ['open_time', 'open_price', 'high_price', 'low_price', 'close_prise', 'volume', 'close_time',
                  'asset_volume', 'number_trades', 'taker_buy_base', 'taker_buy_quote']
    return df




async def get_order_book(api_key, symbol, limit, futures=False, start_time=None, end_time=None):

    """
    :param api_key: (str) api key of binance account;
    :param symbol: (str) trading pair. ex: 'ETHUSDT';
    :param limit: (int) number of orders (max 5000);
    :param start_time: (long) time in ms;
    :param end_time: (long) time in ms;
    :return df_asks, df_bids: (Data Frame, Data Frame) orderbooks with asks and bids;
    """
    
    if futures:
        BASE_URL = 'https://fapi.binance.com'
        PATH = '/fapi/v1/depth'
        
    else:
        BASE_URL = 'https://api.binance.com'
        PATH = '/api/v3/depth'
        
    params = {
        'symbol': symbol,
        'limit': limit
    }
    headers = {'X-MBX-APIKEY': api_key}
    url = urljoin(BASE_URL, PATH)
    anomal_quantity = 10

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as response:
            depth = await response.json()

    df_bids = pd.DataFrame(depth['bids'], columns=['low_price', 'quantity_bid'])
    df_bids = df_bids.astype(float)
    anomal_bids = df_bids[df_bids['quantity_bid'] > anomal_quantity]
    #print(anomal_bids)
    quantity_bid = df_bids['quantity_bid'].sum()
    #print('\\\\\\\\\\\\\\\\\\\\\\\\\\\\')
    #print(anomal_bids)
    
    df_asks = pd.DataFrame(depth['asks'], columns=['high_price', 'quantity_ask'])
    df_asks = df_asks.astype(float)
    anomal_asks = df_asks[df_asks['quantity_ask'] > anomal_quantity]
    #print(anomal_asks)
    quantity_ask = df_asks['quantity_ask'].sum()
    #print('\\\\\\\\\\\\\\\\\\\\\\\\\\\\')
    #print(anomal_asks)

    print(df_asks)
    print('\\\\\\')
    print(df_bids)
    return df_asks, df_bids



async def create_df_high(df):
    """
    :param df: (pandas Data Frame) CandleStick Data;
    :return df_high: (pandas Data Frame) CandleStick Highs Data;

    """

    global size_high
    last_string = 0
    df_high = pd.DataFrame(columns=['open_time', 'high_price', 'num_kicks'])
    num = 0
    delta = 2.0

    while num < df.shape[0]:        
        not_high = False        
        if df_high.shape[0] == 0:
            current_line = pd.Series({'open_time': df.iloc[num]['open_time'], 'high_price' : df.iloc[num]['high_price'], 'num_kicks' : 1})
            df_high.loc[len(df_high.index)] = current_line

        else:
            last_row_index = df_high.shape[0] - 1
            if float(df_high.iloc[last_row_index]['high_price']) + delta < float(df.iloc[num]['high_price']):
                df_high = df_high.drop(last_row_index)
                num -= 1
                
            elif float(df_high.iloc[last_row_index]['high_price']) - delta < float(df.iloc[num]['high_price']) :
                num_kick = df_high.loc[last_row_index, 'num_kicks'] + 1
                df_high.loc[last_row_index, 'num_kicks'] = num_kick
                #print(df_high.loc[last_row_index, 'high_price'], df_high.loc[last_row_index, 'num_kicks'])
                
            else:
                if num >= size_high:
                    for i in range(size_high):
                        if float(df.iloc[num]['high_price']) < float(df.iloc[num - i]['high_price']):
                            not_high = True
                            break
                    if not not_high:
                        current_line = pd.Series({'open_time': df.iloc[num]['open_time'], 'high_price' : df.iloc[num]['high_price'], 'num_kicks' : 1})
                        df_high.loc[len(df_high.index)] = current_line
        num += 1
    
    df_high['high_price'] = df_high['high_price'].astype(float)
    print(df_high)
    return df_high



async def create_df_low(df):
    """
    :param df: (pandas Data Frame) CandleStick Data;
    :return df_low: (pandas Data Frame) CandleStick Lows Data;

    """

    global size_low
    last_string = 0
    df_low = pd.DataFrame(columns=['open_time', 'low_price', 'num_kicks'])
    num = 0

    while num < df.shape[0]:        
        not_low = False        
        if df_low.shape[0] == 0:
            current_line = pd.Series({'open_time': df.iloc[num]['open_time'], 'low_price' : df.iloc[num]['low_price'], 'num_kicks' : 1})
            df_low.loc[len(df_low.index)] = current_line

        else:
            last_row_index = df_low.shape[0] - 1           
            if df_low.iloc[last_row_index]['low_price'] > df.iloc[num]['low_price']:
                df_low = df_low.drop(last_row_index)
                num -= 1
                
            elif df_low.iloc[last_row_index]['low_price'] == df.iloc[num]['low_price']:
                num_kick = df_low.loc[last_row_index, 'num_kicks'] + 1
                df_low.loc[last_row_index, 'num_kicks'] = num_kick
                #print(df_low.loc[last_row_index, 'low_price'], df_low.loc[last_row_index, 'num_kicks'])
                
            else:
                if num >= size_low:
                    for i in range(size_low):
                        if float(df.iloc[num]['low_price']) > float(df.iloc[num - i]['low_price']):
                            not_low = True
                            break
                    if not not_low:
                        current_line = pd.Series({'open_time': df.iloc[num]['open_time'], 'low_price' : df.iloc[num]['low_price'], 'num_kicks' : 1})
                        df_low.loc[len(df_low.index)] = current_line
        num += 1
    
    df_low['low_price'] = df_low['low_price'].astype(float)
    print(df_low)
    return df_low


                
    
    




symbol = "BTCUSDT"
interval = '30m'

if interval == '30m':
    add_time_ms = 1800000000
    
date_time = datetime(2021, 6, 1, 0, 0, 0)
start_time = int(date_time.timestamp()) * 1000

current_datetime = datetime.now()
current_timestamp = int(current_datetime.timestamp()) * 1000

stop_time = start_time + add_time_ms

df_futures_klines = asyncio.run(get_kline_data(api_key, symbol, interval, 1000, True, start_time, stop_time))
time.sleep(1)
df_futures_highs = asyncio.run(create_df_high(df_futures_klines))

"""
df_futures_klines = asyncio.run(get_kline_data(api_key, symbol, interval, 1000, True))
time.sleep(1)
df_futures_asks, df_futures_bids = asyncio.run(get_order_book(api_key, symbol, 1000, True))
time.sleep(1)
df_futures_lows = asyncio.run(create_df_low(df_futures_klines))
time.sleep(1)
df_futures_highs = asyncio.run(create_df_high(df_futures_klines))

futures_lows = pd.merge(df_futures_lows, df_futures_bids, on='low_price')
futures_highs = pd.merge(df_futures_highs, df_futures_asks, on='high_price')

print('Future highs')
print(futures_highs)
print('Future lows')
print(futures_lows)

"""

"""
time.sleep(1)
df_spot_klines = asyncio.run(get_kline_data(api_key, symbol, interval, 1000))
time.sleep(1)
df_spot_asks, df_spot_bids = asyncio.run(get_order_book(api_key, symbol, 1000))
time.sleep(1)
df_spot_highs = asyncio.run(create_df_high(df_spot_klines))
time.sleep(1)
df_spot_lows = asyncio.run(create_df_low(df_spot_klines))

spot_highs = pd.merge(df_spot_highs, df_futures_asks, on='high_price')
spot_lows = pd.merge(df_spot_lows, df_spot_bids, on='low_price')

print('Spot highs')
print(spot_highs)
print('Spot lows')
print(spot_lows)

"""












