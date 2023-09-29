import asyncio
import aiohttp
import pandas as pd
import time
import os
import csv
from urllib.parse import urljoin, urlencode
from datetime import datetime

from cred import api_key, way_to_dir

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
    if not df.empty and len(df.columns) > 0:
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
    print(anomal_asks)
    quantity_ask = df_asks['quantity_ask'].sum()
    print('\\\\\\\\\\\\\\\\\\\\\\\\\\\\')


    print(df_asks)
    print('\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\')
    print(df_bids)
    return df_asks, df_bids



async def create_df_high(df_new, df_old):
    """
    :param df: (pandas Data Frame) CandleStick Data;
    :return df_high: (pandas Data Frame) CandleStick Highs Data;

    """

    global size_high
    last_string = 0
    
    num = 0
        
    delta = 2.0

    while num < df_new.shape[0]:        
        not_high = False        
        if df_old.shape[0] == 0:
            current_line = pd.Series({'open_time': df_new.iloc[num]['open_time'], 'high_price' : df_new.iloc[num]['high_price'], 'num_kicks' : 1})
            df_old.loc[len(df_old.index)] = current_line

        else:
            last_row_index = df_old.shape[0] - 1
            if float(df_old.iloc[last_row_index]['high_price']) + delta < float(df_new.iloc[num]['high_price']):
                df_old = df_old.drop(last_row_index)
                num -= 1
                
            elif float(df_old.iloc[last_row_index]['high_price']) - delta < float(df_new.iloc[num]['high_price']) :
                num_kick = df_old.loc[last_row_index, 'num_kicks'] + 1
                df_old.loc[last_row_index, 'num_kicks'] = num_kick
                #print(df_high.loc[last_row_index, 'high_price'], df_high.loc[last_row_index, 'num_kicks'])
                
            else:
                if num >= size_high:
                    for i in range(size_high):
                        if float(df_new.iloc[num]['high_price']) < float(df_new.iloc[num - i]['high_price']):
                            not_high = True
                            break
                    if not not_high:
                        current_line = pd.Series({'open_time': df_new.iloc[num]['open_time'], 'high_price' : df_new.iloc[num]['high_price'], 'num_kicks' : 1})
                        df_old.loc[len(df_old.index)] = current_line
        num += 1
    
    df_old['high_price'] = df_old['high_price'].astype(float)
    #print(df_old)
    return df_old



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




def read_start_time(filename):
    """
    The function for reading the time of the next iteration
    """
    with open(filename, 'r') as file:
        constant = file.read().strip()
    return constant                
    

def write_start_time(filename, constant):
    """
    The function for writing the  time of the next iteration
    """
    with open(filename, 'w') as file:
        file.write(str(constant))    



"""
Highs / Lows of trand
"""
symbol = "BTCUSDT"    # coin's name of current iteration
interval = '30m'
name_csv = 'F_BTCUSDT_H.csv'    # name of file with current coin's highs
start_time_file = 'START_TIME.txt'
full_way = os.path.join(way_to_dir, name_csv)    
numb_iter = 0    # number of current df. only indicator

if interval == '30m':
    add_time_ms = 1800000000    # digit needed for is it the last df before the now time

    
current_datetime = datetime.now()
current_timestamp = int(current_datetime.timestamp()) * 1000    # now time


if not os.path.isfile(full_way):
    # if there isn't csv file with high/low -> creating empty df and write it in csv file
    df_old = pd.DataFrame(columns=['open_time', 'high_price', 'num_kicks'])
    df_old.to_csv(full_way, index=False)
    date_time = datetime(2023, 6, 1, 0, 0, 0)    # start time of getting highs/lows
    start_time = int(date_time.timestamp()) * 1000
else:
    # if there is the csv file in the dir, start time is reading from the file start_time_file
    start_time = int(read_start_time(os.path.join(way_to_dir, start_time_file))) 


while current_timestamp > start_time + add_time_ms:
    # while the start time of df + summary time of this df < current time -> doing iterations below
    numb_iter += 1    # only indicator increment
    print(numb_iter)    # only indication
    df_old = pd.read_csv(full_way)    # reading df with old highs
    stop_time = start_time + add_time_ms    
    df_futures_klines = asyncio.run(get_kline_data(api_key, symbol, interval, 1000, True, start_time, stop_time))    # getting new df with new klines (with new start and end interval)
    time.sleep(1)        
    df_futures_highs = asyncio.run(create_df_high(df_futures_klines, df_old))    # getting new highns and writing them into the csv file
    df_futures_highs.to_csv(full_way, index=False)
    start_time = stop_time    # getting the new start time for the next iteration

numb_iter += 1
print(numb_iter)  
start_time = start_time - 1800000    # changing start_time for the getting at least one kline in next df
  
df_old = pd.read_csv(full_way)
stop_time = current_timestamp    # in the last iteration stop_time is  now time 
df_futures_klines = asyncio.run(get_kline_data(api_key, symbol, interval, 1000, True, start_time, stop_time))
time.sleep(1)        
df_futures_highs = asyncio.run(create_df_high(df_futures_klines, df_old))
df_futures_highs.to_csv(full_way, index=False)
write_start_time(os.path.join(way_to_dir, start_time_file), stop_time)    # writting the time of the last iteration in txt file



"""
Increased density of requests
"""
pd.set_option('display.max_rows', None)    # settings for max displaying of DF 
pd.set_option('display.max_columns', None)

order_book = asyncio.run(get_order_book(api_key, symbol, 1000, True))













