import asyncio
import aiohttp
import pandas as pd
from urllib.parse import urljoin, urlencode

from cred import api_key

size_high = size_low = 3



async def get_kline_data(api_key, symbol, interval, limit, futures=False, start_time=None, end_time=None):
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
        'limit': limit
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
    :return: (float, float) sum quantities of bid and ask (for number of orders);
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

    df_bids = pd.DataFrame(depth['bids'], columns=['high_price', 'quantity_bid'])
    df_bids = df_bids.astype(float)
    anomal_bids = df_bids[df_bids['quantity_bid'] > anomal_quantity]
    #print(anomal_bids)
    quantity_bid = df_bids['quantity_bid'].sum()
    
    df_asks = pd.DataFrame(depth['asks'], columns=['high_price', 'quantity_ask'])
    df_asks = df_asks.astype(float)
    anomal_asks = df_asks[df_asks['quantity_ask'] > anomal_quantity]
    #print(anomal_asks)
    quantity_ask = df_asks['quantity_ask'].sum()
    print('\\\\\\\\\\\\\\\\\\\\\\\\\\\\')
    print(anomal_asks)


    return df_asks



async def create_df_high(df):
    """
    :param df: (pandas Data Frame) CandleStick Data;
    :return df_high: (pandas Data Frame) CandleStick Highs Data;

    """

    global size_high
    last_string = 0
    df_high = pd.DataFrame(columns=['open_time', 'high_price', 'num_kicks'])
    num = 0

    while num < df.shape[0]:        
        not_high = False        
        if df_high.shape[0] == 0:
            current_line = pd.Series({'open_time': df.iloc[num]['open_time'], 'high_price' : df.iloc[num]['high_price'], 'num_kicks' : 1})
            df_high.loc[len(df_high.index)] = current_line

        else:
            last_row_index = df_high.shape[0] - 1           
            if df_high.iloc[last_row_index]['high_price'] < df.iloc[num]['high_price']:
                df_high = df_high.drop(last_row_index)
                num -= 1
                
            elif df_high.iloc[last_row_index]['high_price'] == df.iloc[num]['high_price']:
                num_kick = df_high.loc[last_row_index, 'num_kicks'] + 1
                df_high.loc[last_row_index, 'num_kicks'] = num_kick
                print(df_high.loc[last_row_index, 'high_price'], df_high.loc[last_row_index, 'num_kicks'])
                
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
    return df_high




                
    
    




symbol = "BTCUSDT"
interval = '15m'

data = asyncio.run(get_kline_data(api_key, symbol, interval, 500, True))
data_2 = asyncio.run(get_order_book(api_key, symbol, 1000, True))
print(type(data_2))
data_3 = asyncio.run(create_df_high(data))
print(type(data_3))

merged_df = pd.merge(data_3, data_2, on='high_price')
print('\\\\\\\\\\\\\\\\\\\\\\\\\\')
print(data_2)
print('\\\\\\\\\\\\\\\\\\\\\\\\\\')
print(data_3)
print('\\\\\\\\\\\\\\\\\\\\\\\\\\')
print(merged_df)






