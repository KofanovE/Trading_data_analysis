import asyncio
import aiohttp
import pandas as pd
from urllib.parse import urljoin, urlencode

from cred import api_key

size_high = size_low = 3



async def get_kline_data(api_key, symbol, interval, limit=500, start_time=None, end_time=None):
    """
    :param api_key: (str) api key of binance account;
    :param symbol: (str) trading pair. ex: 'ETHUSDT';
    :param interval: (str) interval of candles. ex: 1m, 5m, 30m, 1h, 1d;
    :param limit: (int) number of candles (default 500, max 1000);
    :param start_time: (long) time in ms;
    :param end_time: (long) time in ms;
    :return: (pandas Data Frame) CandleStick Data
    """
    api_key = api_key
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




async def get_order_book(api_key, symbol, limit=5000, start_time=None, end_time=None):

    """
    :param api_key: (str) api key of binance account;
    :param symbol: (str) trading pair. ex: 'ETHUSDT';
    :param limit: (int) number of orders (max 5000);
    :param start_time: (long) time in ms;
    :param end_time: (long) time in ms;
    :return: (float, float) sum quantities of bid and ask (for number of orders)
    """

    BASE_URL = 'https://api.binance.com'
    PATH = '/api/v3/depth'
    params = {
        'symbol': symbol,
        'limit': limit
    }
    headers = {'X-MBX-APIKEY': api_key}
    url = urljoin(BASE_URL, PATH)
    anomal_quantity = 5

    async with aiohttp.ClientSession() as session:

        async with session.get(url, headers=headers, params=params) as response:
            depth = await response.json()

    df_bids = pd.DataFrame(depth['bids'], columns=['Price_bid', 'Quantity_bid'])
    df_bids = df_bids.astype(float)
    #median_bids = df_bids['Quantity_bid'].median()
    anomal_bids = df_bids[df_bids['Quantity_bid'] > anomal_quantity]
    print(anomal_bids)
    quantity_bid = df_bids['Quantity_bid'].sum()
    
    df_asks = pd.DataFrame(depth['asks'], columns=['Price_ask', 'Quantity_ask'])
    df_asks = df_asks.astype(float)
    anomal_asks = df_asks[df_asks['Quantity_ask'] > anomal_quantity]
    print(anomal_asks)
    quantity_ask = df_asks['Quantity_ask'].sum()

    return quantity_bid, quantity_ask



async def create_df_high(df):
    """
    """

    global size_high
    last_string = 0
    df_high = pd.DataFrame(columns=['open_time', 'high_price', 'num_kicks', 'quantity_bid'])
    num = 0

    while num < df.shape[0]:
        print(df.iloc[num])


        if df_high.shape[0] == 0:
            print(1)
            current_line = pd.Series({'open_time': df.iloc[num]['open_time'], 'high_price' : df.iloc[num]['high_price'], 'num_kicks' : 1, 'quantity_bid' : 0})
            df_high.loc[len(df_high.index)] = current_line
            print(111)
            print(df_high)
        else:
            print(2)
            last_row_index = df_high.shape[0] - 1
            if df_high.iloc[last_row_index]['high_price'] < df.iloc[num]['high_price']:
                print(3)
                print(last_row_index)
                df_high = df_high.drop(last_row_index)
                print(333)
                print(df_high)
                num -= 1
            elif df_high.iloc[last_row_index]['high_price'] == df.iloc[num]['high_price']:
                print(4)
                df_high.iloc[last_row_index]['num_kicks'] += 1
            else:
                print(5)
                if num >= size_high:
                    print(6)
                    for i in range(size_high):
                        print(df.iloc[num]['high_price'], type(df.iloc[num]['high_price']))
                        print(df.iloc[num - i])
                        if float(df.iloc[num]['high_price']) < float(df.iloc[num - i]['high_price']):
                            print(7)
                            break
                    print(8)
                    current_line = pd.Series({'open_time': df.iloc[num]['open_time'], 'high_price' : df.iloc[num]['high_price'], 'num_kicks' : 1, 'quantity_bid' : 0})
                    df_high.loc[len(df_high.index)] = current_line
                    print(888)
                    print(df_high)
        num += 1



    return df_high




                
    
    




symbol = "BTCUSDT"
interval = '15m'

data = asyncio.run(get_kline_data(api_key, symbol, interval))
#data_2 = asyncio.run(get_order_book(api_key, symbol))
data_3 = asyncio.run(create_df_high(data))
print(data)
print(data_3)
