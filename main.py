import asyncio
import aiohttp
import pandas as pd
from urllib.parse import urljoin, urlencode

from cred import api_key



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






symbol = "BTCUSDT"
interval = '15m'

data = asyncio.run(get_kline_data(api_key, symbol, interval))
#data_2 = asyncio.run(get_order_book(api_key, symbol))
print(data)
