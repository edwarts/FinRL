import os

import pandas as pd
from stockstats import StockDataFrame as Sdf
import numpy as np
import exchange_calendars as tc

from finrl.meta.data_processors.processor_localcustom import LocalCustom

if __name__ == '__main__':
    localcustom=LocalCustom()
    ticker_list=['AAPL',"NVDA","AMZN","GOOG","FB"]
    start_date="2020-01-01"
    end_date="2024-03-31"
    price_df=localcustom.download_data(ticker_list,start_date,end_date)
    pr_dt=[]
    for each_ticker in ticker_list:
        print(each_ticker)
        dt=price_df[price_df.tic==each_ticker]['datetime']
        pr_dt=dt
        print(len(dt))