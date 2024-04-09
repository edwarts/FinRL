import os

import pandas as pd
class LocalCustom():
    def __init__(self,local_price_root="/mnt/f/alpha_vantage_raw_csv/1m/",local_indicator_root="/mnt/f/refinery_data/indicators_lake/stock/1m/"):
        self.local_price_root = local_price_root
        self.local_indicator_root = local_indicator_root
    # current only support 1m or 5m
    def get_year_month_strings_pandas(self,start_date, end_date, freq='M'):
        if freq == 'M':
            dates = pd.date_range(start_date, end_date, freq='M')
            return [date.strftime("%Y-%m") for date in dates]
        if freq == 'D':
            dates = pd.date_range(start_date, end_date, freq='D')
            return [date.strftime("%Y-%m-%d") for date in dates]
    def download_data(self,
        ticker_list,
        start_date,
        end_date,
        time_interval="1m",
        date_interval="M"
    ):
        dates=self.get_year_month_strings_pandas(start_date,end_date,date_interval)
        final_price_df=pd.DataFrame()
        # get date from the start and end

        for ticker in ticker_list:

            for each_date in dates:
                single_file_path=os.path.join(self.local_price_root,time_interval,ticker,f"rund"each_date)
                print(f"Load from {ticker} at path {single_file_path}")
                df=pd.read_csv(self.local_price_root)
        pass
    def add_technical_indicator(self,df, tech_indicator_list):
        pass

    def clean_data(self, df):
        return df

    def df_to_array(self,
        df, tech_indicator_list, if_vix
    ):
        pass

    def add_technical_indicator(self,df, tech_indicator_list):
        pass
    def add_turbulence(self,df):
        return df
    def add_vix(self,df):
        return df

    def add_vixor(self, df) -> pd.DataFrame:
        return df