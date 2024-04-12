import os

import pandas as pd
from stockstats import StockDataFrame as Sdf
class LocalCustom():
    def __init__(self,local_price_root="/mnt/f/alpha_vantage_raw_csv/",local_indicator_root="/mnt/f/refinery_data/indicators_lake/stock/"):
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
                single_file_path=os.path.join(self.local_price_root,time_interval,f"symbol={ticker}",f"run_date={each_date}","data.csv")
                print(f"Load from {ticker} at path {single_file_path}")
                single_df=pd.read_csv(single_file_path)
                single_df['tic']=ticker
                final_price_df=pd.concat([final_price_df,single_df])
        final_price_df['timestamp']=pd.to_datetime(final_price_df['datetime'])
        return final_price_df
    def add_technical_indicator(self,df, tech_indicator_list=[
            "macd",
            "boll_ub",
            "boll_lb",
            "rsi_30",
            "dx_30",
            "close_30_sma",
            "close_60_sma",
        ]):
        pass

    def clean_data(self, df):
        return df

    def df_to_array(self,
        df, tech_indicator_list, if_vix
    ):
        pass

    def add_technical_indicator(self,df, tech_indicator_list):
        unique_ticker = df.tic.unique()
        stock = Sdf.retype(df)
        print("Running Loop")
        for indicator in tech_indicator_list:
            indicator_dfs = []
            for tic in unique_ticker:
                tic_data = stock[stock.tic == tic]
                indicator_series = tic_data[indicator]

                tic_timestamps = stock.loc[stock.tic == tic, "timestamp"]

                indicator_df = pd.DataFrame(
                    {
                        "tic": tic,
                        "date": tic_timestamps.values,
                        indicator: indicator_series.values,
                    }
                )
                indicator_dfs.append(indicator_df)

            # Concatenate all intermediate dataframes at once
            indicator_df = pd.concat(indicator_dfs, ignore_index=True)

            # Merge the indicator data frame
            df = df.merge(
                indicator_df[["tic", "date", indicator]],
                left_on=["tic", "timestamp"],
                right_on=["tic", "date"],
                how="left",
            ).drop(columns="date")
        print("Finished adding Indicators")
        return df
    def add_turbulence(self,df):
        return df
    def add_vix(self,df):
        return df
    def add_vixor(self, df) -> pd.DataFrame:
        return df

if __name__ == '__main__':
    localcustom=LocalCustom()
    ticker_list=['AAPL',"NVDA","AMZN","GOOG","FB"]
    start_date="2020-01-01"
    end_date="2024-03-31"
    price_df=localcustom.download_data(ticker_list,start_date,end_date)
    p_with_indicator=localcustom.add_technical_indicator(price_df,[
            "macd",
            "boll_ub",
            "boll_lb",
            "rsi_30",
            "dx_30",
            "close_30_sma",
            "close_60_sma"])
    print(p_with_indicator)
    # print(price_df)
    # price_df=price_df[price_df["tic"]=="AAPL"]
    # indicators=Sdf.retype(price_df)

    # for indicator in [
    #         "macd",
    #         "boll_ub",
    #         "boll_lb",
    #         "rsi_30",
    #         "dx_30",
    #         "close_30_sma",
    #         "close_60_sma"]:
    #     single_indicator=indicators[indicator]
    #     print(single_indicator)