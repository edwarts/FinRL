import os

import pandas as pd
from stockstats import StockDataFrame as Sdf
import numpy as np
import exchange_calendars as tc
class LocalCustom():


    def __init__(self,local_price_root="/mnt/f/alpha_vantage_raw_csv/",local_indicator_root="/mnt/f/refinery_data/indicators_lake/stock/"):
        self.local_price_root = local_price_root
        self.local_indicator_root = local_indicator_root


    def get_trading_days(self, start, end):
        nyse = tc.get_calendar("NYSE")
        df = nyse.sessions_in_range(
            pd.Timestamp(start).tz_localize(None), pd.Timestamp(end).tz_localize(None)
        )
        trading_days = []
        for day in df:
            trading_days.append(str(day)[:10])

        return trading_days
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
        df = df.copy()
        unique_ticker = df.tic.unique()
        if_first_time = True
        for tic in unique_ticker:
            if if_first_time:
                price_array = df[df.tic == tic][["close"]].values
                tech_array = df[df.tic == tic][tech_indicator_list].values
                if if_vix:
                    turbulence_array = df[df.tic == tic]["VIXY"].values
                else:
                    turbulence_array = df[df.tic == tic]["turbulence"].values
                if_first_time = False
            else:
                price_array = np.hstack(
                    [price_array, df[df.tic == tic][["close"]].values]
                )
                tech_array = np.hstack(
                    [tech_array, df[df.tic == tic][tech_indicator_list].values]
                )
                #        print("Successfully transformed into array")
        return price_array, tech_array, turbulence_array

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

    def calculate_turbulence(self, data, time_period=252):
        # can add other market assets
        df = data.copy()
        df_price_pivot = df.pivot(index="timestamp", columns="tic", values="close")
        # use returns to calculate turbulence
        df_price_pivot = df_price_pivot.pct_change()

        unique_date = df.timestamp.unique()
        # start after a fixed timestamp period
        start = time_period
        turbulence_index = [0] * start
        # turbulence_index = [0]
        count = 0
        for i in range(start, len(unique_date)):
            current_price = df_price_pivot[df_price_pivot.index == unique_date[i]]
            # use one year rolling window to calcualte covariance
            hist_price = df_price_pivot[
                (df_price_pivot.index < unique_date[i])
                & (df_price_pivot.index >= unique_date[i - time_period])
                ]
            # Drop tickers which has number missing values more than the "oldest" ticker
            filtered_hist_price = hist_price.iloc[
                                  hist_price.isna().sum().min():
                                  ].dropna(axis=1)

            cov_temp = filtered_hist_price.cov()
            current_temp = current_price[[x for x in filtered_hist_price]] - np.mean(
                filtered_hist_price, axis=0
            )
            temp = current_temp.values.dot(np.linalg.pinv(cov_temp)).dot(
                current_temp.values.T
            )
            if temp > 0:
                count += 1
                if count > 2:
                    turbulence_temp = temp[0][0]
                else:
                    # avoid large outlier because of the calculation just begins
                    turbulence_temp = 0
            else:
                turbulence_temp = 0
            turbulence_index.append(turbulence_temp)

        turbulence_index = pd.DataFrame(
            {"timestamp": df_price_pivot.index, "turbulence": turbulence_index}
        )

        # print("turbulence_index\n", turbulence_index)

        return turbulence_index

    def add_turbulence(self, data, time_period=252):
        """
        add turbulence index from a precalcualted dataframe
        :param data: (df) pandas dataframe
        :return: (df) pandas dataframe
        """
        df = data.copy()
        turbulence_index = self.calculate_turbulence(df, time_period=time_period)
        df = df.merge(turbulence_index, on="timestamp")
        df = df.sort_values(["timestamp", "tic"]).reset_index(drop=True)
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
    price_df["VIXY"] = 0
    vixy_df=localcustom.download_data(["VIXY"],start_date,end_date)
    tech_list=[
            "macd",
            "boll_ub",
            "boll_lb",
            "rsi_30",
            "dx_30",
            "close_30_sma",
            "close_60_sma"]
    p_with_indicator=localcustom.add_technical_indicator(price_df,tech_list)
    # print(p_with_indicator)
    # final_price=localcustom.add_turbulence(p_with_indicator)

    # print(final_price)
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
    price_df,tech_df,turb_df=localcustom.df_to_array(p_with_indicator,tech_list,True)
    turb_df=vixy_df["close"].values