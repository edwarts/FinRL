import os

import pandas as pd
from stockstats import StockDataFrame as Sdf
import numpy as np
import exchange_calendars as tc
import pandas_market_calendars as mcal
class LocalCustom():


    def __init__(self,local_price_root="/mnt/f/alpha_vantage_raw_csv/",local_indicator_root="/mnt/f/refinery_data/indicators_lake/stock/"):
        self.local_price_root = local_price_root
        self.local_indicator_root = local_indicator_root

    def generate_trading_times(self,start_date, end_date,with_extended=False):
        # Load NYSE calendar
        nyse = mcal.get_calendar('NYSE')

        # Get valid trading days within the specified range
        valid_days = nyse.valid_days(start_date=start_date, end_date=end_date)

        # Define trading hours
        extended_morning_start = "04:00"
        regular_start = "09:30"
        regular_end = "16:00"
        extended_evening_end = "20:00"

        # Initialize an empty list to store trading minutes
        trading_minutes = []

        for single_date in valid_days:
            # Morning extended hours
            morning_times = pd.date_range(
                start=pd.Timestamp(f"{single_date.date()} {extended_morning_start}"),
                end=pd.Timestamp(f"{single_date.date()} {regular_start}"),
                freq='T',  # 'T' for minute frequency
                closed='left'
            )

            # Regular trading hours
            regular_times = pd.date_range(
                start=pd.Timestamp(f"{single_date.date()} {regular_start}"),
                end=pd.Timestamp(f"{single_date.date()} {regular_end}"),
                freq='T',
                closed='left'
            )

            # Evening extended hours
            evening_times = pd.date_range(
                start=pd.Timestamp(f"{single_date.date()} {regular_end}"),
                end=pd.Timestamp(f"{single_date.date()} {extended_evening_end}"),
                freq='T',
                closed='left'
            )

            # Append all times for the day to the list
            if with_extended==True:
                trading_minutes.extend(morning_times)
                trading_minutes.extend(evening_times)
            trading_minutes.extend(regular_times)


        # Convert the list to a DataFrame
        trading_minutes_df = pd.DataFrame(trading_minutes, columns=['trading_times'])

        return trading_minutes_df


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
        date_interval="M",with_extend=False
    ):
        self.start=start_date
        self.end=end_date
        self.time_interval=time_interval
        self.date_interval=date_interval
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
        final_price_df['datetime'] = pd.to_datetime(final_price_df['datetime'])
        return final_price_df

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
                add_on_price=df[df.tic == tic][["close"]].values
                price_array = np.hstack(
                    [price_array, add_on_price]
                )
                add_on_tech=df[df.tic == tic][tech_indicator_list].values
                tech_array = np.hstack(
                    [tech_array, add_on_tech]
                )
                #        print("Successfully transformed into array")
        return price_array, tech_array, turbulence_array

    def add_technical_indicators_to_single_symbol(self,df, tech_indicator_list):
        unique_ticker = df.tic.unique()
        stock = Sdf.retype(df)
        print("Running Loop")
        # TODO issue here, not be able to get indicator all together, should do one by one
        # TODO tic issue only AAPL and NaN
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


    def add_technical_indicator(self,data, tech_indicator_list):
        df = data.copy()
        df = df.sort_values(by=["tic", "timestamp"])
        unique_ticker = df.tic.unique()
        stock = Sdf.retype(df)
        print("Running Loop")
        # TODO issue here, not be able to get indicator all together, should do one by one
        # TODO tic issue only AAPL and NaN
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
        # TODO need to add vixy as vix here
        return df
    def add_vixor(self, df) -> pd.DataFrame:
        return df



if __name__ == '__main__':
    localcustom=LocalCustom()
    # NO data for FB
    # TLSA only 3 months
    ticker_list=['AAPL',"NVDA","AMZN","GOOG"]
    start_date="2024-01-01"
    end_date="2024-05-31"
    #
    # Usage example:
    start_date = '2024-01-01'
    end_date = '2024-05-31'
    trading_times = localcustom.generate_trading_times(start_date, end_date)
    print(trading_times)
    price_df=localcustom.download_data(ticker_list,start_date,end_date)
    # final_price=localcustom.add_turbulence(p_with_indicator)
    price_df.set_index('datetime', inplace=True)
    # print(final_price)
    # print(price_df)
    final_price_df = pd.DataFrame()
    tech_list = [
        "macd",
        "boll_ub",
        "boll_lb",
        "rsi_30",
        "dx_30",
        "close_30_sma",
        "close_60_sma"]
    # TODO put merge work trading times key outside and remerge them
    # TODO new idea, to use prompt with ohlc and indicator to finetune LLam3
    p_with_indicator = pd.DataFrame()
    for each_ticker in ticker_list:
        each_price_df=price_df[price_df["tic"]==each_ticker]

        trading_times.index = pd.to_datetime(trading_times['trading_times'])
        result_df = trading_times.merge(each_price_df, how='left', left_index=True, right_index=True)

        result_df.fillna(method='ffill', inplace=True)
        final_price_df = pd.concat([final_price_df,result_df])
        # p_with_indicator_t = localcustom.add_technical_indicator(final_price_df, tech_list)
        # p_with_indicator=pd.concat([p_with_indicator,p_with_indicator_t])
        # print(final_price_df.isna().sum())
        # Print the result to check
    # indicators=Sdf.retype(final_price_df)
    # feeding with only normal working hour
    #

    # for indicator in tech_list:
    #     single_indicator=indicators[indicator]
    #     print(single_indicator)
    # p_with_indicator["VIXY"] = 0
    p_with_in=localcustom.add_technical_indicator(final_price_df,tech_list)
    p_with_in["VIXY"] = 0
    price_df,tech_df,turb_df=localcustom.df_to_array(p_with_in,tech_list,True)
    print(price_df)
    # turb_df=vixy_df["close"].values