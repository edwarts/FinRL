import pandas as pd
class LocalCustom():
    def __init__(self):
        pass

    def download_data(self,
        ticker_list,
        start_date,
        end_date,
        time_interval,
    ):
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