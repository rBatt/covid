import pandas as pd
from covid.data.data import DailyHistCT

class Xform:
    def __init__(self, place_col='state', time_col='date', con=None):
        self.place_col = place_col
        self.time_col = time_col
        self.key_cols = [place_col, time_col]
        self.con = con or self._get_con()

    @staticmethod
    def _get_con():
        con = DailyHistCT.build_con(**DailyHistCT.con_args)
        return con

    def _get_dhct_df(self):
        dhct = DailyHistCT(spark=None, con=self.con)
        df = dhct.read_psql().sort_values(self.key_cols)
        return df

    def _get_diff(self, df, col='positive'):
        df_diff = df\
            .set_index(self.key_cols)\
            .loc[:,[col]]\
            .groupby([self.place_col]).diff()\
            .reset_index()
        return df_diff.rename({col:col+"_diff"}, axis=1)

    def get_xfdata(self):
        df = self._get_dhct_df()
        pos_diff_df = self._get_diff(df=df, col='positive')\
            .set_index(self.key_cols)
        df = df\
            .set_index(self.key_cols)\
            .join(pos_diff_df, on=self.key_cols)\
            .reset_index()
        df.loc[:,"positive_diff_norm"] = df.loc[:,'positive_diff'] / df.loc[:,"totalTestResultsIncrease"]
        df.loc[:, self.time_col] = pd.to_datetime(df.loc[:,self.time_col])
        return df