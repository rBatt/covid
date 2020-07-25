import os
import requests
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql.types import *



class BaseData:
    """
    Base Data Class

    bd = BaseData(spark=spark, tbl_nm='test_result')
    out = bd.read_psql()
    out.show()
    """
    db = 'covid'
    url = f"jdbc:postgresql://localhost:5432/{db}"
    properties = {"user":"postgres", "password":"postgres", "driver":"org.postgresql.Driver"}

    def __init__(self, spark, tbl_nm=None):
        self.spark = spark
        self.tbl_nm = tbl_nm

    def read_psql(self, tbl=None):
        tbl = tbl or self.tbl_nm
        return self.spark.read.jdbc(url=self.url, table=tbl, properties=self.properties)

    def check_psql_tbl(self, tbl=None):
        tbl = tbl or self.tbl_nm
        df = self.spark.read.jdbc(
                url=self.url
                , table='information_schema.tables'
                , properties=self.properties
            )\
            .select('table_catalog', 'table_schema', 'table_name')\
            .filter(f"table_name = '{tbl}'")
        return df

    def write_psql(self, df, tbl=None, mode='overwrite'):
        tbl = tbl or self.tbl_nm
        df.write.jdbc(
            url=self.url
            , table=tbl
            , mode=mode
            , properties=self.properties
        )

    @staticmethod
    def get_uni_combos(df, key_cols):
        uni_key = df\
            .select(key_cols)\
            .distinct()\
            .orderBy(key_cols)\
            .select(F.concat(*key_cols)).alias('pk')\
            .collect()
        return [x[0] for x in uni_key]

    def are_keys_diff(self, df1, df2, key_cols, return_diff=False):
        key_set1 = set(self.get_uni_combos(df1, key_cols))
        key_set2 = set(self.get_uni_combos(df2, key_cols))
        if return_diff:
            return key_set1 - key_set2
        return bool(key_set1 ^ key_set2)



class DailyHistCT(BaseData):
    call = "https://covidtracking.com"+"/api/v1/states/daily.csv"
    useCols = [
        'date'
        , 'state'

        , 'totalTestResults' # Total Test Results Provided by the State
        , 'totalTestsViral' # Total number of PCR tests performed.
        , 'pending' # Number of tests whose results have yet to be determined.
        , 'positive'
        , 'negative' # Total number of people who have tested negative for COVID-19 so far.
        , 'hospitalizedCurrently' # number of people in hospital for covid today
        , 'recovered' # Total number of people who have recovered from COVID-19 so far.
        , 'death' # total number of people who have died from covid so far

        , 'deathIncrease' # daily difference in death
        , 'hospitalizedCumulative' # total number of people who have gone to hospital for covid so far
        , 'hospitalizedIncrease' # daily difference in hospitalized
        , 'inIcuCumulative'
        , 'inIcuCurrently'
        , 'negativeTestsViral' # Total number of negative PCR tests.
        , 'onVentilatorCumulative'
        , 'onVentilatorCurrently'
        , 'positiveCasesViral' # Total number of positive cases measured with PCR tests.
        , 'positiveTestsViral' # Total number of positive PCR tests.
        , 'totalTestResultsIncrease' # Daily Difference in totalTestResults
    ]
    table = 'daily_hist_ct'
    table_api = table+'_api'

    def __init__(self, spark):
        super().__init__(spark=spark, tbl_nm=self.table)

        self.df_new = None
        self.df_old = None
        self.df_needs_updating = None

        self._api_status_new = None
        self._api_status_old = None
        self._api_needs_updating = None

    def check_api(self):
        call_status = "https://covidtracking.com"+"/api/v1/status.csv"
        response = requests.get(call_status)
        rdd_ctp = self.spark.sparkContext.parallelize(response.text.split('\n'))
        df_api_status = self.spark.read.csv(rdd_ctp, header=True, sep=",", inferSchema=True)
        return df_api_status

    @property
    def api_status_new(self):
        self._api_status_new = self.check_api()
        return self._api_status_new

    @property
    def api_status_old(self):
        api_old_exists = bool(self.check_psql_tbl(tbl=self.table_api).count())
        if api_old_exists:
            self._api_status_old = self.read_psql(tbl=self.table_api)
        return self._api_status_old

    @property
    def api_needs_updating(self):
        if not self.api_status_old:
            return True
        new_row = self.api_status_new.collect()[0]
        old_rows = self.api_status_old.collect()
        if new_row not in old_rows:
            return True
        return False

    def update_api(self, mode='append'):
        if not self.api_status_old:
            mode = 'overwrite'
        self.write_psql(df=self.api_status_new, tbl=self.table_api, mode=mode)

    def get_df_new(self):
        response = requests.get(self.call)
        rdd_ctp = self.spark.sparkContext.parallelize(response.text.split('\n'))
        df_ctp = self.spark.read.csv(rdd_ctp, header=True, sep=",", inferSchema=True).select(self.useCols)
        df_ctp = df_ctp.withColumn('date', F.to_date(df_ctp.date.cast('string'), "yyyyMMdd"))
        self.df_new = df_ctp

    def get_df_old(self):
        df_old_exists = bool(self.check_psql_tbl().count())
        if df_old_exists:
            self.df_old = self.read_psql()
        else:
            self.df_old = None

    def check_df(self):
        if self.df_old:
            self.df_needs_updating = self.are_keys_diff(
                self.df_new
                , self.df_old
                , ['date','state']
            )
        else:
            self.df_needs_updating = True

    def update_table(self, mode='overwrite'):
        self.write_psql(df=self.df_new, tbl=self.tbl_nm, mode=mode)



if __name__=='__main__':
    sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.14 pyspark-shell'
    spark = SparkSession\
        .builder\
        .appName('covid_data')\
        .config('spark.driver.host', 'localhost')\
        .config('spark.cores.max', '20')\
        .getOrCreate()