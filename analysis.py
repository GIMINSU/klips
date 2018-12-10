import csv
from cnx_information import BaseManager
from pymysql.cursors import DictCursor
from datetime import datetime, date
import pandas as pd
import numpy as np
import pprint
import codecs
import seaborn as sns; sns.set()
import matplotlib.pyplot as plt
import scipy.stats as stats
from statsmodels.formula.api import ols
import statsmodels.api as sm
from statsmodels.stats.multicomp import pairwise_tukeyhsd

class Worker(BaseManager):
    def analysis_data(self):
        # df = pd.read_csv('C:/Users/is/Desktop/dividend_rawdata2.csv', encoding='utf-8')

        cnx_sql = Connect.cnx()
        cur = cnx_sql.cursor()
        sql_query = '''
        select * from dividend_rawdata2'''

        cur.execute(sql_query)
        rows = cur.fetchall()

        print(rows)
        # for row in rows:
        #     print(row['name'])



        # variable_list = df.columns.values
        # print(variable_list)
        # # remove_list = ['name', 'code', 'month', 'sector']
        # # for var in variable_list:
        # #     if var in remove_list:
        # #         variable_list.remove(var)
        # #
        # # print(variable_list)
        #
        #
        # # self.target_var = df[]
        # print(df.columns.values)
        # # variable_list = df.columns
        # # print(df)
        # df = df.where((pd.notnull(df)), None)
        # df['name'] = df.name.astype(str)
        # # df['code'] = df.code.asytpe(str)
        # df['year'] = pd.to_datetime(df['year'].astype(str), format='%Y')
        # # df['year'] =df['year'].astype(int)
        # df['dividend_rate'] = df.dividend_rate.astype(float)
        # df['par_value'] = df.par_value.astype(float)
        # df['ending_shares'] = df.ending_shares.astype(float)
        # df['dividend_per_share'] = df.dividend_per_share.astype(float)
        # df['dividend_payoff_ratio'] = df.dividend_payoff_ratio.astype(float)
        # df['total_dividend_amount'] = df.total_dividend_amount.astype(float)
        # df['market_dividend_rate'] = df.market_dividend_rate.astype(float)
        # # print(df.ending_shares)
        #
        # print(df.dtypes)
        # # print(df)
        #
        #
        # # print(df.describe(include='all'))
        # df = df.drop(['code', 'month'], axis=1)
        # sns.heatmap(df.corr())
        #
        # # print(df)
        # # df = df.groupby(['year'])
        # # print(df)
        # print(df.groupby(['year']).count()['name'])
        #
        # # anova test
        # X = df['year']
        # y = df['dividend_payoff_ratio']
        #
        # anova_model = ols('dividend_payoff_ratio ~ C(year)', data=df).fit()
        # print(anova_model.summary())
        # anova_result = sm.stats.anova_lm(anova_model, type=2)
        # print(anova_result)
        # tukey = pairwise_tukeyhsd(endog=df['dividend_payoff_ratio'], groups=df['year'], alpha=0.5)
        # print(tukey.summary())
        # # print(groups.size())
        # # df = df.set_index(['name', 'year'])
        # # print(df)
        #
        # # print(df)
        #
        # # print(df)
        # # # ftest
        # # df_2015 = df[df['year'] == '2015-01-01']
        # # df_2015 = df_2015.where((pd.notnull(df)), None)
        # # print(df_2015.dtypes)
        # # df_2015['dividend_payoff_ratio'] = df_2015.dividend_payoff_ratio.astype(float)
        # #
        # # df_2016 = df[df['year'] == '2016-01-01']
        # # df_2016 = df_2016.where((pd.notnull(df)), None)
        # # df_2016['dividend_payoff_ratio'] = df_2016.dividend_payoff_ratio.astype(float)
        # # df_2017 = df[df['year'] == '2016-01-01']
        # # print(df_2015['dividend_payoff_ratio'])
        # # print(df_2016['dividend_payoff_ratio'])
        #
        # fest_2015_2016 = stats.f_oneway(df[df['year'] == '2015-01-01']['dividend_payoff_ratio'], df[df['year'] == '2016-01-01']['dividend_payoff_ratio'])
        # print(fest_2015_2016)
        # # print(df_2015)
        #
        # # plt.show()
Worker().analysis_data()