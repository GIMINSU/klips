import pymysql
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

from config import config

class Worker(BaseManager):
    def __init__(self):
        self.dbname=config['db']["first_server"]
        self.user=config['db']['first_server']["user"]
        self.password=config['db']['first_server']['password']
        self.host=config['db']['first_server']['host']
        self.database=config['db']['first_server']['schema']

    def analysis_data(self):

        # db 연결
        cnx_sql = pymysql.connect(user=self.user,
                              password=self.password,
                              host=self.host,
                              database=self.database,
                              charset='utf8mb4',
                              cursorclass=DictCursor,
                              autocommit=False)
        cur = cnx_sql.cursor(DictCursor)

        # db query 실행
        sql_query = '''
        select * from rawdata_20181215'''

        cur.execute(sql_query)
        rows = cur.fetchall()

        # 예외 업종 목록
        except_list = ['유통업', '금융업', '보험', '증권', '은행']

        # 상호출자제한 기업집단 목록
        cross_shareholding_limited_group_dict = dict()
        cross_shareholding_limited_group_dict['2017'] = [
                                                            '삼성', '현대자동차', '에스케이', '엘지', '롯데', '포스코',
                                                            '지에스', '한화', '현대중공업', '농협', '신세계', '케이티',
                                                            '두산', '한진', '씨제이', '부영', '엘에스', '대림',
                                                            '금호아시아나', '대우조선해양', '미래에셋', '에쓰-오일',
                                                            '현대백화점', '오씨아이', '효성', '영풍', '케이티앤지',
                                                            '한국투자금융', '대우건설', '하림', '케이씨씨'
                                                        ]
        # 분석 자료 구성
        analysis_dict = dict()
        threeyears_company_dict = dict()
        threeyears_company_list = []
        for row in rows:
            name = row['\ufeffname']
            code = row['code']
            year = int(row['year'])
            month = int(row['month'])
            sector = row['sector']
            dividend_rate_by_sector = float(row['dividend_rate_by_sector'])
            par_value = int(row['par_value'])
            ending_shares = int(row['ending_shares'])

            # 인코딩 문제 해결 못해서 발생한 문제 임시 해결
            if row['total_dividend_amount'] == '':
                row['total_dividend_amount'] = 0
            total_dividend_amount = int(row['total_dividend_amount'])
            if row['dividend_per_share'] == '':
                row['dividend_per_share'] = 0
            dividend_per_share = float(row['dividend_per_share'])
            if row['dividend_payoff_ratio'] == '':
                row['dividend_payoff_ratio'] = 0
            dividend_payoff_ratio = float(row['dividend_payoff_ratio'])
            if row['market_dividend_rate'] == '':
                row['market_dividend_rate'] = 0
            market_dividend_rate = float(row['market_dividend_rate'])

            # 3년치 자료가 모두 있는 기업만 분석하겠다.
            if year not in threeyears_company_dict:
                threeyears_company_dict[year] = total_dividend_amount
            if code not in threeyears_company_dict:
                threeyears_company_dict[code] = []
            if code in threeyears_company_dict:
                threeyears_company_dict[code].append(year)
            if len(threeyears_company_dict[code]) == 3:
                threeyears_company_list.append(code)

            if month == 12 and len(str(sector)) != 0 and sector not in except_list and len(str(total_dividend_amount)) != 0:
                if code in threeyears_company_list:
                    if code not in analysis_dict:
                        analysis_dict[code] = dict()
                    if code in analysis_dict:
                        analysis_dict[code] = dict(
                            year=year,
                            dividend_rate_by_sector=dividend_rate_by_sector,
                            par_value=par_value,
                            ending_shares=ending_shares,
                            total_dividend_amount=total_dividend_amount,
                            dividend_per_share=dividend_per_share,
                            dividend_payoff_ratio=dividend_payoff_ratio,
                            market_dividend_rate=market_dividend_rate,
                            target2016=0,
                            target2017=0,
                            total_2015=0,
                            total_2016=0,
                            total_2017=0,
                        )

        # target 변수 생성
        for code in analysis_dict:
            if analysis_dict[code]['year'] == 2015:
                analysis_dict[code]['total_2015'] = analysis_dict[code]['dividend_payoff_ratio']
            if analysis_dict[code]['year'] == 2016:
                analysis_dict[code]['total_2016'] = analysis_dict[code]['dividend_payoff_ratio']
            if analysis_dict[code]['year'] == 2017:
                analysis_dict[code]['total_2017'] = analysis_dict[code]['dividend_payoff_ratio']
            if analysis_dict[code]['total_2017'] > analysis_dict[code]['total_2015']:
                analysis_dict[code]['target2017'] = 1
            if analysis_dict[code]['total_2015'] is not None:
                if analysis_dict[code]['total_2016'] > analysis_dict[code]['total_2015']:
                    analysis_dict[code]['target2016'] = 1

        print('ANALYSIS COMPANY COUNT:', len(analysis_dict.keys()))
        # pprint.pprint(analysis_dict)

        # data 분석 실행
        df = pd.DataFrame(analysis_dict)
        df = df.T.reset_index(drop=True)
        # print('ANALYSIS DATA DESCRIBE: \n', df.describe())

        # correlation
        sns.heatmap(df.corr(), square=True, annot=True)

        seg1516 = df[df['year'] == 2015][['year', 'target2016']]
        seg1616 = df[df['year'] == 2016][['year', 'target2016']]
        seg1517 = df[df['year'] == 2016][['year', 'target2017']]
        seg1717 = df[df['year'] == 2017][['year', 'target2017']]

        df16 = df[df['year'] != 2017][['target2016', 'year', 'total_dividend_amount', 'dividend_payoff_ratio', 'dividend_rate_by_sector', 'ending_shares', 'market_dividend_rate']]
        df17 = df[df['year'] != 2016][
            ['target2017', 'year', 'total_dividend_amount', 'dividend_payoff_ratio', 'dividend_rate_by_sector',
             'ending_shares', 'market_dividend_rate']]

        # anova test
        ftest_year15_year16 = stats.f_oneway(seg1516['target2016'], seg1616['target2016'])
        ftest_year15_year17 = stats.f_oneway(seg1517['target2017'], seg1717['target2017'])
        print('DIVIDEND PAYOFF RATIO F-TSET(2015-2016) RESULT: ', ftest_year15_year16)
        print('DIVIDEND PAYOFF RATIO F-TSET(2015-2015) RESULT: ', ftest_year15_year17)

        # post hoc test
        tukey = pairwise_tukeyhsd(endog=df['target2016'], groups=df['year'], alpha=0.05)
        print('RESULT TUKEY-TEST', tukey.summary())

        # regression
        total_dividend_amount_model = ols('dividend_payoff_ratio ~ C(year) + total_dividend_amount + dividend_rate_by_sector + ending_shares + market_dividend_rate', data=df).fit()
        print('RESULT REGRESSION DIVIDEND_PAYOFF_RATIO: ', total_dividend_amount_model.summary())

        reg_model16 = ols(
            'target2016 ~ C(year) + total_dividend_amount + dividend_rate_by_sector + ending_shares + market_dividend_rate',
            data=df16).fit()
        print('RESULT REGRESSION TARGET16: ', reg_model16 .summary())

        reg_model17 = ols(
            'target2017 ~ C(year) + total_dividend_amount + dividend_rate_by_sector + ending_shares + market_dividend_rate',
            data=df17).fit()
        print('RESULT REGRESSION TARGET17: ', reg_model17 .summary())

        # correlation heatmap graph plot
        plt.show()

Worker().analysis_data()