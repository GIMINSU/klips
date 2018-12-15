import pymysql
from pymysql.cursors import DictCursor
import csv
from datetime import datetime, date
import pprint


from config import config

class Worker():
    def __init__(self):
        self.dbname=config['db']["first_server"]
        self.user=config['db']['first_server']["user"]
        self.password=config['db']['first_server']['password']
        self.host=config['db']['first_server']['host']
        self.database=config['db']['first_server']['schema']

    def load_csv_and_import_mysql(self):

        cnx_sql = pymysql.connect(user=self.user,
                                  password=self.password,
                                  host=self.host,
                                  database=self.database,
                                  charset='utf8mb4',
                                  cursorclass=DictCursor,
                                  autocommit=False)
        cur = cnx_sql.cursor(DictCursor)

        csv_file_name = 'C:/Users/is/Desktop/dividend_rawdata.csv'
        # print(csv_file_name)
        f = open(csv_file_name, 'r', encoding='UTF-8')
        reader = csv.reader(f)
        # with open(csv_file_name, newline='') as f:
        # print(type(reader))
        data_dict = dict()
        idata = []
        for row in reader:
            if row[0] != '\ufeffname' or row[1] != 'code' or row[2] != 'year':
                name = str(row[0])
                code = str(row[1])
                year = int(row[2])
                # print(year)
            if len(row[0]) < 30:
                # print(name)
                if row[0] != '\ufeffname':
                    print(row[2])
                    idata.append(row)
                    # print(name)
                    # if name not in data_dict:
                    #     data_dict[name] = dict()
                    # if year not in data_dict[name]:
                    #     data_dict[name][year] = dict(
                    #         code=row['code']
                    #     )
            # if name in data_dict:
            #     if year in data_dict[name]:
            #         data_dict[name][year]['code'] = row['code']
            #         # if name in data_dict:
            #         #     data_dict[name]['code'] = row['code']
        pprint.pprint(idata)
        # idata.append(row)
        # pprint.pprint(data_dict)
        # pprint.pprint(idata[0])
        # second_data = []
        # for i in range(len(idata)):
        #     if i > 0:
        #         second_data.append(i)
        # print(second_data)

        cur.execute('''
        INSERT INTO rawdata_dividend(
        name,
        code,
        year,
        month,
        sector,
        dividend_rate_by_sector,
        par_value,
        ending_shares,
        dividend_per_share,
        dividend_payoff_ratio,
        total_dividend_amount,
        total_dividend_rate
        ) VALUES (
        %(name)s,
        %(code)s,
        %(year)s,
        %(month)s,
        %(sector)s,
        %(dividend_rate_by_sector)s,
        %(par_value)s,
        %(ending_shares)s,
        %(dividend_per_share)s,
        %(dividend_payoff_ratio)s,
        %(total_dividend_amount)s,
        %(total_dividend_rate)s
        )''', idata)

        cnx_sql.commit()
        cur.close()
        print("DONE")


Worker().load_csv_and_import_mysql()