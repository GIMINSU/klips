import csv
from cnx_information import Connect
from datetime import datetime, date
import pprint

class Worker():
    # def __init__(self):
        # self.csv_home_name = 'c:/Users/is/Desktop/1_19_labor/csv/klips' + '%s' % self.klips_order + 'h.csv'
    def load_csv_and_import_mysql(self):
        cur = Connect().cnx().cursor()
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
                print(year)
            # if len(row[0]) < 30:
            #     # print(name)
            #     if row[0] != '\ufeffname':
            #         print(row[2])
            #         idata.append(row)
                    # # print(name)
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
        # pprint.pprint(idata)
        # idata.append(row)
        # pprint.pprint(data_dict)
        # # pprint.pprint(idata[0])
        # second_data = []
        # for i in range(len(idata)):
        #     if i > 0:
        #         second_data.append(i)
        #

        cur.execute('''
        INSERT INTO dividend_rawdata(
        name,
        code,
        year,
        month,
        sector,
        dividend_rate,
        per_value,
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
        %(dividend_rate)s,
        %(per_value)s,
        %(ending_shares)s,
        %(dividend_per_share)s,
        %(dividend_payoff_ratio)s,
        %(total_dividend_amount)s,
        %(total_dividend_rate)s
        )''', idata)

        Connect().cnx().commit()
        cur.close()
        print("DONE")


Worker().load_csv_and_import_mysql()