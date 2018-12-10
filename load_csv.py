import csv
from cnx_information import Connect


class Worker():
    def __init__(self):
        self.klips_order = 1

        self.csv_home_name = 'c:/Users/is/Desktop/1_19_labor/csv/klips' + '%s' % self.klips_order + 'h.csv'

    def load_csv_and_import_mysql(self):
        cur = Connect().cnx().cursor()
        for order in range(1, 20):
            csv_person_name = 'c:/Users/is/Desktop/1_19_labor/csv/klips' + '%s' % order + 'p.csv'
            # print(csv_person_name)
            with open(csv_person_name) as csvfile:
                sp = csv.DictReader(csvfile)
                print(sp)
                # for row in sp:
                sql ="""
                CREATE
                TABLE
                `workbench`.
                `klips1h`(`orghid98`
                text, `hhid01`
                text, `hwave01`
                text, `hwaveent`
                text, `sample98`
                text, `h010141`)
                """

Worker().load_csv_and_import_mysql()