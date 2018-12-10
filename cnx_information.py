from config import config

import pymysql

class BaseManager:
    def get_cnx(db_name, **kwargs):
        db = config['db'][db_name]
        engine = db['engine']

        if engine == db['engine']:
            cnx = pymysql.connect(user=db['user'], password=db['password'],
                                  host=db['host'], database=db['schema'],
                                  charset=['utf8mb4'],
                                  **kwargs
                                  )
            return cnx