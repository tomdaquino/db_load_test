import mysql.connector as mysql
from mysql.connector import Error
import os
import random
import time
from retry import retry

class MySQLDB:
    def __init__(self, config, isReconnect=False):
        self.config = config
        self.load_limit = self.config["Load_Limit"]
        self._db_name = self.config["Load_Host"]
        self._conn = mysql.connect(host=self.config["Load_Host"],
                        port=self.config["Load_Port"],
                        user=self.config["Load_User"],
                        passwd=self.config["Load_Password"],
                        database=self.config["Load_Database"],
                        connection_timeout=2,
                        )
        if self._conn.is_connected() and not isReconnect:
            dbInfo = self._conn.get_server_info()
            print("Connected to MySQL Server version {}".format(dbInfo))
        self._cursor = self._conn.cursor()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()

    @retry(exceptions=Exception, tries=3, delay=1)
    def prepared_load(self):
        print("query1")
        if self._db_name == "employees":
            sqlQueryList = os.listdir('sql_queries/')
            for sqlFile in sqlQueryList:
                with open('../sql_queries/' + sqlFile) as sql:
                    try:
                        print("\nFetching data from database....\n")
                        print(self.query(sql.read()))
                        time.sleep(2)
                    except Exception as e:
                        print("Error running query")
        else:
            print("Pre-defined queries not available for - {}".format(self._db_name))

    @retry(exceptions=Exception, tries=3, delay=0)
    def random_query_generator(self):
        table_selected = ''
        tables = self.query("show tables;")
        # print(tables)
        if len(tables):
            table_selected = random.choice(tables)[0]
            columns = self.query("desc %s;" % (table_selected,))
            if len(columns):
                col_list = [column[0] for column in columns]
                col_selected = random.choices(col_list, k=3)
            sel_query = "SELECT %s,%s,%s from %s ORDER BY 2 DESC LIMIT %s;" % (
                        col_selected[0], col_selected[1], col_selected[2],
                        table_selected, self.load_limit)
            print(sel_query)
            print("Query: {}".format(sel_query))
            return sel_query

    def exit(self):
        # self.commit()
        self.connection.close()
