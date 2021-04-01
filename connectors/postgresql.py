import psycopg2
from psycopg2 import Error
import random
import time


class PGDB:
    def __init__(self, config):
        try:
            self.load_limit = int(config["load_limit"])
            self._db_name = config["Load_Host"]
            self._query_count = int(config["query_count"])
            self._conn = psycopg2.connect(host=config["Load_Host"],
                                          port=config["Load_Port"],
                                          user=config["Load_User"],
                                          password=config["Load_Password"],
                                          database=config["Load_Database"])
            self._cursor = self._conn.cursor()
            print("Connected to Database")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    def exit(self):
        self.commit()
        self.connection.close()

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

    def random_query_generator(self):
        try:
            tables = self.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            table_list = [table[0] for table in tables]
            random_table = random.choices(table_list, k=1)
            table_selected = random_table[0]
            columns = self.query(
                "select column_name from information_schema.columns where table_schema='public' and table_name='%s';"
                % (table_selected))
            sel_query = ""
            if len(columns):
                col_list = [column[0] for column in columns]
                col_selected = random.choices(col_list, k=3)
                sel_query = "SELECT %s,%s,%s from %s ORDER BY 2 DESC LIMIT %s" % (
                    col_selected[0], col_selected[1], col_selected[2],
                    table_selected, self.load_limit)
                if self._query_count % 4 == 0:
                    sel_query = "SELECT provider_street_address from %s LIMIT %s;" % (
                        table_selected, self.load_limit)
            return sel_query

        except Exception as error:
            print("Error while querying PostgreSQL", error)
