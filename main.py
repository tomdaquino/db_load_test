import os
import time
import sys
import pprint
from retry import retry

try:
    from connectors.mysql import MySQLDB
    from connectors.postgresql import PGDB
except Exception as e:
    sys.exit("{}. Please run 'pip3 install -r requirements.txt'.".format(e))

from configparser import ConfigParser

# use config parser to read default values

config = ConfigParser()
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
config.read(config_file)

default = config["DEFAULT"]
mysql_config = config["MYSQL"]
pg_config = config["PG"]
pg_config["query_count"] = config["DEFAULT"]["Load_QueryCount"]

def print_table(table):
    if not table: return
    print("Returned data:")
    for row in table:
        print(row)
    print("")

@retry(tries=5, delay=1)
def connect(db_type, isReconnect=False):
    if db_type == "mysql":
        return MySQLDB(mysql_config, isReconnect)
    if db_type == "pg":
        return PGDB(pg_config)
    sys.exit("Please specify the database type as MYSQL|PG'")

# @retry(tries=100, delay=10)
def run_with_retry(query_count, db_type):
    db_obj = connect(db_type)

    while run_with_retry.counter < query_count:
        try:
            run_with_retry.counter += 1
            print("QUERY COUNT: {}".format(run_with_retry.counter))
            ret = db_obj.query(db_obj.random_query_generator())
            if db_type in ['mysql', 'pg']:
                print_table(ret)
            else:
                pprint.pprint(ret.next())
            time.sleep(int(default["Load_DelayTimeSeconds"]))
        except (KeyboardInterrupt):
            sys.exit("Exiting...")
        except Exception as e:
            # print("Failed to execute query: {}".format(e))
            # Reconnect
            # print("Reconnecting...")
            try:
                db_obj = connect(db_type, True)
            except Exception as e:
                ...

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit(
            "Please use the syntax 'python3 main.py MYSQL|PG'")
    query_count = int(default["Load_QueryCount"])
    run_with_retry.counter = 1
    db_obj = None
    print("Generating load")
    run_with_retry(query_count, sys.argv[1].lower())
