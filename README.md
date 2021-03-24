# mysql_load_test
Simple query generator for putting a load on a MySQL database

Download the source:
`git clone https://github.com/tomdaquino/mysql_load_test.git`

Create a python virtual environment:
`python3 -m venv mysql_load_test/venv`

Activate the virtual environment:
`cd mysql_load_test; source venv/bin/activate`

Install the required python libs:
`pip3 install -r requirements.txt`

**Note that you may need to change the mysql connector version in the requirements.txt file to match that of your MySQL database.

Edit the config.ini file and change as follows:

    Load_Host - The hostname or IP address of your database

    Load_Port - The port number the database where the is listening

    Load_User - The username to authenticate to the database with

    Load_Password - The password for the above user

    Load_Database - The name of the database you want to query

    Load_Limit - The row limit for the queries that are issued

Run the script:
`python3 main.py MYSQL`
