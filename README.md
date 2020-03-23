This project use Python 3 with the libraries pymongo, pymysql, pprint, threading, datetime, time, json and csv

For the web part the mysql part should work as most of the apache environment come with mysql and php drivers already, but for the mongoDB part we need to install the mongoDB drivers for php and also the mongoDB/mongoDB library with composer, here the library is already in the repository.

My branch use localhost and port 3308 for mysql and 27017 for mongoDB to change the port, host, user and password used by the python code change the values in the csvTOmongo/config.py and csvTOmysql/config.py
