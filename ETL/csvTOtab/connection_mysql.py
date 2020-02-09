#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql
"""
Module which connects to database
"""
# NOTE: passwords for script user are different on the two servers
def connection_db(host, db, password, user):
    """
    Connect to database
    """
    try:
        db = pymysql.connect(host=host, db=db, password=password, user=user, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        return db
    except Exception as E:
        print(str(E))        
