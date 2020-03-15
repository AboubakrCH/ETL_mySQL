#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymongo
"""
Module which connects to database
"""
# NOTE: passwords for script user are different on the two servers
def connection_db(host, port, database):
    """
    Connect to database
    """
    try:
        client = pymongo.MongoClient(host=host, port=port)
        db = client[database]
        return db
    except Exception as E:
        print(str(E))        

def connection_client(host, port):
    """
    Connect to database
    """
    try:
        client = pymongo.MongoClient(host=host, port=port)
        
        return client
    except Exception as E:
        print(str(E))        

