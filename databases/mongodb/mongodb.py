#!/usr/bin/env python
# coding:utf-8
# Author:   --<lucas>
# Purpose: MongoDB的使用
# Created: 2017/06/22
from pymongo import MongoClient
from apps.config.Config import mongo_conf
from apps.config.CitySouceMapping import get_db
import logging
import contextlib

class MongoDB(object):
    def __init__(self, *args, **kwargs):
        self.conf_name = kwargs.get("conf_name")
        self.conf = mongo_conf.get(self.conf_name)

    @contextlib.contextmanager
    def getMongoConn(self, *args, **kwargs):
        conf = self.conf.get(kwargs.get("link_type", 'default'))
        city = kwargs.get("city")
        host = conf.get("host")
        port = conf.get("port")
        user = conf.get("username", "")
        passwd = conf.get("password", "")
        self.db_name = get_db(type=self.conf_name, city=city)
        if user and passwd:
            uri = "mongodb://{username}:{password}@{host}:{port}".\
                format(username=user, password=passwd, host=host, port= port)
        else:
            uri = "mongodb://{host}:{port}".format(host=host, port=port)
        conn = MongoClient(uri)
        try:
            client = conn.get_database(self.db_name)
            yield client
        except Exception as e:
            logging.error('ERROR',e)
        finally:
            # conn.close()
            pass
