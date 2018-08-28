#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-20 上午10:56
# @Author  : jianguo@zhugefang.com
# @Desc    :
from apps.config.Config import es_config
from elasticsearch import Elasticsearch
import contextlib

class EsDB(object):
    def __init__(self, *args, **kwargs):
        self.conf_name = kwargs.get("conf_name")
        db_config = es_config.get(self.conf_name)

        self.host = db_config.get("host")
        self.port = db_config.get("port")
        self.user = db_config.get("username", "")
        self.passwd = db_config.get("password", "")
        self.maxsize = db_config.get('maxsize',10)

    @contextlib.contextmanager
    def get_conn(self, **kwargs):
        # self.city = kwargs.get("city")
        # self.db_name = get_db(type=self.conf_name, city=self.city)
        es = Elasticsearch(hosts=self.host, maxsize=self.maxsize)
        yield es