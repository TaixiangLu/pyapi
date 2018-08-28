#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 17-10-10 下午8:25
# @Author  : jianguo@zhugefang.com
# @Desc    : 公共查询es语句
import json
class BaseSearchDSL(object):

    def match_all(self):
        match_all = {"query": {"bool": {"must": [{"match_all": {}}], "must_not": [], "should": []}}, "from": 0, "size": 10,"sort": [{"cnt": {"order": "desc"}}], "aggs": {}}
        return match_all

    def search_in(self, ids):
        search_in = {"query": {"bool": {"must": [{"terms": {"id": ids}}], "must_not": [], "should": []}}, "from": 0, "size": 10, "sort": [], "aggs": {}}
        return search_in

    def search_filter(self, filter):
        pass
