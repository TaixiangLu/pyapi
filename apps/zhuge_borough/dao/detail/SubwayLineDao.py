#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 地铁线数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.config.Config import mongo_conf
from databases.dbfactory.dbfactory import dbfactory
from apps.zhuge_borough.model.Subway_line import Subway_line
class SubwayLineDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Subway_line),  conf_name=kwargs.get("conf_name", "borough"))
