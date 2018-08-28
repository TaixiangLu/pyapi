#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 城区月均价数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.config.Config import mongo_conf
from databases.dbfactory.dbfactory import dbfactory
from apps.zhuge_borough.model.City_month_list_price import City_month_list_price
from cache.LocalCache import LocalCache
class CityMonthListPriceDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", City_month_list_price), conf_name=kwargs.get("conf_name", "borough"))

    @LocalCache(conf_name='borough_api', key="city", time=86400)
    def getCityMonthPrice(self, *args, **kwargs):
        return self.find_page(*args, **kwargs)
