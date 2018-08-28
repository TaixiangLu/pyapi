#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 城区周均价数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.City_week_price import City_week_price
from cache.LocalCache import LocalCache
class CityWeekPriceDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", City_week_price),  conf_name=kwargs.get("conf_name", "borough"))

    @LocalCache(conf_name='borough_api', key="city", time=86400)
    def getCityWeekPrice(self, *args, **kwargs):
        return self.find_page(*args, **kwargs)