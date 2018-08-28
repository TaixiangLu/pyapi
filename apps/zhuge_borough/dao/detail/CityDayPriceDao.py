#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 城区周均价数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.City_day_price import City_day_price
class CityDayPriceDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", City_day_price),  conf_name=kwargs.get("conf_name", "borough"))

