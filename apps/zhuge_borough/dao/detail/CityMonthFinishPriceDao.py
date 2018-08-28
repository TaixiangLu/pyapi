#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 城区月均价数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.City_month_finish_price import City_month_finish_price
from cache.LocalCache import LocalCache


class CityMonthFinishPriceDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", City_month_finish_price), conf_name=kwargs.get("conf_name", "borough"))
