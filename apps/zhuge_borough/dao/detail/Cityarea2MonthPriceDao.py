#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 商圈价格数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.Cityarea2_month_price import Cityarea2_month_price
from cache.LocalCache import LocalCache


class Cityarea2MonthPriceDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Cityarea2_month_price), conf_name=kwargs.get("conf_name", "borough"))

    @LocalCache(conf_name='borough_api', key="cache_key", time=86400)
    def getCityarea2MonthPrice(self, *args, **kwargs):
        return self.find_page(*args, **kwargs)
