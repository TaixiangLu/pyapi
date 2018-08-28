#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 小区数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.Borough_month_price import Borough_month_price
from cache.LocalCache import LocalCache
from cache.Pcache import Pcache
class BoroughMonthPriceDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Borough_month_price), conf_name=kwargs.get("conf_name", "borough"))

    @LocalCache(conf_name='borough_api', key="cache_key", time=86400)
    def getBoroughMonthPrice(self, *args, **kwargs):
        return self.find_page(*args, **kwargs)

    @Pcache(conf_name='sell_api', key="borough_id")
    def getBoroughMonthPricePK(self, *args, **kwargs):
        return self.find_page(*args, **kwargs)
