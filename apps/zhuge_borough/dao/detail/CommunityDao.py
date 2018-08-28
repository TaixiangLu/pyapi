#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 城区数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.Community import Community
from cache.LocalCache import LocalCache
class CommunityDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Community), conf_name=kwargs.get("conf_name", "borough"))

    def get_cityatea_by_id(self):
        return

    @LocalCache(conf_name='borough_cache', key="city", time=86400)
    def getCityareaListByCity(self, city, field):
        return self.find_page(city = city, field=field)