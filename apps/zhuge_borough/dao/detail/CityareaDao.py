#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 城区数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.Cityarea import Cityarea
from cache.LocalCache import LocalCache


class CityareaDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Cityarea), conf_name=kwargs.get("conf_name", "borough"))

    def get_cityatea_by_id(self):
        return

    @LocalCache(conf_name='borough_api', key="city", time=86400)
    def getCityareaListByCity(self, *args, **kwargs):
        return self.find_page(*args, **kwargs)

    @LocalCache(conf_name='borough_api', key="_id", time=86400)
    def getCityareaInfoById(self, *args, **kwargs):
        return self.find_page(*args, **kwargs)
