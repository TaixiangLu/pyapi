#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 商圈数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.Cityarea2 import Cityarea2
from cache.LocalCache import LocalCache


class Cityarea2Dao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Cityarea2), conf_name=kwargs.get("conf_name", "borough"))

    @LocalCache(conf_name='borough_api', key='cityarea_id', time=86400)
    def getCityarea2InfoByCityareaId(self, city, cityarea_id):
        filter = {'cityarea_id': cityarea_id}
        field = {'_id': 1, 'name': 1, 'area2_pinyin': 1}
        page = {"index": 1, "size": 100}
        return self.find_page(city=city, filter=filter, field=field, page=page)

    @LocalCache(conf_name='borough_api', key="cityarea2_id", time=86400)
    def getCityarea2InfoById(self, city, cityarea2_id):
        filter = {'_id': cityarea2_id}
        field = {'_id': 1, 'name': 1, 'loc': 1, 'area2_pinyin': 1}
        page = {"index": 1, "size": 1}
        return self.find_page(city=city, filter=filter, field=field, page=page)

    @LocalCache(conf_name='borough_api', key="city", time=86400)
    def getCityarea2ListByCity(self, *args, **kwargs):
        return self.find_page(*args, **kwargs)

    @LocalCache(conf_name='borough_api', update="Cityarea2Dao.getCityarea2InfoById")
    def delete(self):
        pass
