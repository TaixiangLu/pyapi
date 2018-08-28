#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 地铁站数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.Subway_station import Subway_station
class SubwayStationDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Subway_station),  conf_name=kwargs.get("conf_name", "borough"))

