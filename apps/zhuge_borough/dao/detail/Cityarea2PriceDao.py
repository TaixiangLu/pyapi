#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 城区数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.Cityarea2Price import Cityarea2Price


class Cityarea2PriceDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Cityarea2Price), conf_name=kwargs.get("conf_name", "borough"))
