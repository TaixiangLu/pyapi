#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 小区数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.Borough_online import Borough_online
from cache.LocalCache import LocalCache
from cache.Pcache import Pcache


class BoroughOnlineDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Borough_online), conf_name=kwargs.get("conf_name", "borough"))

    # 根据条件获取数据
    def get_one(self, *args, **kwargs):
        return self.find_one(*args, **kwargs)

    # 获取小区详情信息
    @LocalCache(conf_name='borough_api', key="borough_id", time=86400)
    def getBoroughDetail(self, *args, **kwargs):
        return self.find_one(*args, **kwargs)

    # 获取小区列表
    # @LocalCache(conf_name='borough_api', key="borough_id")
    def getBoroughList(self, *args, **kwargs):
        return self.find_page(*args, **kwargs)

    # 获取小区详情信息PK
    @Pcache(conf_name='sell_api', key="borough_id")
    def getBoroughDetailPK(self, *args, **kwargs):
        return self.find_page(*args, **kwargs)
