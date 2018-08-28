#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.SubwayStationDao import SubwayStationDao
class SubwayStationService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = SubwayStationDao(*args, **kwargs)

    def get_all(self, *args, **kwargs):
        return self.dao.find_all(*args, **kwargs)