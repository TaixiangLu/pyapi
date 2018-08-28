#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.BoroughRecycleDao import BoroughRecycleDao
class BoroughRecycleService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = BoroughRecycleDao(*args, **kwargs)

    def insert_one(self, city, datas):
        result = self.add_one(city=city, datas=datas)
        return result



