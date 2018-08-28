#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-3-5 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from dao.BaseDao.BaseEs import BaseES
from apps.zhuge_borough.model.BoroughSearchMapping import BoroughSearchMapping
class BoroughSearchDao(BaseES):
    def __init__(self, *args, **kwargs):
        super().__init__(conf_name=kwargs.get("conf_name", "house_new_online"), model=kwargs.get("model", BoroughSearchMapping))