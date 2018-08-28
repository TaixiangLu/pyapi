#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 小区月均价数据层
from model.BaseModel.Base import Base
class Borough_month_price(Base):
    __tablename__ = 'borough_month_price'

    fields = {
        "id": 0,
        "name": 0,
    }

