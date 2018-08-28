#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : wanfengpu@zhugefang.com
# @Desc    : 全国居民家庭房产价值
from model.BaseModel.Base import Base
class CitizensHousePrice(Base):
    __tablename__ = 'citizens_house_price'

    fields = {
        "id": 0,
        "price_area": "",
        "holds": "",
        "indexes": 0,
    }
