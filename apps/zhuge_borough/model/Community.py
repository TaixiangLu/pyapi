#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 商圈数据层
from model.BaseModel.Base import Base
class Community(Base):
    __tablename__ = 'community'
    # __tablename__ = 'cityarea_0328'
    fields = {
        "id": 0,
        "name": 0,

    }

