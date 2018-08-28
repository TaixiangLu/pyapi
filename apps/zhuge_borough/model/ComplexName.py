# -*- coding: utf-8 -*-

"""
@author 万丰溥
@email wanfengpu@zhuge.com
@desc 小区均价gov表实体类
@class_name AllBoroughPrice
"""
from model.BaseModel.Base import Base


class ComplexName(Base):
    __tablename__ = 'all_borough_price'
    fields = {
        "id": 0,
        "complex_id": 0,
        "complex_name": "",
        "cityarea_name": "",
        "cityarea2_name": "",
        "avgprice": 0,
        "city_en": "",
        "city_id": 0,
        "city_cn": "",
        "type": "",
        "rank": "",
    }
