# -*- coding: utf-8 -*-

"""
@author lucas
@email lutaixiang@zhugefang.com
@desc 二手房源GOV表实体类
@class_name HouseSellGov
"""
from model.BaseModel.Base import Base
from decimal import Decimal

class Complex(Base):
    __tablename__ = 'complex'
    fields = {
        'id': 0,
        'base_id': 0,
        'cityarea_name': '',
        'cityarea_id': 0,
        'city_id': 0,
        'complex_id': 0,
        'complex_pinyin': '',
        'complex_alias': '',
        'complex_loopline': 0,
        'cityarea2_pinyin': '',
        'cityarea2_id': '',
        'complex_desc': '',
        'complex_address': '',
        'cityarea2_name': '',
        'complex_building_type': '',
        'complex_name': '',
        'complex_tag': '',
        'cityarea_pinyin': '',
        'ctime': 0,
        'developer_offer': '',
        'developer_offer_expiry': '',
        'developer_name': '',
        'first_saletime': 0,
        'firstnew_saletime': 0,
        'first_delivertime': '',
        'first_logic_saletime': 0,
        'greening_rate': '',
        'heating_mode': '',
        'hydropower_gas': '',
        'hot_line': '',
        'lng': Decimal(1),
        'lat': Decimal(1),
        'license': '',
        'property_year': '',
        'property_type': '',
        'preferential_status': '',
        'parking_rate': '',
        'property_costs': '',
        'property_company': '',
        'priority': '',
        'parking_count': '',
        'sale_status': '',
        'source_url': '',
        'subway_info': '',
        'renovation': '',
        'source_id': '',
        'sortweight': 0,
        'merge_total': 0,
        'take_land_time': '',
        'utime': 0,
        'volume_rate': '',
        'sale_weight': 0,
        'sale_phone': '',
        'status': 1,
        'human_modified': '',
        'property_desc': "",
        'floor_desc': "",
        'architectural': '',
        'salesoffice_address': '',
        'building_totalarea': '',
        'building_area': '',
        'building_total': '',
        'house_total': '',
        'property_id': 0,
        'developer_id': 0,
        'developer_price': 0
    }
