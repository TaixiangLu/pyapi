#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
import sys
sys.path.append("../../../../")
from service.BaseService.BaseMysqlService import BaseMysqlService
from apps.zhuge_price.dao.detail.MonthlyHouseSellPriceDistributionDao import MonthlyHouseSellPriceDistributionDao
from apps.zhuge_sell.dao.detail.HouseSellInfoDao import HouseSellInfoDao
from utils.BaseUtils import BaseUtils
from apps.config.CityConfig import CityConfig
import datetime
import time


class PriceDistributionService(BaseMysqlService):
    """
    不同城市等级对应价格范围的房源分布
    每天晚上例行
    """
    def __init__(self, *args, **kwargs):
        self.house_sell_info = HouseSellInfoDao()
        self.monthly_house_sell_price_distribution = MonthlyHouseSellPriceDistributionDao(*args, **kwargs)
        self.citys = CityConfig.get_cityinfo(where={"is_sell": 1})  # 获取当前二手房开通城市列表

        # 城市分级 默认为3
        self.city_level_mapping = {
            'bj': 1,
            'sh': 1,
            'gz': 1,
            'sz': 1,
            'hz': 1,
            'nj': 2,
            'tj': 2,
            'wh': 2,
            'su': 2,
            'dg': 2,
            'zh': 2,
            'qd': 2,
            'fz': 2,
            'cz': 2,
        }

        # 不同城市对应的价格范围
        self.price_range_mapping = {
            1: [0, 200, 250, 300, 500, 1000, 99999],
            2: [0, 100, 200, 300, 500, 1000, 99999],
            3: [0, 30, 50, 80, 100, 150, 9999],
        }
        self.price_range_mapping_x = {
            1: ['200万以下', '200~250万', '250~300万', '300~500万', '500~1000万', '1000万以上'],
            2: ['100万以下', '100~200万', '200~300万', '300~500万', '500~1000万', '1000万以上'],
            3: ['30万以下', '30~50万', '50~80万', '80~100万', '100~150万', '150万以上']
        }

    def run(self, **kwargs):
        type_mapping = {
            1: "city_distribution",
            2: "cityarea_distribution",
            3: "cityarea2_distribution"
        }
        pms = kwargs.get('pms')
        area_type = pms.get('type', 1)
        return getattr(PriceDistributionService, type_mapping.get(int(area_type)))(self, **kwargs)

    def city_distribution(self, **kwargs):
        city = kwargs.get('city')
        city_level = self.city_level_mapping.get(city, 3)
        price_range = self.price_range_mapping.get(city_level)
        price_range_x = self.price_range_mapping_x.get(city_level)
        try:
            data = dict()
            for i in range(len(price_range) - 1):
                # 城市价格分布
                sql = "SELECT count( DISTINCT( B.id ) ) as total FROM house_sell B " \
                      "LEFT JOIN house_sell_info A ON A.id = B.id WHERE A.source != 10  AND A.`status` = 1  " \
                      f"AND A.house_price BETWEEN {price_range[i]}  AND {price_range[i+1] - 1}"
                result = self.house_sell_info.city(city=city, sql={'sql': sql}, cache_key=i)
                if result:
                    data.setdefault(price_range_x[i], int(result.get("total", 0)))
            return {'data': data}
        except Exception as e:
            print(e)

    def cityarea_distribution(self, **kwargs):
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        cityarea_id = pms.get('cityarea_id', -1)
        city_level = self.city_level_mapping.get(city, 3)
        price_range = self.price_range_mapping.get(city_level)
        price_range_x = self.price_range_mapping_x.get(city_level)
        try:
            data = dict()
            for i in range(len(price_range) - 1):
                # 城区价格分布
                area_sql = "SELECT count( DISTINCT( B.id ) ) as total FROM house_sell B " \
                           "LEFT JOIN house_sell_info A ON A.id = B.id WHERE A.source != 10 " \
                           f"AND A.`status` = 1 AND B.cityarea_id={cityarea_id} AND A.house_price " \
                           f"BETWEEN {price_range[i]} AND {price_range[i+1] - 1}"
                cache_key = str(cityarea_id) + "_" + str(i)
                area_result = self.house_sell_info.cityarea(city=city, sql={'sql': area_sql}, cache_key=cache_key)
                if area_result:
                    data.setdefault(price_range_x[i], int(area_result.get("total", 0)))
            return {'data': data}
        except Exception as e:
            print(e)

    def cityarea2_distribution(self, **kwargs):
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        cityarea2_id = pms.get('cityarea2_id', -1)
        city_level = self.city_level_mapping.get(city, 3)
        price_range = self.price_range_mapping.get(city_level)
        price_range_x = self.price_range_mapping_x.get(city_level)
        try:
            data = dict()
            for i in range(len(price_range) - 1):
                # 商圈价格分布
                area2_sql = "SELECT count( DISTINCT( B.id ) ) as total FROM house_sell B " \
                            "LEFT JOIN house_sell_info A ON A.id = B.id WHERE A.source != 10 " \
                            f"AND A.`status` = 1 AND B.cityarea2_id={cityarea2_id} AND A.house_price " \
                            f"BETWEEN {price_range[i]} AND {price_range[i+1] - 1}"
                cache_key = str(cityarea2_id) + "_" + str(i)
                area2_result = self.house_sell_info.cityarea2(city=city, sql={'sql': area2_sql}, cache_key=cache_key)
                if area2_result:
                    data.setdefault(price_range_x[i], int(area2_result.get("total", 0)))
            return {'data': data}
        except Exception as e:
            print(e)
