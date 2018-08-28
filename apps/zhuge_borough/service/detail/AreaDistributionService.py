#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
import sys

sys.path.append("../../../../")
from service.BaseService.BaseMysqlService import BaseMysqlService
from apps.zhuge_price.dao.detail.MonthlyHouseSellAreaDistributionDao import MonthlyHouseSellAreaDistributionDao
from apps.zhuge_sell.dao.detail.HouseSellInfoDao import HouseSellInfoDao
from apps.config.CityConfig import CityConfig


class AreaDistributionService(BaseMysqlService):
    """
    不同城市等级对应面积范围的房源分布
    每天晚上例行
    """

    def __init__(self, *args, **kwargs):
        self.house_sell_info = HouseSellInfoDao()
        self.monthly_house_sell_area_distribution = MonthlyHouseSellAreaDistributionDao(*args, **kwargs)
        self.citys = CityConfig.get_cityinfo(where={"is_sell": 1})  # 获取当前二手房开通城市列表

        # 不同城市对应的范围
        self.area_range = [0, 60, 90, 140, 200, 9999]
        self.area_range_x = ['60平米以下', '60~90平米', '90~140平米', '140~200平米', '200平米以上']

    def run(self, **kwargs):
        type_mapping = {
            1: "city_distribution",
            2: "cityarea_distribution",
            3: "cityarea2_distribution"
        }
        pms = kwargs.get('pms')
        area_type = pms.get('type', 1)
        return getattr(AreaDistributionService, type_mapping.get(int(area_type)))(self, **kwargs)

    def city_distribution(self, **kwargs):
        city = kwargs.get('city')
        try:
            data = dict()
            for i in range(len(self.area_range) - 1):
                # 城市面积分布
                sql = "SELECT count( DISTINCT( B.id ) ) as total FROM house_sell B " \
                      "LEFT JOIN house_sell_info A ON A.id = B.id WHERE A.source != 10  AND A.`status` = 1  " \
                      f"AND B.house_totalarea BETWEEN {self.area_range[i]}  AND {self.area_range[i+1] - 1}"
                result = self.house_sell_info.city(city=city, sql={'sql': sql}, cache_key=str(i))
                if result:
                    data.setdefault(self.area_range_x[i], int(result.get("total", 0)))
            return {'data': data}
        except Exception as e:
            print(e)

    def cityarea_distribution(self, **kwargs):
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        cityarea_id = pms.get('cityarea_id', -1)
        try:
            data = dict()
            for i in range(len(self.area_range) - 1):
                # 城区面积分布
                area_sql = "SELECT count( DISTINCT( B.id ) ) as total FROM house_sell B " \
                           "LEFT JOIN house_sell_info A ON A.id = B.id WHERE A.source != 10 " \
                           f"AND A.`status` = 1 AND B.cityarea_id={cityarea_id} AND B.house_totalarea " \
                           f"BETWEEN {self.area_range[i]} AND {self.area_range[i+1] - 1}"
                cache_key = str(cityarea_id) + "_" + str(i)
                area_result = self.house_sell_info.cityarea(city=city, sql={'sql': area_sql}, cache_key=cache_key)
                if area_result:
                    data.setdefault(self.area_range_x[i], int(area_result.get("total", 0)))
            return {'data': data}
        except Exception as e:
            print(e)

    def cityarea2_distribution(self, **kwargs):
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        cityarea2_id = pms.get('cityarea2_id', -1)
        try:
            data = dict()
            for i in range(len(self.area_range) - 1):
                # 商圈面积分布
                area2_sql = "SELECT count( DISTINCT( B.id ) ) as total FROM house_sell B " \
                            "LEFT JOIN house_sell_info A ON A.id = B.id WHERE A.source != 10 " \
                            f"AND A.`status` = 1 AND B.cityarea2_id={cityarea2_id} AND B.house_totalarea " \
                            f"BETWEEN {self.area_range[i]} AND {self.area_range[i+1] - 1}"
                cache_key = str(cityarea2_id) + "_" + str(i)
                area2_result = self.house_sell_info.cityarea2(city=city, sql={'sql': area2_sql}, cache_key=cache_key)
                if area2_result:
                    data.setdefault(self.area_range_x[i], int(area2_result.get("total", 0)))
            return {'data': data}
        except Exception as e:
            print(e)
