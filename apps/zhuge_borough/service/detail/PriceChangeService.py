#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
import sys
sys.path.append("../../../../")
from service.BaseService.BaseMysqlService import BaseMysqlService
from apps.zhuge_price.dao.detail.DailyHouseSellAvgpriceDao import DailyHouseSellAvgpriceDao
from apps.zhuge_sell.dao.detail.HouseSellHistoryPriceDao import HouseSellHistoryPriceDao
from apps.config.CityConfig import CityConfig
import datetime
import time

# TODO group by 效率太低这个类和方法都没用


class PriceChangeService(BaseMysqlService):
    """
    新上涨降价房源数量
    """
    def __init__(self, *args, **kwargs):
        self.house_sell_history_price = HouseSellHistoryPriceDao()
        self.daily_house_sell_avgprice = DailyHouseSellAvgpriceDao()
        self.cities = CityConfig.get_cityinfo(where={"is_sell": 1})
        self.change_type_mapping = {
            0: 'count_add',  # 新增
            1: 'count_reduce',  # 降价
            2: 'count_rise',    # 涨价
        }

    def run(self, **kwargs):
        type_mapping = {
            1: "city",
            2: "cityarea",
            3: "cityarea2"
        }
        pms = kwargs.get('pms')
        area_type = pms.get('type', 1)
        return getattr(PriceChangeService, type_mapping.get(int(area_type)))(self, **kwargs)

    def city(self, **kwargs):
        city = kwargs.get('city')
        t = int(time.time())
        today_zero_time = t - t % 86400
        try:
            data = dict()
            # 城市涨降价分布
            sql = 'select change_type, count(1) total from house_sell_history_price ' \
                  f'where now_created >={today_zero_time} GROUP BY change_type'
            result = self.house_sell_history_price.city(city=city, sql={'sql': sql})
            for i in result:
                data.setdefault(i['change_type'], i['total'])
            return {'data': data}
        except Exception as e:
            print(e)

    def cityarea(self, **kwargs):
        city = kwargs.get('city')
        t = int(time.time())
        today_zero_time = t - t % 86400
        pms = kwargs.get('pms')
        cityarea_id = int(pms.get('cityarea_id', -1))
        try:
            data = dict()
            # 城区涨降价分布
            area_sql = 'select change_type, count(1) total from house_sell_history_price ' \
                        f'where now_created >={today_zero_time} and cityarea_id={cityarea_id} GROUP BY change_type'
            area_result = self.house_sell_history_price.cityarea(city=city, sql={'sql': area_sql}, cache_key=cityarea_id)
            for i in area_result:
                data.setdefault(i['change_type'], i['total'])
            return {'data': data}
        except Exception as e:
            print(e)

    def cityarea2(self, **kwargs):
        city = kwargs.get('city')
        t = int(time.time())
        today_zero_time = t - t % 86400
        pms = kwargs.get('pms')
        cityarea2_id = str(pms.get('cityarea2_id', -1))
        try:
            data = dict()
            # 商圈涨降价分布
            area_sql = 'select change_type, count(1) total from house_sell_history_price ' \
                       f'where now_created >={today_zero_time} and cityarea2_id={cityarea2_id} GROUP BY change_type'
            area_result = self.house_sell_history_price.cityarea2(city=city, sql={'sql': area_sql}, cache_key=cityarea2_id)
            for i in area_result:
                data.setdefault(i['change_type'], i['total'])
            return {'data': data}
        except Exception as e:
            print(e)
