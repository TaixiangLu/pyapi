#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.CityDayPriceDao import CityDayPriceDao
from apps.zhuge_borough.dao.detail.CityMonthListPriceDao import CityMonthListPriceDao
class CityDayPriceService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = CityDayPriceDao(*args, **kwargs)
        self.monthPriceServiceDao = CityMonthListPriceDao(*args, **kwargs)

    def get_percent(self, *args, **kwargs):
        """
        获取涨跌百分比
        """
        nowPrice = kwargs.get("nowPrice")
        prePrice = kwargs.get("prePrice")
        percent = round((nowPrice - prePrice) / prePrice, 4)
        return percent

    def get_type(self, *args, **kwargs):
        """
        获取涨跌状态
        """
        nowPrice = kwargs.get("nowPrice")
        prePrice = kwargs.get("prePrice")
        price = nowPrice - prePrice

        if nowPrice-prePrice:
            return 1
        else:
            return -1

    def getCityDayPrice(self, *args, **kwargs):
        result = {}
        data = {}
        city = kwargs.get('city')
        field = {'_id': 0, 'price': 1, 'date': 1}  # 返回字段
        sort = [("date", -1)]  # 过滤条件
        page = kwargs.get('page') or {"index": 1, "size": 1}
        day_price_data = self.dao.find_page(city=city, sort=sort, page=page, field=field)
        data['nowPrice'] = day_price_data[0]['price']
        month_price_data = self.monthPriceServiceDao.find_page(city=city, sort=sort, page=page, field=field)
        data['prePrice'] = month_price_data[0]['price']
        data['percent'] = self.get_percent(nowPrice=data['nowPrice'], prePrice=data['prePrice'])
        data['type'] = self.get_type(nowPrice=data['nowPrice'], prePrice=data['prePrice'])
        result.setdefault('data', data)
        return result



