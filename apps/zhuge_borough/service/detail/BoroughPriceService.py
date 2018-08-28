#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.service.detail.BoroughMonthPriceService import BoroughMonthPriceService
from apps.zhuge_borough.dao.detail.BoroughPriceDao import BoroughPriceDao
from cache.Pcache import Pcache
class BoroughPriceService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = BoroughPriceDao(*args, **kwargs)
        self.borough_month_price_service = BoroughMonthPriceService(*args, **kwargs)

    def get_page(self, *args, **kwargs):
        borough_id = kwargs.get("borough_id")  # 起始时间
        kwargs.get("sort") or [("_id", 1)]
        kwargs['field'] = {'_id': 0, 'borough_price': 1, 'date': 1, 'year': 1, 'week': 1}
        kwargs['filter'] = {'borough_id' : borough_id}  # 过滤条件
        kwargs['sort'] = [("date", -1)]
        return self.dao.find_page(*args, **kwargs)

    def get_new_price(self, *args, **kwargs):
        borough_id = kwargs['borough_id']
        kwargs['page'] = {'index': 1, 'size': 1}
        kwargs['sort'] = [('date', -1)]
        kwargs['field'] = {'_id': 0, 'avg_price': 1, 'date': 1}  # 返回字段
        kwargs['filter'] = {'borough_id' : borough_id}
        return self.dao.find_page(*args, **kwargs)

    @Pcache(conf_name="sell_api", key="borough_id")
    def getBoroughDayPrice(self, *args, **kwargs):
        result = {}
        data = {}
        city = kwargs.get('city')
        borough_id = kwargs.get('borough_id')
        day_price_data = self.get_page(city=city, borough_id=borough_id)
        if day_price_data:
            nowPrice = day_price_data[0]['borough_price']
        else:
            nowPrice = 0
        data['nowPrice'] = nowPrice
        month_price_data = self.borough_month_price_service.get_new_price(city=city, borough_id=borough_id)

        if month_price_data:
            prePrice = month_price_data[0]['avg_price']
            data['borough_name'] = month_price_data[0]['borough_name']
        else:
            prePrice = 0
        data['prePrice'] = prePrice
        data['percent'] = self.borough_month_price_service.get_percent(nowPrice=nowPrice, prePrice=prePrice)
        data['type'] = self.borough_month_price_service.get_type(nowPrice=nowPrice, prePrice=prePrice)
        data['lowPrice'] = int(nowPrice*0.8)
        data['highPrice'] = int(nowPrice*1.2)
        data['borough_id'] = borough_id
        result.setdefault('data', data)
        return result
