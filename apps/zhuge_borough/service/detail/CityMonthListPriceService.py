#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.CityMonthListPriceDao import CityMonthListPriceDao
class CityMonthListPriceService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = CityMonthListPriceDao(*args, **kwargs)

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

    def get_month_price(self, *args, **kwargs):
        filter = kwargs.get('filter')
        startMonth = filter.get('startMonth')  # 起始时间
        endMonth = filter.get('endMonth')  # 结束时间
        kwargs['filter'] = {"date" : {'$gte' : startMonth, '$lte' : endMonth}}  # 过滤条件
        return self.dao.find_page(*args, **kwargs)

    def getCityMonthPrice(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        sort = pms.get('sort',{})
        sort = [('yymm', 1)] if sort.get('yymm', 1) == 1 else [('yymm', -1)]
        page = pms.get('page')
        index = page.get('index')
        size = page.get('size')
        field = {'_id': 0, 'price': 1, 'date': 1, }  # 返回字段
        filter = pms.get('filter')
        startMonth = filter.get('startMonth')  # 起始时间
        endMonth = filter.get('endMonth')  # 结束时间
        filter = {"date" : {'$gte' : startMonth, '$lte' : endMonth}}  # 过滤条件
        total = self.dao.find_count(city=city, filter=filter)
        data = self.dao.getCityMonthPrice(city=city, sort=sort, page=page, field=field, filter=filter)
        result.setdefault('data', data)
        result.setdefault('page', {"index": index, "size": size, "total": total})
        return result
