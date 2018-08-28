#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.CityWeekPriceDao import CityWeekPriceDao
class CityWeekPriceService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = CityWeekPriceDao(*args, **kwargs)

    def getCityWeekPrice(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        sort = pms.get('sort',{})
        sort = [('lastday', 1)] if sort.get('lastday', 1) == 1 else [('lastday', -1)]
        page = pms.get('page')
        index = page.get('index')
        size = page.get('size')
        field = {'_id': 0, 'week': 1, 'price': 1, 'year': 1, 'firstday': 1, 'lastday': 1}  # 返回字段
        startDate = pms.get('filter').get('startDate')  # 起始时间
        endDate = pms.get('filter').get('endDate')  # 结束时间
        filter = {"firstday" : {'$gte' : startDate},"lastday" : {'$lte' : endDate}}  # 过滤条件
        total = self.dao.find_count(city=city, filter=filter)
        data = self.dao.getCityWeekPrice(city=city, sort=sort, page=page, field=field, filter=filter)
        result.setdefault('data', data)
        result.setdefault('page', {"index": index, "size": size, "total": total})
        return result



