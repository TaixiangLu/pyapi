#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.Cityarea2MonthPriceDao import Cityarea2MonthPriceDao


class Cityarea2MonthPriceService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = Cityarea2MonthPriceDao(*args, **kwargs)

    def get_page(self, *args, **kwargs):
        filter = kwargs.get('filter')
        cityarea2_id = filter.get('id')  # 起始时间
        startMonth = filter.get('startMonth')  # 起始时间
        endMonth = filter.get('endMonth')  # 结束时间
        kwargs['cityarea2_id'] = cityarea2_id
        kwargs['field'] = {'_id': 0, 'time': 0, 'date': 0, 'cityarea2_name': 0, 'cityarea2_id': 0}
        kwargs['filter'] = {'yymm': {'$gte': startMonth, '$lte': endMonth}, 'cityarea2_id': cityarea2_id}  # 过滤条件
        kwargs['cache_key'] = str(cityarea2_id) + str(startMonth) + str(endMonth)  # 缓存小key
        return self.dao.getCityarea2MonthPrice(*args, **kwargs)

    def get_count(self, *args, **kwargs):
        filter = kwargs.get('filter')
        cityarea2_id = filter.get('id')  # 起始时间
        startMonth = filter.get('startMonth')  # 起始时间
        endMonth = filter.get('endMonth')  # 结束时间
        kwargs['filter'] = {'yymm': {'$gte': startMonth, '$lte': endMonth}, 'cityarea2_id': cityarea2_id}  # 过滤条件
        return self.dao.find_count(*args, **kwargs)

    def getCityarea2MonthPrice(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        sort = pms.get('sort', {})
        sort = [('yymm', 1)] if sort.get('yymm', 1) == 1 else [('yymm', -1)]
        page = pms.get('page') or {"index": 1, "size": 30}
        index = page.get('index')
        size = page.get('size')
        filter = pms.get('filter')
        total = self.get_count(city=city, filter=filter)
        data = self.get_page(city=city, sort=sort, page=page, filter=filter)
        result.setdefault('data', data)
        result.setdefault('page', {"index": index, "size": size, "total": total})
        return result
