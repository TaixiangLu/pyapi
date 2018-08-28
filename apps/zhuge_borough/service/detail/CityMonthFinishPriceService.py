#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.CityMonthFinishPriceDao import CityMonthFinishPriceDao


class CityMonthFinishPriceService(BaseMgoService):
    def __init__(self):
        self.dao = CityMonthFinishPriceDao()

    def get_city_month_finish_price(self, *args, **kwargs):
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        query_time = pms.get('query_time', [])

        data = dict()
        res = dict()
        for now_time in query_time:
            result = self.dao.find_all(city=city, field=['price'], filter={'date': now_time})
            if result:
                res.setdefault('finish_price', result[0]['price'])
                data[str(now_time)] = res
        return {'data': data}
