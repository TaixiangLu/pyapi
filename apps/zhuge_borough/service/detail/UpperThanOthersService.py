#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : wanfengpu@zhugefang.com
# @Desc    : 获取房产价格高于的百分比
import sys
sys.path.append("../../../../")
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.CitizensHousePriceDao import CitizensHousePriceDao
from decimal import Decimal


class UpperThanOthersService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.citizens_house_price = CitizensHousePriceDao(*args, **kwargs)

    def upper_than_percent(self, *args, **kwargs):
        try:
            self.house_price = kwargs.get('pms').get('house_price', 0)
            consult_sql = f"select holds from citizens_house_price where indexes <= {self.house_price}"
            result_sql = self.citizens_house_price.exe_s_sqls(city='bj', sql={"sql": consult_sql})
            result = Decimal('0.0000')
            for results in result_sql:
                result += Decimal(results.get("holds")[:-1])
            return str(result) + '%'
        except Exception as e:
            print(e)

if __name__ == '__main__':
    print(UpperThanOthersService().upper_than_percent(pms={"house_price": 55}))
