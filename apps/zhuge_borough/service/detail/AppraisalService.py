#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.BoroughOnlineDao import BoroughOnlineDao
from apps.zhuge_borough.dao.detail.ComplexDao import ComplexDao
from apps.zhuge_borough.dao.detail.ComplexNameDao import ComplexNameDao


class AppraisalService(object):
    def __init__(self):
        self.borough_online = BoroughOnlineDao()
        self.complex = ComplexDao()
        self.complexName = ComplexNameDao()

    def appraisal_level(self, **kwargs):
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        borough_name = pms.get('borough_name', '')
        in_sql = borough_name + '%%'
        result = self.complexName.count_model(city=city, field="complex_id, type, rank, (length(complex_name)"
                                                               f"-length('{borough_name}'))",
                                              filter=f"id<=100037 and city_en='{city}' and complex_name like '{in_sql}' order by total")
        res = dict()
        if result:
            res.setdefault("appraisal_level", result.get("rank"))
        else:
            res.setdefault("appraisal_level", 5)
        res.setdefault("house_type", result.get("type", ""))
        res.setdefault("complex_id", result.get("complex_id", 0))
        return {"data": res}

    def get_borough_name(self, **kwargs):
        # 获取城市信息和模糊查询字段
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        borough_name = pms.get('borough_name', '')
        in_sql = borough_name + '%%'
        # sql语句编写及查询
        consult_sql = f"select complex_name from all_borough_price where id<=100037 and city_en='{city}' and" \
                      f" LENGTH(complex_name)<=30 and complex_name like '{in_sql}' limit 50"
        result = self.complexName.getBoroughNameByQuery(city=city, borough_name=borough_name, sql={'sql': consult_sql})
        # 将获得的数据去重后存入res列表内
        res = list(set([items.get('complex_name') for items in result]))
        # 返回res
        return {"data": res}

if __name__ == "__main__":
    print(AppraisalService().get_borough_name(city='sh', pms={"borough_name": "一品"}))
