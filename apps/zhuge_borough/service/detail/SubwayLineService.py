#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.SubwayLineDao import SubwayLineDao
class SubwayLineService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = SubwayLineDao(*args, **kwargs)

    def getSubwayList(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        sort = pms.get('sort')
        #处理sort
        sort = [('_id', 1)] if sort.get('id', 1) == 1 else [('_id', -1)]
        page = pms.get('page')
        index = page.get('index')
        size = page.get('size')
        filter = pms.get('filter', {})
        field = {'_id': 1, 'name': 1, 'loc': 1, 'area_pinyin': 1}  # 返回字段
        total = self.dao.find_count(city=city, pms=pms)
        data = self.dao.getCityareaListByCity(city=city, filter=filter, page=page, sort=sort, field=field)
        for cityarea_info in data:
            cityarea_id = cityarea_info.get('_id')
            cityarea2List = self.cityarea2_dao.getCityarea2InfoByCityareaId(city=city, cityarea_id=cityarea_id)
            cityarea_info['cityarea2List'] = cityarea2List
        result.setdefault('data', data)
        result.setdefault('page', {"index": index, "size": size, "total": total})
        return result

