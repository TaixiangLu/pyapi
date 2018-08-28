#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.CityareaDao import CityareaDao
from apps.zhuge_borough.dao.detail.Cityarea2Dao import Cityarea2Dao
class CityareaService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = CityareaDao(*args, **kwargs)
        self.cityarea2_dao = Cityarea2Dao(*args, **kwargs)

    # 根据指定的城区id查询数据
    def getcityarea_data(self, city, cityarea_id, field):
        filter = {'_id': cityarea_id}
        data = self.get_page(city=city, field=field, filter=filter)
        return data

    def update_filter(self, city, filter, datas):
        data = self.update_by_filter(city=city, filter=filter, datas=datas)
        return data

    def find(self, city, filter):
        data = self.get_one(city=city, filter=filter)
        return data

    def getCityareaList(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        # sort = pms.get('sort')
        # sort = [('_id', 1)] if sort.get('id', 1) == 1 else [('_id', -1)]
        sort = [('_id', 1)]
        page = pms.get('page')
        index = page.get('index', "")
        size = page.get('size', "")
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

    def getCityareaInfoById(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        cityarea_id = kwargs.get('id')
        filter = {'_id': cityarea_id}
        field = {'_id': 1, 'name': 1, 'loc': 1, 'area_pinyin': 1}  # 返回字段
        cityarea_info = self.dao.getCityareaInfoById(city=city, field=field, filter=filter,_id=cityarea_id)[0]
        cityarea_id = cityarea_info.get('_id')
        cityarea2List = self.cityarea2_dao.getCityarea2InfoByCityareaId(city=city, cityarea_id=cityarea_id)
        cityarea_info['cityarea2List'] = cityarea2List
        result.setdefault('data', cityarea_info)
        return result

    def getCityareaInfoNo2ById(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        cityarea_id = kwargs.get('id')
        filter = {'_id': cityarea_id}
        field = {'_id': 1, 'name': 1, 'loc': 1, 'area_pinyin': 1}  # 返回字段
        cityarea_info = self.dao.getCityareaInfoById(city=city, field=field, filter=filter,_id=cityarea_id)[0]
        result.setdefault('data', cityarea_info)
        return result
