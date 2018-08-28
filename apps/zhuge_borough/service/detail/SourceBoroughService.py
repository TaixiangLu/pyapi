#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2018/5/11 上午11:53
# @Author  : Sunbowen
# @Email   : sunbowen@zhugefang.com
# @File    : SourceBoroughService.py
# @Software: PyCharm
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.SourceBoroughDao import AnjukeBoroughDao, FangBoroughDao, Five8BoroughDao, \
    KufangBoroughDao, LianjiaBoroughDao, MaitianBoroughDao, TuituiBoroughDao, WiwjBoroughDao, XingshandichanBoroughDao, \
    ZhongyuanBoroughDao


class SourceBoroughService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.anjukeBoroughDao = AnjukeBoroughDao(*args, **kwargs)
        self.fangBoroughDao = FangBoroughDao(*args, **kwargs)
        self.five8BoroughDao = Five8BoroughDao(*args, **kwargs)
        self.kufangBoroughDao = KufangBoroughDao(*args, **kwargs)
        self.lianjiaBoroughDao = LianjiaBoroughDao(*args, **kwargs)
        self.maitianBoroughDao = MaitianBoroughDao(*args, **kwargs)
        self.tuituiBoroughDao = TuituiBoroughDao(*args, **kwargs)
        self.wiwjBoroughDao = WiwjBoroughDao(*args, **kwargs)
        self.xingshandichanBoroughDao = XingshandichanBoroughDao(*args, **kwargs)
        self.zhongyuanBoroughDao = ZhongyuanBoroughDao(*args, **kwargs)

    def getSourceBoroughList(self, *args, **kwargs):
        result = {}
        data = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        filter = pms.get('filter', {})
        field = {'_id': 0, 'borough_name': 1, 'borough_info': 1, 'cityarea': 1, 'loc': 1, 'borough_price': 1, 'pics': 1}
        data['anjuke'] = self.anjukeBoroughDao.find_page(city=city, filter=filter, field=field)
        data['fang'] = self.fangBoroughDao.find_page(city=city, filter=filter, field=field)
        data['five8'] = self.five8BoroughDao.find_page(city=city, filter=filter, field=field)
        data['kufang'] = self.kufangBoroughDao.find_page(city=city, filter=filter, field=field)
        data['lianjia'] = self.lianjiaBoroughDao.find_page(city=city, filter=filter, field=field)
        data['maitian'] = self.maitianBoroughDao.find_page(city=city, filter=filter, field=field)
        data['tuitui'] = self.tuituiBoroughDao.find_page(city=city, filter=filter, field=field)
        data['wiwj'] = self.wiwjBoroughDao.find_page(city=city, filter=filter, field=field)
        data['xingshang'] = self.xingshandichanBoroughDao.find_page(city=city, filter=filter, field=field)
        data['zhongyuan'] = self.zhongyuanBoroughDao.find_page(city=city, filter=filter, field=field)
        result.setdefault('data', data)
        return result
