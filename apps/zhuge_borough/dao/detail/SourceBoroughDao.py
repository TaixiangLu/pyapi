#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2018/5/11 上午11:56
# @Author  : Sunbowen
# @Email   : sunbowen@zhugefang.com
# @File    : SourceBoroughDao.py
# @Software: PyCharm
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.Source_borough import Anjuke_borough, Fang_borough, Five8_borough, Kufang_borough, \
    Lianjia_borough, Maitian_borough, Tuitui_borough, Wiwj_borough, Xingshandichan_borough, Zhongyuan_borough


class AnjukeBoroughDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Anjuke_borough), conf_name=kwargs.get("conf_name", "borough"))


class FangBoroughDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Fang_borough), conf_name=kwargs.get("conf_name", "borough"))


class Five8BoroughDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Five8_borough), conf_name=kwargs.get("conf_name", "borough"))


class KufangBoroughDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Kufang_borough), conf_name=kwargs.get("conf_name", "borough"))


class LianjiaBoroughDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Lianjia_borough), conf_name=kwargs.get("conf_name", "borough"))


class MaitianBoroughDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Maitian_borough), conf_name=kwargs.get("conf_name", "borough"))


class TuituiBoroughDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Tuitui_borough), conf_name=kwargs.get("conf_name", "borough"))


class WiwjBoroughDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Wiwj_borough), conf_name=kwargs.get("conf_name", "borough"))


class XingshandichanBoroughDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Xingshandichan_borough),
                         conf_name=kwargs.get("conf_name", "borough"))


class ZhongyuanBoroughDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Zhongyuan_borough), conf_name=kwargs.get("conf_name", "borough"))
