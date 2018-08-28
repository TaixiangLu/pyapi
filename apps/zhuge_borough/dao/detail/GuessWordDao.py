#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-24 下午6:39
# @Author  : jianguo@zhugefang.com
# @Desc    : 城区数据层
from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.Guess_word import GuessWord
class GuessWordDao(BaseMongo):
    guessword_type = {1: '城区', 2: '商圈', 3: '小区', 4: '地铁', 5: '地铁站'}
    guessword_key = {1: 'cityarea_id', 2: 'cityarea2_id', 3: 'borough_id'}

    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", GuessWord), conf_name=kwargs.get("conf_name", "borough"))
