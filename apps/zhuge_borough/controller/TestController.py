#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :

from controller.BaseController import Route

route = Route()
from controller.BaseController import catch
from controller.BaseController import BaseController
import json


@route('/hello')
class hello(BaseController):
    # @Time    : 18-4-14 下午2:30
    # @Author  : jianguo@zhugefang.com
    # @Desc    : 查询经纪公司列表
    # @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        self.write("ok")
        # return {"data": "欢迎使用诸葛找房小区基础数据api服务!!!"}
