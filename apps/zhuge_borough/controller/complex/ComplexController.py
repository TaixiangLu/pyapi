#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :

from controller.BaseController import Route

route = Route()
from controller.BaseController import catch
from apps.zhuge_borough.service.detail.Complex_Analysis_dayService import Complex_Analysis_dayService
from controller.BaseController import BaseController
import json


@route('/hello')
class hello(BaseController):
    # @Time    : 18-4-14 下午2:30
    # @Author  : jianguo@zhugefang.com
    # @Desc    : 查询经纪公司列表
    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        return {"data": "欢迎使用诸葛找房成交基础数据api服务!!!"}


@route('/(?P<city>\w*)/borough/complex/house_totalarea')
class CityareaListDetailHandle(BaseController):
    '''
    获取新房每天成交面积，7天均值面积天走势数据，以及成交面积和7天平均成交套数
    @:param city:城市 {bj}
    @:param filter:查询条件 {query_time:[201712, 201801]}


    @:return code    状态码
    @:return runtime 运行时间
    @:return total   记录总数
    @:return data    数据

    '''

    def initialize(self):
        self.service = Complex_Analysis_dayService()
        self.result = {"message": "success", "code": 200}


    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        print(self.request.body)
        pms = json.loads(self.request.body)
        data = self.service.get_house_total_totalarea(city=city, pms=pms)
        self.result["data"] = data
        return self.result