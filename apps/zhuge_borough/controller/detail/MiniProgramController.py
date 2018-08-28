#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/3/10 下午4:36
# @Author  : liuwei
from controller.BaseController import Route

route = Route()
from controller.BaseController import catch
from controller.BaseController import BaseController
from apps.zhuge_borough.service.detail.MiniProgramService import MiniProgramService
from apps.zhuge_borough.service.detail.AreaBoroughTopTenService import SortBoroughSum
from apps.zhuge_borough.service.detail.AreaDistributionService import AreaDistributionService
from apps.zhuge_borough.service.detail.PriceDistributionService import PriceDistributionService
from apps.zhuge_borough.service.detail.PriceChangeService import PriceChangeService
from apps.zhuge_borough.service.detail.UpperThanOthersService import UpperThanOthersService
from apps.zhuge_borough.service.detail.NewHouseMPService import NewHouseMPService
from apps.zhuge_borough.service.detail.AppraisalService import AppraisalService
import json, requests
from apps.zhuge_borough.util.house_predict import predict

@route("/MiniProgramHello")
class Hello(BaseController):
    """
    hello
    """

    def get(self, *args, **kwargs):
        result = {"message": "success", "code": 200}
        result["data"] = '欢迎使用诸葛找房小程序接口业务！！！'
        self.write(result)


@route("/(?P<city>\w*)/borough/detail/getPriceWave")
class PriceWaveHandle(BaseController):
    """
    弃用 20180713 --hbd
    desc： [MP接口] 城市,城区,商圈新上,涨降价接口

    """

    def initialize(self):
        self.service = PriceChangeService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city', '')
        pms = json.loads(self.request.body)
        data = self.service.run(city=city, pms=pms)
        return data


@route("/(?P<city>\w*)/borough/detail/getHousePriceDistribution")
class HousePriceDistributionHandle(BaseController):
    """
    desc： [MP接口] 城市,城区,商圈房源价格分布

    """

    def initialize(self):
        self.service = PriceDistributionService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city', '')
        pms = json.loads(self.request.body)
        data = self.service.run(city=city, pms=pms)
        return data


@route("/(?P<city>\w*)/borough/detail/getHouseAreaDistribution")
class AvgPriceHandle(BaseController):
    """
    desc： [MP接口] 城市,城区,商圈房源面积分布

    """

    def initialize(self):
        self.service = AreaDistributionService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city', '')
        pms = json.loads(self.request.body)
        data = self.service.run(city=city, pms=pms)
        return data


@route("/(?P<city>\w*)/borough/detail/getCityAvgPrice")
class CityAvgPriceHandle(BaseController):
    """
    desc： [MP接口] 城市均价接口

    """

    def initialize(self):
        self.service = MiniProgramService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city', '')
        data = self.service.city_avgprice(city=city)
        return data


@route("/(?P<city>\w*)/borough/detail/getHouseAvgPrice")
class HouseAvgPriceHandle(BaseController):
    """
    desc： [MP接口] 挂牌均价接口

    """

    def initialize(self):
        self.service = MiniProgramService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city', '')
        data = self.service.getHouseAvgPrice(city=city)
        return data


@route("/(?P<city>\w*)/borough/detail/getHousetypeDistribution")
class HousetypeDistributionHandle(BaseController):
    """
    desc： [MP接口] 户型分布
    """
    def initialize(self):
        self.service = MiniProgramService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city', '')
        pms = json.loads(self.request.body)
        data = self.service.getHousetypeDistribution(city=city, pms=pms)
        return data


@route("/(?P<city>\w*)/borough/detail/getHotBorough")
class CityareaBoroughTop(BaseController):
    """
    desc:  [MP接口] 热门小区TOP接口
    """
    def initialize(self):
        self.service = SortBoroughSum()

    @catch() # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')
        pms = json.loads(self.request.body)
        if not pms.get("cityarea_id") and not pms.get("cityarea2_id"):
            data = self.service.city_borough_top(city=city,pms=pms)
        elif pms.get("cityarea_id") and not pms.get("cityarea2_id"):
            data = self.service.cityarea_borough_top(city=city,pms=pms)
        elif not pms.get("cityarea_id") and pms.get("cityarea2_id"):
            data = self.service.cityarea2_borough_top(city=city,pms=pms)
        else:
            return None
        return data


@route("/(?P<city>\w*)/borough/detail/SellWeekChangeCnt")
class SellWeekChangeCnt(BaseController):
    """
    desc： [MP接口] 房源变动趋势接口

    """

    def initialize(self):
        self.service = MiniProgramService()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        city = kwargs.get('city')
        cityarea_id = int(self.get_argument("cityarea_id",0))
        cityarea2_id = int(self.get_argument("cityarea2_id",0))
        data = self.service.sell_week_change_cnt(city=city,cityarea_id=cityarea_id,cityarea2_id=cityarea2_id)
        return {"data": data}


@route("/(?P<city>\w*)/borough/detail/sellFinishAreaDistribution")
class SellWeekChangeCnt(BaseController):
    """
    desc： [MP接口] 二手房城市成交面积房源分布

    """

    def initialize(self):
        self.service = MiniProgramService()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        city = kwargs.get('city')
        data = self.service.sellFinishAreaDistribution(city=city)
        return {"data": data}

@route("/(?P<city>\w*)/borough/detail/sellFinishPriceDistribution")
class SellWeekChangeCnt(BaseController):
    """
    desc： [MP接口] 二手房城市成交价格房源分布

    """

    def initialize(self):
        self.service = MiniProgramService()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        city = kwargs.get('city')
        data = self.service.sellFinishPriceDistribution(city=city)
        return {"data": data}

@route("/(?P<city>\w*)/borough/detail/sellFinishCompanyDistribution")
class SellWeekChangeCnt(BaseController):
    """
    desc： [MP接口] 二手房城市成交经纪公司房源分布

    """

    def initialize(self):
        self.service = MiniProgramService()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        city = kwargs.get('city')
        data = self.service.sellFinishCompanyDistribution(city=city)
        return {"data": data}

@route("/(?P<city>\w*)/borough/detail/sellFinishCityareaDistribution")
class SellWeekChangeCnt(BaseController):
    """
    desc： [MP接口] 二手房城市成交城区房源分布

    """

    def initialize(self):
        self.service = MiniProgramService()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        city = kwargs.get('city')
        data = self.service.sellFinishCityareaDistribution(city=city)
        return {"data": data}

@route("/borough/detail/upperThanOthers")
class HowMuchYouUpper(BaseController):
    """
    desc: [MP接口] 获取高出全国居民房产价值多少

    """

    def initialize(self):
        self.service = UpperThanOthersService()

    @catch()
    def post(self, *args, **kwargs):
        pms = json.loads(self.request.body)
        data = self.service.upper_than_percent(pms=pms)
        return {"data": data}


# ==============新房成交小程序接口(暂时放在这里后期迁到新房)======

@route("/(?P<city>\w*)/newhouse/detail/getFinishTrend")
class FinishTrendHandle(BaseController):
    """
    desc： [MP接口] 1.获取新房首页城市走势（成交均价&成交量）

    """

    def initialize(self):
        self.service = NewHouseMPService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city', '')
        pms = json.loads(self.request.body)
        data = self.service.get_finish_trend(city=city, pms=pms)
        return data


@route("/(?P<city>\w*)/newhouse/detail/getFinishTop5")
class FinishTop5Handle(BaseController):
    """
    desc： [MP接口] 1.获取成交楼盘的TOP5的数据列表

    """

    def initialize(self):
        self.service = NewHouseMPService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city', '')
        data = self.service.finish_top5(city=city)
        return data


@route("/(?P<city>\w*)/newhouse/detail/getHousetypeDistribution")
class HousetypeDistributionHandle(BaseController):
    """
    desc： [MP接口] 1.获取新房成交楼盘户型分布

    """

    def initialize(self):
        self.service = NewHouseMPService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city', '')
        pms = json.loads(self.request.body)
        data = self.service.housetype_distribution(city=city, pms=pms)
        return data


@route("/(?P<city>\w*)/newhouse/detail/getPriceDistribution")
class PriceDistributionHandle(BaseController):
    """
    desc： [MP接口] 1.获取新房成交楼盘价格分布

    """

    def initialize(self):
        self.service = NewHouseMPService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city', '')
        pms = json.loads(self.request.body)
        data = self.service.price_distribution(city=city, pms=pms)
        return data


@route("/(?P<city>\w*)/borough/detail/getGuessword")
class GuesswordHandle(BaseController):
    """
    desc： [MP接口] 1.估价小区楼盘联想词接口

    """

    def initialize(self):
        self.service = AppraisalService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city', '')
        pms = json.loads(self.request.body)
        data = self.service.get_borough_name(city=city, pms=pms)
        return data


@route("/(?P<city>\w*)/borough/detail/getAppraisalLevel")
class AppraisalLevelHandle(BaseController):
    """
    desc： [MP接口] 1.获取估价等级

    """

    def initialize(self):
        self.service = AppraisalService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city', '')
        pms = json.loads(self.request.body)
        data = self.service.appraisal_level(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/predictHouse')
class CityareaListDetailHandle(BaseController):
    '''
    估价算法接口:
    @:param city:城市 {bj}
    @:param filter:查询条件 {name:链家}
    @:param sort:排序条件 {1:正序, -1:倒序}
    @:param page:页码条数 {index:页码, size:条数}
    @:param field:

    @:return code    状态码
    @:return runtime 运行时间
    @:return total   记录总数
    @:return data    数据

    '''

    def initialize(self):
        pass

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        aimhouse = {
            "borough_id" : int(pms.get("borough_id",0)),
            "house_fitment" : str(pms.get("house_fitment",'')),
            "house_floor" : int(pms.get("house_floor",0)),
            "house_hall" : int(pms.get("house_hall",0)),
            "house_price" : 1,
            "house_room" : int(pms.get("house_room",0)),
            'house_topfloor': int(pms.get("house_topfloor",0)),  # 最高
            'house_totalarea': str(pms.get("house_totalarea",'')),#面积
            'house_toward': str(pms.get("house_toward",'')),#朝向
            'house_type': int(pms.get("house_type",1)),#默认住宅
            'id': 0,
            "property_right_years": str(pms.get("property_right_years",""))# 产权默认为空70年
        }
        borough_id = pms.get("borough_id")
        cityarea_id = pms.get("cityarea_id",0)
        cityarea2_id = pms.get("cityarea2_id",0)
        # 通过小区id获取城区商圈id
        result =  predict(city, borough_id, cityarea_id,cityarea2_id, aimhouse)
        res = round(result["avg_price"],2)
        type = result.get("type",6)
        # totalPrice = int(pms.get("house_totalarea",0))*int(res) / 10000
        data = {"data":res,"type":type}
        return data


@route('/(?P<city>\w*)/borough/detail/predictHouseTest')
class CityareaListDetailHandle(BaseController):
    '''
    估价算法接口:
    @:param city:城市 {bj}
    @:param filter:查询条件 {name:链家}
    @:param sort:排序条件 {1:正序, -1:倒序}
    @:param page:页码条数 {index:页码, size:条数}
    @:param field:

    @:return code    状态码
    @:return runtime 运行时间
    @:return total   记录总数
    @:return data    数据

    '''

    def initialize(self):
        pass

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        aimhouse = {
            "borough_id" : int(pms.get("borough_id",0)),
            "house_fitment" : str(pms.get("house_fitment",'')),
            "house_floor" : int(pms.get("house_floor",0)),
            "house_hall" : int(pms.get("house_hall",0)),
            "house_price" : 1,
            "house_room" : int(pms.get("house_room",0)),
            'house_topfloor': int(pms.get("house_topfloor",0)),  # 最高
            'house_totalarea': str(pms.get("house_totalarea",'')),#面积
            'house_toward': str(pms.get("house_toward",'')),#朝向
            'house_type': int(pms.get("house_type",1)),#默认住宅
            'id': 0,
            "property_right_years": str(pms.get("property_right_years",""))# 产权默认为空70年
        }
        borough_id = pms.get("borough_id")
        cityarea_id = pms.get("cityarea_id",0)
        cityarea2_id = pms.get("cityarea2_id",0)
        # 通过小区id获取城区商圈id
        result =  predict(city, borough_id, cityarea_id,cityarea2_id, aimhouse)
        res = round(result["avg_price"],2)
        type = result.get("type",6)
        # totalPrice = int(pms.get("house_totalarea",0))*int(res) / 10000
        data = {"avg_price":res,"type":type}
        return {"data":data}