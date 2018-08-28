#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :

from controller.BaseController import Route

route = Route()
from controller.BaseController import catch
from apps.zhuge_borough.service.detail.BoroughOnlineService import BoroughOnlineService
from apps.zhuge_borough.service.detail.CityareaService import CityareaService
from apps.zhuge_borough.service.detail.Cityarea2Service import Cityarea2Service
from apps.zhuge_borough.service.detail.SubwayLineService import SubwayLineService
from apps.zhuge_borough.service.detail.CityWeekPriceService import CityWeekPriceService
from apps.zhuge_borough.service.detail.CityMonthListPriceService import CityMonthListPriceService
from apps.zhuge_borough.service.detail.CityMonthFinishPriceService import CityMonthFinishPriceService
from apps.zhuge_borough.service.detail.CityDayPriceService import CityDayPriceService
from apps.zhuge_borough.service.detail.BoroughMonthPriceService import BoroughMonthPriceService
from apps.zhuge_borough.service.detail.CityareaMonthPriceService import CityareaMonthPriceService
from apps.zhuge_borough.service.detail.Cityarea2MonthPriceService import Cityarea2MonthPriceService
from apps.zhuge_borough.service.detail.BoroughPriceService import BoroughPriceService
from apps.zhuge_borough.service.detail.CommunityService import CommunityService
from apps.zhuge_borough.service.detail.SourceBoroughService import SourceBoroughService
from controller.BaseController import BaseController
import json


@route('/hello')
class hello(BaseController):
    # @Time    : 18-4-14 下午2:30
    # @Author  : jianguo@zhugefang.com
    # @Desc    : 查询经纪公司列表
    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        return {"data": "欢迎使用诸葛找房小区基础数据api服务!!!"}


@route('/(?P<city>\w*)/borough/detail/cityareaList')
class CityareaListDetailHandle(BaseController):
    '''
    获取城区列表:
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
        self.service = CityareaService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.service.getCityareaList(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/cityarea2List')
class Cityarea2ListDetailHandle(BaseController):
    '''
    获取商圈列表
    @:param city:城市 {bj}
    @:param filter   :查询条件 {name:链家}
    @:param sort:排序条件 {1:正序, -1:倒叙}
    @:param page:页码条数 {index:页码, size:条数}
    @:param field:

    @:return code
    @:return runtime
    @:return total
    @:return data
    '''

    def initialize(self):
        self.service = Cityarea2Service()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.service.getCityarea2List(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/cityarea')
class CityareaDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param id:城区id {id:1}
    获取城区详情
    '''

    def initialize(self):
        self.service = CityareaService()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        id = int(self.get_argument("id"))
        data = self.service.getCityareaInfoById(city=city, id=id)
        return data


@route('/(?P<city>\w*)/borough/detail/cityarea2')
class Cityarea2DetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param id:商圈id {id:1}
    获取商圈详情
    '''

    def initialize(self):
        self.service = Cityarea2Service()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        id = int(self.get_argument("id"))
        data = self.service.getCityarea2InfoById(city=city, id=id)
        return data


@route('/(?P<city>\w*)/borough/detail/subwayList')
class SubwayListDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param filter:查询条件 {name:线路}
    @:param sort:排序条件 {1:正序, -1:倒叙}
    @:param page:页码条数 {index:页码, size:条数}
    获取地铁列表
    '''

    def initialize(self):
        self.service = SubwayLineService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.service.getSubwayList(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/cityWeekPrice')
class CityWeekPriceDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param filter:查询条件 {startMonth:201701, endMonth:结束月份}
    @:param sort:排序条件 {lastday:1}
    @:param page:页码条数 {index:页码, size:条数}

    获取城市周均价列表
    '''

    def initialize(self):
        self.service = CityWeekPriceService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.service.getCityWeekPrice(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/cityMonthPrice')
class CityMonthPriceDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param filter:查询条件 {startMonth:201701, endMonth:结束月份}
    @:param sort:排序条件 {yymm:1}
    @:param page:页码条数 {index:页码, size:条数}

    获取城市月均价列表
    '''

    def initialize(self):
        self.service = CityMonthListPriceService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.service.getCityMonthPrice(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/cityNowPrice')
class CityNowPriceDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    获取城市当前均价
    '''

    def initialize(self):
        self.day_price_service = CityDayPriceService()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        data = self.day_price_service.getCityDayPrice(city=city)
        return data


@route('/(?P<city>\w*)/borough/detail/boroughMonthPrice')
class BoroughMonthPriceDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param filter:查询条件 {startMonth:201701, endMonth:结束月份}
    @:param sort:排序条件 {yymm:1}
    @:param page:页码条数 {index:页码, size:条数}

    获取小区月均价列表
    '''

    def initialize(self):
        self.service = BoroughMonthPriceService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.service.getBoroughMonthPrice(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/cityareaMonthPrice')
class CityareaMonthPriceDetailHandle(BaseController):
    '''
    @:param city:城市
    @:param filter:查询条件 {startMonth:201701, endMonth:结束月份}
    @:param sort:排序条件 {yymm:1}
    @:param page:页码条数 {index:页码, size:条数}
    获取城区月均价列表
    '''

    def initialize(self):
        self.service = CityareaMonthPriceService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.service.getCityareaMonthPrice(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/cityarea2MonthPrice')
class Cityarea2MonthPriceDetailHandle(BaseController):
    '''
    @:param city:城市
    @:param filter:查询条件 {startMonth:201701, endMonth:结束月份}
    @:param sort:排序条件 {yymm:1}
    @:param page:页码条数 {index:页码, size:条数}
    获取商圈月均价列表
    '''

    def initialize(self):
        self.service = Cityarea2MonthPriceService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.service.getCityarea2MonthPrice(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/boroughNowPrice')
class BoroughNowPriceDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param id:小区id {id:1004567}
    获取小区当前均价
    '''

    def initialize(self):
        self.borough_price_service = BoroughPriceService()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        borough_id = int(self.get_argument("id"))
        print(borough_id)
        data = self.borough_price_service.getBoroughDayPrice(city=city, borough_id=borough_id)
        return data

@route('/(?P<city>\w*)/borough/detail/boroughPriceByIds')
class BoroughNowPriceDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param id:小区id {id:1004567}
    获取小区当前均价
    '''

    def initialize(self):
        self.borough_price_service = BoroughPriceService()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        result = {}
        data = []
        city = kwargs.get('city')  # 城市简拼
        borough_ids = self.get_argument("borough_id").split(",")
        for i in borough_ids:
            data.append(self.borough_price_service.getBoroughDayPrice(city=city, borough_id=int(i)).get("data"))
        result["data"] = data
        return result

@route('/(?P<city>\w*)/borough/detail/getBoroughData')
class BoroughDataDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param id:小区id {id:1004567}
    获取小区当前均价
    '''

    def initialize(self):
        self.service = BoroughOnlineService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.service.getBoroughData(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/threeLinePrice')
class ThreeLinePriceDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param id:小区id {id:1004567}
    获取小区当前均价
    '''

    def initialize(self):
        self.service = BoroughOnlineService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.service.getThreeLinePrice(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/boroughDetail')
class BoroughNowPriceDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param id:小区id {id:1004567}
    获取小区当前均价
    '''

    def initialize(self):
        self.service = BoroughOnlineService()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        borough_id = int(self.get_argument("id"))
        data = self.service.getBoroughDetail(city=city, borough_id=borough_id)
        return data

@route('/(?P<city>\w*)/borough/detail/getCityareaName')
class BoroughNowPriceDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param id:城区id {id:"1,2,3"}
    获取城区名称
    '''

    def initialize(self):
        self.service = CityareaService()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        cityarea_ids = self.get_argument("cityarea_id").split(",")
        data = []
        for i in cityarea_ids:
            data.append(self.service.getCityareaInfoNo2ById(city=city, id=int(i)).get("data"))
        result = {"data": data}
        return result

@route('/(?P<city>\w*)/borough/detail/getCityarea2Name')
class BoroughNowPriceDetailHandle(BaseController):
    '''
    @:param city:城市 {bj}
    @:param id:商圈ids {id:"1,2,3"}
    获取商圈名称
    '''

    def initialize(self):
        self.service = Cityarea2Service()

    @catch()  # 异常装饰器
    def get(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        cityarea2_ids = self.get_argument("cityarea2_id").split(",")
        data = []
        for i in cityarea2_ids:
            data+=self.service.getCityarea2InfoById(city=city,id=int(i)).get("data")
        result = {"data":data}
        return result


@route('/(?P<city>\w*)/borough/detail/getNearbyBorough')
class NearbyBoroughDetailHandle(BaseController):
    '''
    获取附近小区
    @:user 微信小程序, 京东
    '''

    def initialize(self):
        self.service = BoroughOnlineService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')
        pms = json.loads(self.request.body)
        result = self.service.get_nearby_borough(city=city, pms=pms)
        return result


@route('/(?P<city>\w*)/borough/detail/getNearbyBoroughByLoc')
class NearbyBoroughDetailByLocHandle(BaseController):
    '''
    获取附近小区
    @:user 微信小程序, 京东
    '''

    def initialize(self):
        self.service = BoroughOnlineService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')
        pms = json.loads(self.request.body)
        result = self.service.get_nearby_borough_by_loc(city=city, pms=pms)
        return result


@route('/(?P<city>\w*)/borough/detail/getCityMonthFinishPrice')
class CityMonthFinishPriceHandle(BaseController):
    '''
    城市月成交均价
    @:user
    '''

    def initialize(self):
        self.service = CityMonthFinishPriceService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')
        pms = json.loads(self.request.body)
        result = self.service.get_city_month_finish_price(city=city, pms=pms)
        return result


# ====================================zhuge_dm====================================

@route('/(?P<city>\w*)/borough/detail/communityList')
class CommunityListDetailHandle(BaseController):
    '''
    获取社区列表:
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
        self.communityService = CommunityService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.communityService.get_community_list(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/modifyBoroughInCommunity')
class CommunityListDetailHandle(BaseController):
    '''
    社区增减小区:
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
        self.boroughOnlineService = BoroughOnlineService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.boroughOnlineService.modifyBoroughInCommunity(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/createCommunity')
class CommunityListDetailHandle(BaseController):
    '''
    新建社区:
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
        self.communityService = CommunityService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.communityService.create_community(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/deleteCommunity')
class CommunityListDetailHandle(BaseController):
    '''
    删除社区:
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
        self.communityService = CommunityService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.communityService.deleteCommunity(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/modifyCommunityName')
class CommunityListDetailHandle(BaseController):
    '''
    社区改名:
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
        self.communityService = CommunityService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.communityService.modifyCommunityName(city=city, pms=pms)
        return data

@route('/borough/cache/query')
class BoroughCacheQueryHandle(BaseController):
    '''

    @:param city_en:城市 {beijing}
    @:param name:查询条件 {芳园南里}

    @:return code    状态码
    @:return runtime 运行时间
    @:return total   记录总数
    @:return data    数据

    '''

    def initialize(self):
        self.boroughOnlineService = BoroughOnlineService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        pms = json.loads(self.request.body.decode('utf-8'))
        data = self.boroughOnlineService.getBoroughCacheByName(pms=pms)
        return data

# ====================================zhuge_dm====================================

# ====================================zhuge_ccmag====================================
@route('/(?P<city>\w*)/borough/detail/boroughList')
class BoroughListDetailHandle(BaseController):
    '''
    获取城区列表:
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
        self.service = BoroughOnlineService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.service.getBoroughList(city=city, pms=pms)
        return data


@route('/(?P<city>\w*)/borough/detail/sourceBoroughList')
class BoroughListDetailHandle(BaseController):
    '''
    获取城区列表:
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
        self.service = SourceBoroughService()

    @catch()  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get('city')  # 城市简拼
        pms = json.loads(self.request.body)
        data = self.service.getSourceBoroughList(city=city, pms=pms)
        return data

# ====================================zhuge_ccmag====================================
