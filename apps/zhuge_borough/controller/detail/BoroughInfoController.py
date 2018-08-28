#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/3/10 下午4:36
# @Author  : liuwei
from controller.BaseController import Route
route = Route()
from controller.BaseController import catch, catch_exception
from controller.BaseController import BaseController
from apps.zhuge_borough.service.detail.BoroughOnlineService import BoroughOnlineService
from apps.zhuge_borough.service.detail.CityareaService import CityareaService
from apps.zhuge_borough.service.detail.Cityarea2Service import Cityarea2Service
from apps.zhuge_borough.service.search.BoroughSearchService import BoroughSearchService
from utils.BaseUtils import BaseUtils
import time
import json
import logging
@route('/Borough/Api/mod/borough/cityarea')
class ModBoroughCityareaHandle(BaseController):
    '''
    修改小区的城区商圈:
    '''
    def initialize(self):
        self.result = {"message": "success", "code": 200}
        self.service = BoroughOnlineService()
        self.cityarea_service = CityareaService()
        self.cityarea2_service = Cityarea2Service()

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    @catch_exception  # 异常装饰器
    def post(self, *args, **kwargs):
        master_info = {}
        current_time = BaseUtils.getIntTime()
        data = json.loads(self.request.body.decode('utf-8'))
        city = data.get('city', '')
        cityarea_id = data.get('cityarea_id', '')
        cityarea2_id = data.get('cityarea2_id', '')
        borough_id = data.get('borough_id', '')
        field = {'_id': 1, 'borough_name': 1, "cityarea": 1}
        filter = {'_id': borough_id}
        borough_data = self.service.get_page(city=city, field=field, filter=filter)
        if len(borough_data) == 1:
            cityarea_field = {'_id': 1, 'name': 1, 'area_pinyin': 1}
            cityarea2_field = {'_id': 1, 'name': 1, 'area2_pinyin': 1}
            cityarea_data = self.cityarea_service.get_page(city=city, filter={'_id': cityarea_id}, field=cityarea_field)
            cityarea2_data = self.cityarea2_service.get_page(city=city, filter={'_id': cityarea2_id}, field=cityarea2_field)
            cityarea = borough_data[0]['cityarea']
            if len(cityarea_data) == 1:
                cityarea_info = cityarea_data[0]
                new_cityarea ={
                    "cityarea_name": cityarea_info.get("name",""),
                    "cityarea_id": cityarea_info.get("_id",""),
                    "cityarea_py": cityarea_info.get("area_pinyin","")
                }
                cityarea['cityarea'] = new_cityarea
            else:
                cityarea['cityarea'] = cityarea['cityarea']
            if len(cityarea2_data) == 1:
                cityarea2_info = cityarea2_data[0]
                new_cityarea2 = [
                    {
                        "cityarea2_name": cityarea2_info.get("name",""),
                        "cityarea2_id": cityarea2_info.get("_id",""),
                        "cityarea2_py": cityarea2_info.get("area2_pinyin","")
                    }
                ]
                cityarea['cityarea2'] = new_cityarea2
            else:
                cityarea['cityarea2'] = cityarea['cityarea2']
            datas = {"$set": {"cityarea": cityarea, "borough_ctype": 8, "updated": current_time}}
            borough_update_action = self.service.update_by_filter(filter=filter, datas=datas, city=city)
            if borough_update_action['nModified'] ==1:
                # 更改小区缓存
                borough_cache_status = self.service.update_borough_cache(city=city, master={"id": borough_id})
                # 删除小区详情页缓存
                borough_detail_cache_status = self.service.delete_borough_detail_cache(borough_id=borough_id, city=city)
                # 更新小区es城区商圈
                self.service.mod_borough_list_es_cityarea(city, borough_id, cityarea)
                # 根据小区id查询es数据
                # self.service.search_es(borough_id=borough_id, city=city)
                # 更新房源的es城区商圈
                self.service.mod_house_cityarea(borough_id=borough_id, cityarea=cityarea, city=city)
                master_info.setdefault("master_info", {"status": 1, "borough_id": borough_id, "remark": "更新小区id为%s的城区商圈成功" % (borough_id)})
            else:
                master_info.setdefault("master_info", {"status": -1, "borough_id": borough_id, "remark": "更新小区id为%s的城区商圈失败" % (borough_id)})
        else:
            master_info.setdefault("master_info", {"status": -1, "borough_id": borough_id,"remark": "该小区找不到"})
        self.result['data'] = master_info
        self.write(self.result)



@route("/(?P<city>\w*)/Traffic/Api/getTraffic")
class CityTrafficHandle(BaseController):
    """
    获取地铁线信息接口
    """
    def initialize(self):
        self.BoroughInfoService = BoroughOnlineService()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def post(self, *args, **kwargs):
        pms = json.loads(self.request.body)
        city = kwargs.get('city')
        loc = {}
        loc['lng'] = pms.get("lng")
        loc['lat'] = pms.get("lat")
        self.service = BoroughOnlineService(city=city)
        data = self.BoroughInfoService.getTraffic(city=city, loc=loc)
        self.result["data"] = data
        self.write(self.result)

@route("/(?P<city>\w*)/Borough/Api/getCityarea")
class CityareaInfo(BaseController):
    def initialize(self):
        self.BoroughInfoService = BoroughOnlineService()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def post(self, *args, **kwargs):
        city = kwargs.get("city")
        pms = json.loads(self.request.body)
        loc = {}
        loc['lng'] = pms.get("lng")
        loc['lat'] = pms.get("lat")
        self.service = BoroughOnlineService(city=city)
        data = self.BoroughInfoService.getCityarea(city=city, loc=loc)
        self.result["data"] = data
        self.write(self.result)

@route('/Borough/Api/mod/borough/mastername')
class ModBoroughMasternameHandle(BaseController):
    '''
    修改主名
    '''
    def initialize(self):
        self.result = {"message": "success", "code": 200}
        self.service = BoroughOnlineService()

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            master_id = pms.get("borough_id", "")
            master_name = pms.get("master_name", "")
            data = self.service.mod_borough_mastername(borough_id=master_id, master_name=master_name, city=city)
            self.result['data'] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/checkHouse')
class CheckHouseHandle(BaseController):
    '''
    检测房源es房源数量
    '''
    def initialize(self):
        self.essearch_service = BoroughSearchService()

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            borough_id = pms.get("borough_id", "")
            data = self.essearch_service.checkHouseByBoroughId(city=city, borough_id=borough_id)
            self.result['data'] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/ChangeBoroughByName')
class ChangeBoroughbynameHandle(BaseController):
    '''
    小区合并
    '''
    def initialize(self):
        self.service = BoroughOnlineService()

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            datas = pms.get("data")
            run_type = pms.get("run_type", "online")
            updateInfo = self.service.batchUpdateBorough(city=city,datas=datas,run_type=run_type)
            self.result['data'] = updateInfo
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/move/byname/tomaster')
class MoveBynametoMasterHandle(BaseController):
    '''
    小区拆分迁移
    '''
    def initialize(self):
        self.service = BoroughOnlineService()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            master_id = pms.get("master_id", "")
            before_info = pms.get("before_info", "")
            data = self.service.move_byname_to_master(city=city, before_info=before_info,master_id=master_id)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/delete/falseBoroughName')
class DeletefalseBoroughnameHandle(BaseController):
    '''
    小区拆分别名删除
    '''
    def initialize(self):
        self.service = BoroughOnlineService()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            master_id = pms.get("master_id", "")
            slave_names = pms.get("slave_names", [])
            data = self.service.delete_false_boroughname(city=city, slave_names=slave_names, master_id=master_id)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/delete/falseAliasName')
class DeletefalseAliasnameHandle(BaseController):
    '''
    小区拆分别名删除
    '''
    def initialize(self):
        self.service = BoroughOnlineService()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            borough_id = pms.get("borough_id", "")
            alias_names = pms.get("alias_names", [])
            data = self.service.delete_borough_alias_name(city=city, borough_id=borough_id, alias_names=alias_names)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)


@route('/Borough/Api/split/create/newborough')
class CreateNewboroughHandle(BaseController):
    '''
    小区拆分新建
    '''
    def initialize(self):
        self.service = BoroughOnlineService()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.load(self.request.body.decode('utf-8'))
            city = pms.get("city")
            borough_id = pms.get("borough_id", "")
            slave_name = pms.get("slave_name", "")
            new_borough_data = pms.get("new_borough_data", "")
            data = self.service.split_create_new_borough(city=city, borough_id=borough_id, slave_name=slave_name,new_borough_data=new_borough_data)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/add/borough/alias')
class AddBoroughAliasHandle(BaseController):
    '''
    新增小区别名
    '''
    def initialize(self):
        self.result = {"message": "success", "code": 200}
        self.service = BoroughOnlineService()


    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body)
            city = pms.get("city")
            borough_id = pms.get("borough_id")
            alias_names = pms.get("alias_names")
            data = self.service.add_borough_alias_name(city=city, borough_id=borough_id, alias_names=alias_names)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/add/borough/byname')
class AddBoroughBynameHandle(BaseController):
    '''
    新增小区合并名
    '''
    def initialize(self):
        self.service = BoroughOnlineService()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            borough_id = pms.get("borough_id", "")
            salve_names = pms.get("salve_names", "")
            data = self.service.add_borough_byname(city=city, borough_id=borough_id, salve_names=salve_names)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/mod/borough/location')
class ModBoroughLocationHandle(BaseController):
    '''
    小区位置的修改
    '''
    def initialize(self):
        self.service = BoroughOnlineService()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            borough_id = pms.get("borough_id", "")
            location = pms.get("location", "")
            data = self.service.mod_borough_location(city=city, borough_id=borough_id, location=location)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/mod/borough/polygons')
class ModBoroughPolygonsHandle(BaseController):
    '''
    小区边界修改
    '''
    def initialize(self):
        self.service = BoroughOnlineService()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            borough_id = pms.get("borough_id", "")
            polygons = pms.get("polygons", "")
            data = self.service.mod_borough_polygons(city=city, borough_id=borough_id, polygons=polygons)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/create/new/borough')
class CreateNewBoroughHandle(BaseController):
    '''
    新建小区
    '''
    def initialize(self):
        self.service = BoroughOnlineService()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            new_borough_data = pms.get("new_borough_data", "")
            data = self.service.create_new_borough(city=city, data=new_borough_data)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/borough/Del')
class DelBoroughHandle(BaseController):
    '''
    删除小区
    '''
    def initialize(self):
        self.result = {"message": "success", "code": 200}
        self.service = BoroughOnlineService()

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            slave_ids = pms.get("slave_ids", [])
            data = self.service.delete_borough(city=city,slave_ids=slave_ids)
            self.result['data'] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/mod/borough/facility')
class ModBoroughFacilityHandle(BaseController):
    '''
    修改小区的配套
    '''
    def initialize(self):
        self.result = {"message": "success", "code": 200}
        self.service = BoroughOnlineService()

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            borough_id = pms.get("borough_id", "")
            borough_info = pms.get("borough_info", "")
            data = self.service.mod_borough_facility_by_id(city=city,borough_id=borough_id,borough_info=borough_info)
            self.result['data'] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/mod/boroughByname/housenum')
class GetboroughBynameHousenumHandle(BaseController):
    '''
    根据小区id返回 各小区别名量
    '''
    def initialize(self):
        self.result = {"message": "success", "code": 200}
        self.service = BoroughOnlineService()

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            borough_id = pms.get("borough_id", "")
            data = self.service.get_borough_byname_housenum(city=city,borough_id=borough_id)
            self.result['data'] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/merge/cityarea2')
class MergeCityarea2Handle(BaseController):
    '''
    合并商圈
    '''
    def initialize(self):
        self.cityarea2_service = Cityarea2Service()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get('city')
            master_cityarea = pms.get('master_cityarea',{})
            slave_cityarea = pms.get('slave_cityarea',{})
            data = self.cityarea2_service.merge_cityarea(city=city,master_cityarea=master_cityarea,slave_cityarea=slave_cityarea)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/create/new/cityarea2')
class CreateNewCityarea2Handle(BaseController):
    '''
    增加商圈
    '''
    def initialize(self):
        self.cityarea2_service = Cityarea2Service()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get('city')
            cityarea_id = pms.get("cityarea_id", "")
            new_cityarea2 = pms.get("new_cityarea2", "")
            data = self.cityarea2_service.add_cityarea2(city=city,cityarea_id=cityarea_id,new_cityarea2=new_cityarea2)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/delete/cityarea2')
class DeleteCityarea2Handle(BaseController):
    '''
    删除商圈
    '''
    def initialize(self):
        self.cityarea2_service = Cityarea2Service()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get('city')
            cityarea = pms.get("cityarea", "")
            cityarea2 = pms.get("cityarea2", "")
            data = self.cityarea2_service.delete_cityarea2(city=city,cityarea=cityarea,cityarea2=cityarea2)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/mod/cityarea2/name')
class ModCityarea2NameHandle(BaseController):
    '''
    修改商圈名称
    '''
    def initialize(self):
        self.cityarea2_service = Cityarea2Service()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get('city')
            cityarea_id = pms.get("cityarea_id", "")
            cityarea2_info = pms.get("cityarea2_info", "")
            data = self.cityarea2_service.mod_cityarea2_name(city=city,cityarea_id=cityarea_id,cityarea2_info=cityarea2_info)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/mod/cityarea2/relation')
class ModCityarea2RelationHandle(BaseController):
    '''
    修改商圈名称
    '''
    def initialize(self):
        self.cityarea2_service = Cityarea2Service()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get('city')
            before_cityarea_info = pms.get("before_cityarea_info", "")
            after_cityarea_info = pms.get("after_cityarea_info", "")
            data = self.cityarea2_service.mod_cityarea2_relation(city=city,before_cityarea_info=before_cityarea_info,after_cityarea_info=after_cityarea_info)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)

@route('/Borough/Api/getBoroughData')
class GetBoroughDataHandle(BaseController):
    '''
    获取单条小区信息
    '''
    def initialize(self):
        self.service = BoroughOnlineService()
        self.result = {"message": "success", "code": 200}

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city","")
            name = pms.get("name","")
            data = self.service.get_recent_borough_data(city=city,name=name)
            self.result["data"] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)


@route('/Borough/Api/mod/borough/level')
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
        pms = json.loads(self.request.body)
        data = self.service.modifyBoroughLevel(pms=pms)
        return data


@route('/Borough/Api/borough/reduction')
class DelBoroughHandle(BaseController):
    '''
    还原小区
    '''
    def initialize(self):
        self.result = {"message": "success", "code": 200}
        self.service = BoroughOnlineService()

    @catch_exception  # 异常装饰器
    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        try:
            pms = json.loads(self.request.body.decode('utf-8'))
            city = pms.get("city")
            slave_ids = pms.get("slave_ids", [])
            data = self.service.reduction_borough(city=city,slave_ids=slave_ids)
            self.result['data'] = data
        except Exception as e:
            logging.error(e)
        finally:
            self.write(self.result)
