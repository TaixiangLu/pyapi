#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :

from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.service.detail.BoroughMonthPriceService import BoroughMonthPriceService
from apps.zhuge_borough.service.search.BoroughSearchService import BoroughSearchService
from apps.zhuge_newhouse.service.detail.ComplexCityareaService import ComplexCityareaService
from apps.zhuge_newhouse.service.detail.ComplexCityarea2Service import ComplexCityarea2Service
from apps.zhuge_borough.service.detail.SubwayStationService import SubwayStationService
from apps.zhuge_borough.service.detail.BoroughGuesswordService import BoroughGuesswordService
from apps.zhuge_borough.service.detail.BoroughRecycleService import BoroughRecycleService
from apps.zhuge_borough.service.detail.BoroughMaxIdService import BoroughMaxIdService
# from apps.zhuge_borough.service.detail.JjrxtXiaoquService import JjrxtXiaoquService
from apps.zhuge_borough.dao.detail.BoroughOnlineDao import BoroughOnlineDao
from apps.zhuge_borough.dao.detail.CommunityDao import CommunityDao
from apps.config.CitySouceMapping import city_mapping
from utils.BaseUtils import BaseUtils
from databases.dbfactory.dbfactory import dbfactory
from apps.config.Config import redis_conf
import time, json, math, requests


class BoroughOnlineService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = BoroughOnlineDao(*args, **kwargs)
        self.price = BoroughMonthPriceService()
        self.essearch = BoroughSearchService()
        self.complex_cityarea = ComplexCityareaService()
        self.complex_cityarea2 = ComplexCityarea2Service()
        self.subway_service = SubwayStationService()
        self.borough_recycle = BoroughRecycleService()
        # self.jjrxtXiaoquService = JjrxtXiaoquService()
        # 更新小区缓存的链接库
        self.redis_conn = dbfactory.create_db(conf_name="sell", db_type='db_redis')
        # 删除小区缓存的链接库
        self.api_redis_conn = dbfactory.create_db(conf_name="borough_api", db_type='db_redis')
        self.guessword = BoroughGuesswordService()
        self.max_id = BoroughMaxIdService()
        self.communityDao = CommunityDao()

    # 查询mongo数据
    def mod_borough_cityarea(self, city, field, filter):
        data = self.get_page(city=city, field=field, filter=filter)
        return data

    def find(self, city, filter):
        data = self.get_one(city=city, filter=filter)
        return data

    # 更新mongo数据
    def update_filter(self, filter, datas, city):
        result = self.update_by_filter(filter=filter, datas=datas, city=city)
        return result

    def update_filterall(self, filter, datas, city):
        result = self.update_by_filterall(filter=filter, datas=datas, city=city)
        return result

    def delete_id(self, city, filter):
        result = self.remove_by_id(filter=filter, city=city)
        return result

    def insert_one(self, city, datas):
        result = self.add_one(city=city, datas=datas)
        return result

    # 添加小区列表es
    def add_borough_list_es(self, borough_id, es_data, city):
        index_name = "online_" + city + "_borough"
        if city == 'bj':
            doc_name = "spider"
        else:
            doc_name = "spider_" + city
        add_borough_status = self.essearch.insert_one(index_name=index_name, doc_name=doc_name, id=borough_id,
                                                      data=es_data)
        return add_borough_status

    # 生成小区缓存数据
    def generate_cache_data(self, city, name):
        data_info = {}
        if isinstance(name,int):
            find_borough = self.dao.find_one(city=city, filter={"_id": name})
        else:
            find_borough = self.dao.find_one({"borough_byname":name})
        if not find_borough:
            data_info["status"] = 0
            data_info["message"] = "找不到该小区的数据"
            return data_info
        data_info['id'] = find_borough.get("_id",0)
        data_info['name'] = find_borough.get("borough_name","")
        data_info['loc'] = find_borough.get("loc",[])
        borough_cityarea = find_borough.get('cityarea','')
        if not borough_cityarea:
            cityarea = ''
            cityarea2 = ''
        else:
            cityarea = borough_cityarea.get('cityarea',{})
            cityarea2 = borough_cityarea.get('cityarea2',[])
        borough_subway = []
        data_info['subway'] = borough_subway
        if "community" in find_borough:
            data_info['community'] = find_borough.get('community', '')
        if "traffic" in find_borough:
            if not isinstance(find_borough['traffic'], dict):
                data_info['subway'] = borough_subway
            elif 'subway'not in  find_borough['traffic']:
                data_info['subway'] = borough_subway
            elif len(find_borough['traffic']['subway']) > 0:
                for station in find_borough['traffic']['subway']:
                    station_info = {}
                    station_info.setdefault('station_id', station.get('subway_station_id', ''))
                    station_info.setdefault('station_name', station.get('subway_station_name', ''))
                    is_distance = station.get('distance', '')
                    if not is_distance:
                        station_info.setdefault('distance', '')
                        station_info.setdefault('time', '')
                    else:
                        distance = station.get('distance').get('ride', '')
                        dis = distance['dis'] if distance else ''
                        time = station.get('distance').get('ride', '')
                        c_time = time['time'] if distance else ''
                        station_info.setdefault('distance', dis)
                        station_info.setdefault('time', c_time)
                    lines = []
                    for line in station.get('subway_list_line'):
                        lines.append(line)
                    station_info.setdefault('lines', lines)
                    borough_subway.append(station_info)
                    data_info['subway'] = borough_subway
            else:
                data_info['subway'] = borough_subway
        # cityarea_id = find_borough['cityarea']['cityarea']['cityarea_id']
        # cityarea_name = find_borough['cityarea']['cityarea']['cityarea_name']
        # cityarea_py = find_borough['cityarea']['cityarea']['cityarea_py']
        # data_info['cityarea'] = {"cityarea_id":cityarea_id,"cityarea_name":cityarea_name,"cityarea_py":cityarea_py}
        data_info['cityarea'] = cityarea
        data_info['cityarea2'] = cityarea2
        # if 'cityarea2' in find_borough['cityarea']:
        #     data_info['cityarea2'] = find_borough['cityarea']['cityarea2']
        if "borough_config" in find_borough:
            data_info['config'] = find_borough['borough_config']
        else:
            data_info['config'] = []
            data_info['price'] = self.price.getPriceByBoroughId(city=city, borough_id=data_info['id'])
        borough_byname = find_borough.get("borough_byname", [])
        borough_hide = find_borough.get("borough_hide", [])
        name_list = borough_byname + borough_hide
        data_info['name_list'] = name_list
        return data_info

    #更新单个小区缓存
    def update_borough_cache(self, city, master={}, slave={}):
        try:
            key = "%s_borough"%city_mapping.get(city)['fpy']
            if slave == {}:
                if master.get('id',''):
                    datas = self.generate_cache_data(city=city, name=master.get('id'))
                else:
                    datas = self.generate_cache_data(name=master.get('name'))
                name_list = datas.pop('name_list')
                for borough_name in name_list:
                    self.redis_conn.hset(key, BaseUtils.getMd5(borough_name),json.dumps(datas))
            else:
                self.redis_conn.hdel(key, BaseUtils.getMd5(slave.get('name')))
            return True
        except Exception as e:
            print(e)
            return False

    # 城区商圈变化后更新小区缓存
    def update_area_cache(self, city, cityarea={}, cityarea2={}):
        try:
            cityarea_id = cityarea.get('id', '')
            cityarea_name = cityarea.get('name', '')
            cityarea2_id = cityarea2.get('id', '')
            cityarea2_name = cityarea2.get('name', '')
            if cityarea2_id:
                datas = self.find(filter={"cityarea.cityarea2.cityarea2_id": cityarea2_id}, city=city)
            elif cityarea_id:
                datas = self.find(filter={"cityarea.cityarea.cityarea_id": cityarea_id}, city=city)
            elif cityarea_name:
                datas = self.find(filter={"cityarea.cityarea.cityarea_name": cityarea_name}, city=city)
            elif cityarea2_name:
                datas = self.find(filter={"cityarea.cityarea2.cityarea2_name": cityarea2_name}, city=city)
            else:
                datas = []
            for i in datas:
                self.update_borough_cache(city=city, master={'name': i.get('borough_name')})
        except Exception as e:
            print(e)
            return False

    # 删除小区详情的缓存
    def delete_borough_detail_cache(self, borough_id, city):
        cur_time = time.time()
        today = cur_time - cur_time % 86400 + time.timezone
        cache_key = "cache_%s" % (city)
        field = "%s_bor_id_%s" % (int(today), borough_id)
        del_action = self.api_redis_conn.hdel(cache_key, field)
        print(del_action)
        return del_action

    # 获取es数据
    def mod_borough_list_es_cityarea(self, city, borough_id, cityarea):
        index_name = "online_" + city + "_borough"
        if city == 'bj':
            doc_name = "spider"
        else:
            doc_name = "spider_" + city
        cityarea_id = cityarea['cityarea']['cityarea_id']
        cityarea_name = cityarea['cityarea']['cityarea_name']
        cityarea2 = cityarea['cityarea2']
        if cityarea2:
            if 'area2_pinyin' in cityarea2[0]:
                cityarea2[0].pop('area2_pinyin')
        datas = {'cityarea_id': cityarea_id,
                 'cityarea_name': cityarea_name,
                 'cityarea2': cityarea2
                 }
        mod_borough_list = self.essearch.update_index(index=index_name, doc_name=doc_name, borough_id=borough_id,
                                                      body=datas)
        return mod_borough_list

    # 更新社区es数据
    def mod_borough_list_es_community(self, city, borough_id, community):
        index_name = "online_" + city + "_borough"
        if city == 'bj':
            doc_name = "spider"
        else:
            doc_name = "spider_" + city
        datas = {'community': community}
        mod_borough_list = self.essearch.update_index(index=index_name, doc_name=doc_name, borough_id=borough_id,
                                                      body=datas)
        return mod_borough_list

    # 更新社区es数据
    def mod_borough_list_es_level(self, city, borough_id, level):
        index_name = "online_" + city + "_borough"
        if city == 'bj':
            doc_name = "spider"
        else:
            doc_name = "spider_" + city
        datas = {'borough_level': level}
        mod_borough_list = self.essearch.update_index(index=index_name, doc_name=doc_name, borough_id=borough_id,
                                                      body=datas)
        return mod_borough_list

    def mod_house_community(self, city, community, borough_id):
        updateBody = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"borough_id": borough_id}}
                    ]
                }
            },
            "script": {
                "inline": "ctx._source.community = params.community",
                "params": {
                    "community": community
                },
                "lang": "painless"
            }
        }
        if city == 'bj':
            index_doc = 'spider'
        else:
            index_doc = 'spider_' + city
        # 房源的批量修改
        house_index = "online_" + city + "_house_sell"
        house_list_status = self.essearch.update_by_query(index=house_index, doc_type=index_doc, body=updateBody)
        print(house_list_status)

    # 根据小区id更改es房源的小区商圈
    def mod_house_cityarea(self, borough_id, cityarea, city):
        try:
            cityarea_2 = cityarea["cityarea2"]
            if cityarea_2:
                if 'area_pinyin' in cityarea_2[0]:
                    cityarea_2[0].pop('area_pinyin')
            mater_cityarea_id = cityarea['cityarea']['cityarea_id']
            mater_cityarea_name = cityarea['cityarea']['cityarea_name']
            mater_cityarea_py = cityarea['cityarea']['cityarea_py']
            updateBody = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"borough_id": borough_id}}
                        ],
                    }
                },
                "script": {
                    "inline": "ctx._source.cityarea_id = params.cityarea_id;ctx._source.cityarea_name = params.cityarea_name;ctx._source.cityarea_py = params.cityarea_py;ctx._source.cityarea2 = params.cityarea2",
                    "params": {
                        "cityarea_id": mater_cityarea_id,
                        "cityarea_name": mater_cityarea_name,
                        "cityarea_py": mater_cityarea_py,
                        "cityarea_2": cityarea_2
                    },
                    "lang": "painless"
                }
            }
            if city == 'bj':
                index_doc = 'spider'
            else:
                index_doc = 'spider' + city
            # 房源的批量修改
            house_index = "active_" + city + "_house_sell"
            house_list_status = self.essearch.update_by_query(index=house_index, doc_type=index_doc, body=updateBody)
        except Exception as e:
            print("update_fail", e)
            return False

    # 根据小区id查询es数据
    def search_es(self, city, borough_id):
        index_name = "borough_" + city + "_active"
        if city == 'bj':
            doc_name = "spider"
        else:
            doc_name = "spider_" + city
        borough_data = self.essearch.search_filter(index_name=index_name, doc_name=doc_name, borough_id=borough_id)

    # 删除小区列表es
    def delete_borough_list_es(self, city, borough_ids):
        try:
            index_name = "borough_" + city + "_active"
            if city == 'bj':
                doc_name = "spider"
            else:
                doc_name = "spider_" + city
            for borough_id in borough_ids:
                self.essearch.delete_type(index=index_name, doc_type=doc_name, id=borough_id)
        except Exception as e:
            return False

    # 修改主名
    def mod_borough_mastername(self, borough_id, master_name, city):
        master_info = {}
        current_time = BaseUtils.getIntTime()
        field = {'_id': 1, 'borough_name': 1, "borough_byname": 1, "borough_hide": 1}
        filter = {'_id': borough_id}
        master_name = master_name.strip(" ")
        borough_datas = self.get_page(city=city, filter=filter, field=field)
        if len(borough_datas) == 1:
            same_name_borough = self.get_page(city=city, filter={'borough_name': master_name}, field=field)
            if len(same_name_borough) >= 1:
                master_info.setdefault("master_info", {"status": -1, "borough_id": borough_id, "remark": "存在同名小区"})
            else:
                origin_name = borough_datas[0].get("borough_name", "")
                borough_byname = borough_datas[0].get("borough_byname", [])
                if master_name not in borough_byname:
                    borough_byname.append(master_name)
                # 小区联想词更新 略 先更改房源小区名 后更新联想词数量
                guess_status = self.guessword.modify_borough_name(city=city, guessword_dict={origin_name: master_name})
                datas = {"$set": {"borough_name": master_name, "borough_byname": borough_byname, "borough_ctype": 8,
                                  "updated": current_time}}
                borough_update_action = self.update_filter(city=city, filter={'_id': borough_id}, datas=datas)
                if borough_update_action['nModified'] == 1:
                    borough_cache_status = self.update_borough_cache(city=city, master={"id": borough_id})
                    # 删除小区详情页缓存
                    borough_detail_cache_status = self.delete_borough_detail_cache(city=city, borough_id=borough_id)
                    # 更改小区列表的es
                    borough_name = "online_" + city + "_borough"
                    if city == 'bj':
                        doc_name = "spider"
                    else:
                        doc_name = "spider_" + city
                    datas = {"borough_name": master_name}
                    mod_borough_list = self.essearch.update_index(index=borough_name, doc_name=doc_name,
                                                                  borough_id=borough_id, body=datas)
                    # 更改房源的es
                    house_name = "online_" + city + "_house_sell"
                    updateBody = {
                        "query": {
                            "bool": {
                                "must": [
                                    {"term": {"borough_id": borough_id}}
                                ],
                            }
                        },
                        "script": {
                            "inline": "ctx._source.borough_name = params.borough_name",
                            "params": {
                                "borough_name": master_name
                            },
                            "lang": "painless"
                        }
                    }
                    mod_house_list = self.essearch.update_by_query(index=house_name, doc_type=doc_name, body=updateBody)
                    # 发布信息
                    message = {
                        "city": city,
                        "operate": "update",
                        "type": "borough",
                        "pms": {
                            "master": {
                                "id": borough_id,
                                "name": master_name
                            },
                            "slave": []
                        }
                    }
                    publish_status = self.publish_message(channel="dm_info_change", message=message)
                    master_info.setdefault("master_info", {"status": 1, "borough_id": borough_id,
                                                           "remark": "更新小区id为%s的主名成功" % (borough_id)})
                else:
                    master_info.setdefault("master_info", {"status": -1, "borough_id": borough_id,
                                                           "remark": "更新小区id为%s的主名失败" % (borough_id)})
        else:
            master_info.setdefault("master_info",
                                   {"status": -1, "borough_id": borough_id, "remark": "找不到小区id为%d的数据" % (borough_id)})
        return master_info

    def getTraffic(self, city, loc):

        # 检查地铁信息
        loc = self.bd09_to_gcj02(loc)
        data = {}
        filter = {
            "location": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [float(loc['lng']), float(loc['lat'])]
                    },
                    "$maxDistance": 1500
                }
            }
        }
        field = {'_id': 1, 'name': 1, "list_line": 1}
        result = self.subway_service.get_all(city=city, field=field, filter=filter)
        print(filter)
        subway = []
        if result:
            for station in result:
                subway_station = {
                    'subway_station_id': station['_id'],
                    'subway_station_name': station['name'],
                    'subway_list_line': station['list_line'],
                }
                subway.append(subway_station)
            if subway:
                data['subway'] = subway

        # 边界关联商圈 优先关联商圈 商圈映射城区
        cityarea_second = dict()
        cityarea2_second = dict()
        filters = {"polygons1": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [float(loc['lng']), float(loc['lat'])]
                },
            }
        }}
        cityarea2_result = self.complex_cityarea2.get_one(city=city, field={'_id': 1, 'name': 1, 'area2_pinyin': 1},
                                                          filter=filters)
        if cityarea2_result:
            cityarea2_second['id'] = cityarea2_result[0]['_id']
            cityarea2_second['name'] = cityarea2_result[0]['name']
            cityarea2_second['pinyin'] = cityarea2_result[0]['area2_pinyin']
            data['cityarea2'] = cityarea2_second

            cityarea_result = self.complex_cityarea.get_one(
                city=city, field={'_id': 1, 'name': 1, "area_pinyin": 1},
                filter={"list_cityarea2.%s" % cityarea2_result[0]["name"]: cityarea2_result[0]["_id"], "status": 1})
            if cityarea_result:
                cityarea_second['id'] = cityarea_result[0]['_id']
                cityarea_second['name'] = cityarea_result[0]['name']
                cityarea_second['pinyin'] = cityarea_result[0]['area_pinyin']
                data['cityarea'] = cityarea_second

        # 关联城区
        if "cityarea" not in data:
            cityarea_result = self.complex_cityarea.get_one(city=city, field={'_id': 1, 'name': 1, "area_pinyin": 1},
                                                            filter={
                                                                "polygons1": {
                                                                    "$geoIntersects": {
                                                                        "$geometry": {
                                                                            "type": "Point",
                                                                            "coordinates": [float(loc['lng']),
                                                                                            float(loc['lat'])]
                                                                        },
                                                                    }
                                                                },
                                                                "status": 1
                                                            })
            if cityarea_result:
                cityarea_second['id'] = cityarea_result[0]['_id']
                cityarea_second['name'] = cityarea_result[0]['name']
                cityarea_second['pinyin'] = cityarea_result[0]['area_pinyin']
                data['cityarea'] = cityarea_second
        return data

    def getCityarea(self, city, loc):
        loc = self.bd09_to_gcj02(loc)
        cityarea = {}
        cityarea_second = dict()
        # 边界关联商圈
        cityarea2_result = self.complex_cityarea2.get_one(city=city, field={'_id': 1, 'name': 1, "area2_pinyin": 1},
                                                          filter={
                                                              "polygons1": {
                                                                  "$geoIntersects": {
                                                                      "$geometry": {
                                                                          "type": "Point",
                                                                          "coordinates": [float(loc['lng']),
                                                                                          float(loc['lat'])]
                                                                      },
                                                                  }
                                                              }
                                                          })
        cityarea2_second = {}
        if cityarea2_result:
            cityarea2_second['id'] = cityarea2_result[0]['_id']
            cityarea2_second['name'] = cityarea2_result[0]['name']
            cityarea2_second['pinyin'] = cityarea2_result[0]['area2_pinyin']
            cityarea['cityarea2'] = cityarea2_second

            # 匹配到商圈的话直接返回城区
            null_cityarea = self.complex_cityarea.get_all(city=city, field={'_id': 1, 'name': 1, "area_pinyin": 1},
                                                          filter={"list_cityarea2.%s" % cityarea2_result[0]["name"]:
                                                                      cityarea2_result[0]["_id"], "status": 1})
            if null_cityarea:
                cityarea_second['id'] = null_cityarea[0]['_id']
                cityarea_second['name'] = null_cityarea[0]['name']
                cityarea_second['pinyin'] = null_cityarea[0]['area_pinyin']
                cityarea['cityarea'] = cityarea_second

        if "cityarea" not in cityarea:
            # 关联城区
            cityarea_result = self.complex_cityarea.get_one(city=city, field={'_id': 1, 'name': 1, "area_pinyin": 1},
                                                            filter={
                                                                "polygons1": {
                                                                    "$geoIntersects": {
                                                                        "$geometry": {
                                                                            "type": "Point",
                                                                            "coordinates": [float(loc['lng']),
                                                                                            float(loc['lat'])]
                                                                        },
                                                                    }
                                                                }
                                                            })
            if cityarea_result:
                # cityarea_second = dict()
                cityarea_second['id'] = cityarea_result[0]['_id']
                cityarea_second['name'] = cityarea_result[0]['name']
                cityarea_second['pinyin'] = cityarea_result[0]['area_pinyin']
                cityarea['cityarea'] = cityarea_second

        return cityarea

    def bd09_to_gcj02(self, loc):
        """
        百度坐标系(BD-09)转火星坐标系(GCJ-02)
        百度——>谷歌、高德
        :param bd_lat:百度坐标纬度
        :param bd_lon:百度坐标经度
        :return:转换后的坐标列表形式
        """
        bd_lon = float(loc['lng'])
        bd_lat = float(loc['lat'])
        x_pi = 3.14159265358979324 * 3000.0 / 180.0
        x = bd_lon - 0.0065
        y = bd_lat - 0.006
        z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * x_pi)
        theta = math.atan2(y, x) - 0.000003 * math.cos(x * x_pi)
        gg_lng = z * math.cos(theta)
        gg_lat = z * math.sin(theta)
        loc = {}
        loc['lng'] = gg_lng
        loc['lat'] = gg_lat
        return loc

    # 向各方发布消息
    def publish_message(self, channel, message):
        status = False
        try:
            status = self.redis_conn.publish(channel, message)
            if status:
                return True
            return status
        except Exception as e:
            print(e)
        return status

    # 小区合并
    def batchUpdateBorough(self, city, datas, run_type):
        try:
            deal_borough_info = []
            deal_borough_error_info = []
            deal_borough_record = []
            check_borough_house_key = city + "_change_borough_list"
            if len(datas) > 0:
                for item in datas:
                    # 每一次 更新执行的结果
                    change_borough = self.merge_same_borough(master_id=item['master_id'], slave_ids=item['slave_ids'],
                                                             city=city)
                    if "info" not in change_borough:
                        deal_borough_error_info.append(change_borough['record'])
                        continue
                    change_borough_record = change_borough['record']
                    change_borough_info = change_borough['info']
                    if change_borough['info']['slave_ids']:
                        self.redis_conn.lpush(check_borough_house_key, change_borough_info)
                        deal_borough_info.append(change_borough_info)
                        deal_borough_record.append(change_borough_record)
                    else:
                        deal_borough_error_info.append({"salve_ids": change_borough['record']['slave_record']})
                if run_type == "online":
                    if len(deal_borough_info) > 0:
                        pms = {"data": deal_borough_info}
                        city_quanpin = city_mapping.get(city)['fpy']
                        dissolute_url = "http://10.171.93.224:8088/Bad/Bad/update_borough?dbname=%s&type=sell" % (
                            city_quanpin)
                        requests_action = requests.post(dissolute_url, json=pms)
                        print(requests_action.content)
                deal_info = {"success_borough": deal_borough_record, "error_borough": deal_borough_error_info}
                print(deal_info)
                # 执行成功的话 直接掉小汤接口
                return deal_info
            else:
                return {'message': "传入的房源数据为空"}
        except Exception as e:
            print(e)

    # 每一次 更新执行的结果
    def merge_same_borough(self, master_id, slave_ids, city):
        try:
            delete_dict = {}
            update_dict = {}
            slave_info = []
            slave_list = []
            borough_byname = []
            source_url = []
            borough_hide = []
            current_time = BaseUtils.getIntTime()
            # field = {'_id': 1, 'borough_name': 1, "borough_byname": 1, "borough_hide": 1, "source_url": 1}
            filter = {'_id': master_id}
            master_datas = self.get_page(city=city, filter=filter)
            if len(master_datas) == 1:  # 判断主小区是否存在
                master_borough_name = master_datas[0].get("borough_name", "")
                borough_byname += master_datas[0].get("borough_byname", [])
                borough_hide += master_datas[0].get("borough_hide", [])
                source_url += master_datas[0].get("source_url", [])
                filter = {'$or': [{'_id': id} for id in slave_ids]}
                slave_datas = self.get_page(city=city, filter=filter)
                # 从小区的信息
                exist_borough_ids = []
                exist_borough_names = []
                exist_borough_info = []
                if slave_datas:
                    for data in slave_datas:
                        borough_byname += data.get("borough_byname", [])
                        borough_hide += data.get("borough_hide", [])
                        source_url += data.get("source_url", [])
                        id = int(data.get("_id", ""))
                        borough_name = data.get("borough_name", "")
                        exist_borough_ids.append(id)
                        exist_borough_names.append(borough_name)
                        exist_borough_info.append({"id": id, "name": borough_name})
                        filter = {'_id': id}
                        del_action = self.delete_id(city=city, filter=filter)
                        if del_action['n'] == 1:
                            slave_info.append({"borough_id": id, "borough_name": borough_name})
                            slave_list.append({"id": id, "name": borough_name})
                            insert_id = self.borough_recycle.insert_one(city=city, datas=data)
                            delete_dict[id] = {"status": 1, "insert_id": insert_id, "remark": "小区id为%s的删除成功" % (id)}
                        else:
                            delete_dict[id] = {"status": 0, "remark": "小区id为%s的删除成功" % (id)}
                else:
                    delete_dict[master_id] = {"status": -1, "remark": "要合并的小区找不到数据"}
                    return {'record': {"master_info": delete_dict}}
                empty_borough_list = list(set(slave_ids) ^ set(exist_borough_ids))
                if empty_borough_list:
                    for id in empty_borough_list:
                        delete_dict[id] = {"status": -1, "remark": "小区id为%s的找不到数据" % (id)}
                # =======删除==================
                up_datas = {}
                if borough_byname:
                    up_datas.setdefault("borough_byname", list(set(borough_byname)))
                if source_url:
                    up_datas.setdefault("source_url", list(set(source_url)))
                if borough_hide:
                    up_datas.setdefault("borough_hide", list(set(borough_hide)))
                up_datas.setdefault("borough_ctype", 8)
                up_datas.setdefault("updated", current_time)
                # =======更新==================
                datas = {'$set': up_datas}
                filter = {'_id': master_id}
                update_action = self.update_by_filter(filter=filter, datas=datas, city=city)
                if update_action['nModified'] == 1:
                    # 更新二手房小区缓存
                    borough_cache_status = self.update_borough_cache(master={"id": master_id}, city=city)
                    # 删除小区联想词
                    borough_guess = self.guessword.delete_guessword(city=city, guessword_list=exist_borough_info,
                                                                    type_id=3)
                    # 删除小区es列表页
                    borough_list_es = self.delete_borough_list_es(city=city, borough_ids=exist_borough_ids)
                    # publish发布数据的消息
                    message = {
                        "city": city,
                        "operate": "merge",
                        "type": "borough",
                        "pms": {
                            "master": {
                                "id": master_id,
                                "name": master_borough_name
                            },
                            "slave": slave_list
                        }
                    }
                    publish_status = self.publish_message("dm_info_change", message)
                    update_dict[master_id] = {"status": 1, "remark": "更新小区id为%s的成功" % (master_id)}
                else:
                    update_dict[master_id] = {"status": 0, "remark": "更新小区id为%s的失败" % (master_id)}
                # =======更新==================
                master_info = {"master_id": master_id, "borough_name": master_borough_name}
                change_borough_info = {"master_info": master_info, "slave_ids": slave_info}
                change_borough_record = {"master_info": update_dict, "slave_info": delete_dict}
                change_borough = {"info": change_borough_info, "record": change_borough_record}
                return change_borough
            else:
                return {'record': {
                    "master_record": {master_id: {"status": -1, "remark": "master_id: %s的小区id未找到" % (master_id)}}}}
        except Exception as e:
            print("merge_same_borough", e)

    # 移动别名到另一小区
    def move_byname_to_master(self, city, before_info, master_id):
        master_info = {}
        slave_list = []
        slave_names = before_info.get("name", "")
        before_id = before_info.get("borough_id", "")
        field = {'_id': 1, 'borough_name': 1, "borough_byname": 1, "borough_hide": 1, "source_url": 1}
        filter = {'_id': before_id}
        before_datas = self.get_page(city=city, filter=filter, field=field)
        move_names = []
        # 添加到新的小区别名
        after_datas = self.get_page(city=city, filter={'_id': master_id}, field=field)
        if not before_datas:
            master_info.setdefault("master_info",
                                   {"status": -1, "borough_id": before_id, "remark": "没有找到之前的小区id%s信息" % (before_id)})
            return master_info
        if not after_datas:
            master_info.setdefault("master_info",
                                   {"status": -1, "borough_id": master_id, "remark": "没有找到变更后的小区id%s信息" % (master_id)})
            return master_info
        borough_byname = before_datas[0].get("borough_byname", [])
        borough_hide = before_datas[0].get("borough_hide", [])
        for slave_name in slave_names:
            slave_list.append({"id": before_id, "name": slave_name})
            slave_name = slave_name.strip(" ")
            if slave_name in borough_byname:
                borough_byname.remove(slave_name)
                move_names.append(slave_name)
            if slave_name in borough_hide:
                borough_hide.remove(slave_name)
                move_names.append(slave_name)
        borough_byname = list(set(borough_byname))
        borough_hide = list(set(borough_hide))
        # 更新之前的小区信息
        borough_update_action = self.update_by_filter(city=city, filter={'_id': before_id}, datas={
            "$set": {"borough_byname": borough_byname, "borough_ctype": 8, "borough_hide": borough_hide,
                     "updated": int(time.time())}})
        if borough_update_action['nModified'] == 1:
            master_info.setdefault("slave_info",
                                   {"status": 1, "borough_id": before_id, "remark": "更新小区id为%s的别名成功" % (before_id)})
        else:
            master_info.setdefault("slave_info",
                                   {"status": -1, "borough_id": before_id, "remark": "更新小区id为%s的别名失败" % (before_id)})
        # 做处理
        master_borough_name = after_datas[0].get("borough_name", [])
        master_borough_byname = after_datas[0].get("borough_byname", [])
        for master_name in move_names:
            master_borough_byname.append(master_name)
        master_borough_byname = list(set(master_borough_byname))
        master_borough_update_action = self.update_by_filter(city=city, filter={'_id': master_id}, datas={
            "$set": {"borough_byname": master_borough_byname, "borough_ctype": 8, "updated": int(time.time())}})
        if master_borough_update_action['nModified'] == 1:
            # 更新小区缓存
            borough_cache_status = self.update_borough_cache(city=city, master={"id": master_id})
            # publish信息
            message = {
                "city": city,
                "operate": "split_merge",
                "type": "borough",
                "pms": {
                    "master": {
                        "id": master_id,
                        "name": master_borough_name
                    },
                    "slave": slave_list
                }
            }
            publish_status = self.publish_message("dm_info_change", message)
            master_info.setdefault("master_info",
                                   {"status": 1, "borough_id": master_id, "remark": "更新小区id为%s的别名成功" % (master_id)})
        else:
            master_info.setdefault("master_info",
                                   {"status": -1, "borough_id": master_id, "remark": "更新小区id为%s的别名失败" % (master_id)})
        return master_info

    # 删除小区合并名和隐藏别名
    def delete_false_boroughname(self, city, slave_names, master_id):
        master_info = {}
        field = {'_id': 1, 'borough_name': 1, "borough_byname": 1, "borough_hide": 1, "source_url": 1}
        filter = {'_id': master_id}
        master_datas = self.get_page(city=city, filter=filter, field=field)
        if len(master_datas) == 1:
            borough_byname = master_datas[0].get("borough_byname", [])
            borough_hide = master_datas[0].get("borough_hide", [])
            for slave_name in slave_names:
                # 删除别名的二手房缓存
                borough_cache_status = self.update_borough_cache(city=city, slave={"name": slave_name})
                slave_name = slave_name.strip(" ")
                if slave_name in borough_byname:
                    borough_byname.remove(slave_name)
                if slave_name in borough_hide:
                    borough_hide.remove(slave_name)
            datas = {"$set": {"borough_byname": borough_byname, "borough_ctype": 8, "borough_hide": borough_hide,
                              "updated": int(time.time())}}
            borough_update_action = self.update_by_filter(city=city, filter=filter, datas=datas)
            if borough_update_action['nModified'] == 1:
                master_info.setdefault("master_info",
                                       {"status": 1, "borough_id": master_id, "remark": "更新小区id为%s的别名成功" % (master_id)})
            else:
                master_info.setdefault("master_info", {"status": -1, "borough_id": master_id,
                                                       "remark": "更新小区id为%s的别名失败" % (master_id)})
        else:
            master_info.setdefault("master_info",
                                   {"status": -1, "borough_id": master_id, "remark": "更新小区id为%s无数据" % (master_id)})
        return master_info

    # 删除小区别名
    def delete_borough_alias_name(self, city, borough_id, alias_names):
        master_info = {}
        current_time = BaseUtils.getIntTime()
        if alias_names:
            find_alias_names = self.dao.find_one(city=city, filter={"_id": borough_id})
            if not find_alias_names:
                master_info.setdefault("master_info", {"status": -1, "remark": "找不到该小区的信息", "borough_id": borough_id})
                return master_info
            name_list = find_alias_names[0].get("alias_name", [])
            for alias_name in name_list:
                alias_name = alias_name.strip(" ")
                if alias_name in alias_names:
                    name_list.remove(alias_name)
            name_list = list(set(name_list))
            update_action = self.update_by_filter(city=city, filter={'_id': borough_id}, datas={
                "$set": {"alias_name": name_list, "borough_ctype": 8, "updated": current_time}})
            if update_action['nModified'] == 1:
                master_info.setdefault("master_info", {"status": 1, "remark": "小区id为%s的别名删除成功" % (borough_id),
                                                       "borough_id": borough_id})
            else:
                master_info.setdefault("master_info", {"status": -1, "remark": "小区id为%s的别名删除失败" % (borough_id),
                                                       "borough_id": borough_id})
        else:
            master_info.setdefault("master_info", {"status": -1, "remark": "小区别名为空", "borough_id": borough_id})
        return master_info

    # 高德转百度坐标
    def gcj02_to_bd09(self, lng, lat):
        """
        火星坐标系(GCJ-02)转百度坐标系(BD-09)
        谷歌、高德——>百度
        :param lng:火星坐标经度
        :param lat:火星坐标纬度
        :return:
        """
        x_pi = 3.14159265358979324 * 3000.0 / 180.0
        lng = float(lng)
        lat = float(lat)
        z = math.sqrt(lng * lng + lat * lat) + 0.00002 * math.sin(lat * x_pi)
        theta = math.atan2(lat, lng) + 0.000003 * math.cos(lng * x_pi)
        bd_lng = z * math.cos(theta) + 0.0065
        bd_lat = z * math.sin(theta) + 0.006
        return [bd_lng, bd_lat]

    # 拆分新建小区
    def split_create_new_borough(self, city, borough_id, slave_name, new_borough_data):
        update_dict = {}
        new_dict = {}
        current_time = BaseUtils.getIntTime()
        field = {'_id': 1, 'borough_name': 1, "borough_byname": 1, "borough_hide": 1, "source_url": 1}
        filter = {'_id': borough_id}
        master_datas = self.mod_borough_cityarea(city=city, filter=filter, field=field)
        print(master_datas)
        before_borough_byname = []
        if len(master_datas) == 1:
            borough_byname = master_datas[0].get("borough_byname", [])
            borough_hide = master_datas[0].get("borough_hide", [])
            before_borough_byname = borough_byname
            slave_name = (slave_name.strip(" ")).encode()
            if slave_name in borough_byname:
                borough_byname.remove(slave_name)
            if slave_name in borough_hide:
                borough_hide.remove(slave_name)
            datas = {"$set": {"borough_byname": borough_byname, "borough_ctype": 8, "borough_hide": borough_hide,
                              "updated": current_time}}
            borough_update_action = self.update_by_filter(city=city, filter=filter, datas=datas)
            if borough_update_action['nModified'] == 1:
                update_dict[borough_id] = {"status": 0, "remark": "更新小区id为%s的别名成功,但新建失败" % (borough_id)}
            else:
                update_dict[borough_id] = {"status": 0, "remark": "更新小区id为%s的别名失败" % (borough_id)}
        else:
            update_dict[borough_id] = {"status": -1, "remark": "id为%s的小区找不到数据" % (borough_id)}
            return {"split_info": update_dict, "new_info": new_dict}
        new_borough_name = new_borough_data.get("borough_name", "")
        filter = {'$or': [{"borough_hide": new_borough_name}, {"borough_name": new_borough_name}]}
        new_borough_datas = self.mod_borough_cityarea(city=city, filter=filter, field=field)
        if len(new_borough_datas) == 1:
            exist_borough_id = new_borough_datas[0]['_id']
            new_dict[exist_borough_id] = {"status": -1, "remark": "小区id为%d存在相似名字" % exist_borough_id}
            return {"split_info": update_dict, "new_info": new_dict}
        # 小区别名的检测
        byname_list = []
        byname_list.append(new_borough_name)
        borough_byname = new_borough_data.get("borough_byname", "")
        if borough_byname.strip():
            byname_list.append(borough_byname)
        location = new_borough_data.get("location", "")
        lng = location.get("lng", "")
        lat = location.get("lat", "")
        location = {"type": "Point", "coordinates": [float(lng), float(lat)]}
        bd_loc = self.gcj02_to_bd09(lng, lat)
        if bd_loc:
            bd_lng = bd_loc[0]
            bd_lat = bd_loc[1]
        else:
            bd_lng = 0
            bd_lat = 0
        loc = {"lng": bd_lng, "lat": bd_lat}
        cityarea = new_borough_data.get("cityarea", "")
        bor_pinyin = BaseUtils.getFpy(new_borough_name, delimiter='')
        borough_address = new_borough_data.get("borough_address", "")
        borough_completion = new_borough_data.get("borough_completion", "")
        borough_volume = new_borough_data.get("borough_volume", "")
        borough_green = new_borough_data.get("borough_green", "")
        borough_area = new_borough_data.get("borough_area", "")
        borough_company = new_borough_data.get("borough_company", "")
        borough_developer = new_borough_data.get("borough_developer", "")
        borough_costs = new_borough_data.get("borough_costs", "")
        borough_property_right = new_borough_data.get("borough_property_right", "")
        borough_totalnumber = new_borough_data.get("borough_totalnumber", "")
        borough_totalbuilding = new_borough_data.get("borough_totalbuilding", "")
        borough_buildingType = new_borough_data.get("borough_buildingType", "")
        borough_info = {"borough_address": borough_address,
                        "borough_completion": borough_completion,
                        "borough_volume": borough_volume,
                        "borough_green": borough_green,
                        "borough_area": borough_area,
                        "borough_company": borough_company,
                        "borough_developer": borough_developer,
                        "borough_costs": borough_costs,
                        "borough_property_right": borough_property_right,
                        "borough_totalnumber": borough_totalnumber,
                        "borough_totalbuilding": borough_totalbuilding,
                        "borough_buildingType": borough_buildingType
                        }
        new_borough_id = self.max_id.getMaxId(type="borough", city=city)
        datas = {"borough_name": new_borough_name,
                 "borough_byname": byname_list,
                 "loc": loc,
                 "location": location,
                 "cityarea": cityarea,
                 "status": 1,
                 "bor_pinyin": bor_pinyin,
                 "_id": new_borough_id,
                 "borough_info": borough_info,
                 "borough_ctype": 8,
                 "created": current_time,
                 "updated": current_time
                 }
        es_data = {
            "cityarea_name": cityarea['cityarea']['cityarea_name'],
            "cityarea_id": cityarea['cityarea']['cityarea_id'],
            "cityarea2": cityarea['cityarea2'],
            "house_count": 0,
            "company": "",
            "pic": "",
            "borough_id": new_borough_id,
            "borough_name": new_borough_name,
            "traffic": 0,
            "subway": [],
            "price_rate": 0.00002,
            "borough_info": borough_info,
            "price": 0
        }
        # 添加小区基础数据成功
        new_borough_id = self.insert_one(datas=datas)
        if new_borough_id:
            # 添加小区列表es
            es_status = self.add_borough_list_es(borough_id=new_borough_id, es_data=es_data, city=city)
            # 新增小区的缓存
            master_cache_status = self.update_borough_cache(city=city, master={"id": new_borough_id})
            # 更新旧小区列表es数据
            slave_cache_status = self.update_borough_cache(city=city, master={"name": slave_name})
            # 添加小区联想词
            guess_status = self.guessword.add_guessword(city=city, guessword_list=[new_borough_name], type_id=3)
            message = {
                "city": city,
                "operate": "split_create",
                "type": "borough",
                "pms": {
                    "master": {
                        "id": new_borough_id,
                        "name": new_borough_name
                    },
                    "slave": []
                }
            }
            publish_status = self.publish_message("dm_info_change", message)
            new_dict[borough_id] = {"status": 1, "remark": "新建id为%s的小区成功" % (borough_id)}
            return {"split_info": update_dict, "new_info": new_dict, "es_status": es_status}
        else:
            new_dict[borough_id] = {"status": 0, "remark": "新建id为%s的小区失败" % (borough_id)}
            # 添加失败后的重新添加
            filter = {'_id': borough_id}
            datas = {
                "$addToSet": {"borough_byname": before_borough_byname, "borough_ctype": 8, "updated": current_time}}
            self.update_filter(filter=filter, datas=datas, city=city)
            return {"split_info": update_dict, "new_info": new_dict}

    # 小区添加别名
    def add_borough_alias_name(self, city, borough_id, alias_names):
        master_info = {}
        current_time = BaseUtils.getIntTime()
        if alias_names:
            find_alias_names = self.get_one(filter={"_id": borough_id}, city=city)
            if not find_alias_names:
                master_info.setdefault("master_info", {"status": -1, "borough_id": borough_id, "remark": "找不到该小区的信息"})
                return master_info
            name_list = find_alias_names[0].get("alias_name", [])
            alias_names = alias_names + name_list
            alias_names = list(set(alias_names))
            datas = {"$set": {"alias_name": alias_names, "borough_ctype": 8, "updated": current_time}}
            update_action = self.update_filter(filter={'_id': borough_id}, datas=datas, city=city)
            if update_action['nModified'] == 1:
                master_info.setdefault("master_info", {"status": 1, "borough_id": borough_id,
                                                       "remark": "小区id为%s的别名添加成功" % (borough_id)})
            else:
                master_info.setdefault("master_info", {"status": -1, "borough_id": borough_id,
                                                       "remark": "小区id为%s的别名添加失败" % (borough_id)})
        else:
            master_info.setdefault("master_info", {"status": -1, "borough_id": borough_id, "remark": "小区别名为空"})
        return master_info

    # 新增小区别名
    def add_borough_byname(self, borough_id, salve_names, city):
        master_info = {}
        if len(salve_names) > 0:
            field = {'_id': 1, 'borough_name': 1, "borough_byname": 1, "source_url": 1}
            filter = {'_id': borough_id}
            borough_datas = self.mod_borough_cityarea(filter=filter, field=field, city=city)
            if len(borough_datas) == 1:
                byname_list = borough_datas[0].get("borough_byname", "")
                borough_byname = byname_list + salve_names
                borough_byname = list(set(borough_byname))
                datas = {"$set": {"borough_byname": borough_byname, "borough_ctype": 8, "updated": int(time.time())}}
                borough_update_action = self.update_by_filter(filter=filter, datas=datas, city=city)
                if borough_update_action['nModified'] == 1:
                    # 更新小区缓存
                    borough_cache_status = self.update_borough_cache(city=city, master={"id": borough_id})
                    master_info.setdefault("master_info", {"borough_id": borough_id, "status": 1,
                                                           "borough_cache": borough_cache_status,
                                                           "remark": "添加小区id为%s的别名成功" % (borough_id)})
                else:
                    master_info.setdefault("master_info", {"borough_id": borough_id, "status": -1,
                                                           "remark": "添加小区id为%s的别名失败" % (borough_id)})
            else:
                master_info.setdefault("master_info", {"borough_id": borough_id, "status": -1, "remark": "找不到该小区的数据"})
        else:
            master_info.setdefault("master_info", {"borough_id": borough_id, "status": -1, "remark": "别名为空"})
        return master_info

    # 修改小区的坐标
    def mod_borough_location(self, borough_id, location, city):
        master_info = {}
        lng = location.get("lng", "")
        lat = location.get("lat", "")
        bd_loc = self.gcj02_to_bd09(lng, lat)
        if bd_loc:
            bd_lng = bd_loc[0]
            bd_lat = bd_loc[1]
            filter = {'_id': borough_id}
            datas = {"$set": {"loc": {"lng": bd_lng, "lat": bd_lat},
                              "location": {"type": "Point", "coordinates": [lng, lat]}, "borough_ctype": 8,
                              "updated": int(time.time())}}
            location_update_action = self.update_filter(city=city, filter=filter, datas=datas)
            if location_update_action['nModified'] == 1:
                master_info.setdefault("master_info", {"status": 1, "borough_id": borough_id, "remark": "修改小区坐标成功"})
            else:
                master_info.setdefault("master_info", {"status": -1, "borough_id": borough_id, "remark": "修改小区坐标失败"})
        else:
            master_info.setdefault("master_info", {"status": -1, "borough_id": borough_id, "remark": "找不到小区坐标数据"})
        return master_info

    def get_city(self, *args, **kwargs):
        kwargs['field'] = {'_id': 1, 'borough_name': 1, 'cityarea': 1}  # 返回字段
        return self.dao.find_page(*args, **kwargs)

    # 修改小区的边界
    def mod_borough_polygons(self, city, borough_id, polygons):
        master_info = {}
        if borough_id and polygons:
            datas = {"$set": {"polygons": polygons, "borough_ctype": 8, "updated": int(time.time())}}
            filter = {'_id': borough_id}
            polygon_update_action = self.update_by_filter(city=city, filter=filter, datas=datas)
            if polygon_update_action['nModified'] == 1:
                master_info.setdefault("master_info", {"status": 1, "borough_id": borough_id, "remark": "修改小区边界成功"})
            else:
                master_info.setdefault("master_info", {"status": -1, "borough_id": borough_id, "remark": "修改小区的边界失败"})
        else:
            master_info.setdefault("master_info", {"status": -1, "borough_id": borough_id, "remark": "传参有误"})
        return master_info

    # 新建小区
    def create_new_borough(self, data, city):
        add_dict = {}
        borough_name = data.get("borough_name", "")
        field = {'_id': 1, 'borough_name': 1, "borough_byname": 1, "borough_hide": 1, "source_url": 1}
        filter = {'$or': [{"borough_hide": borough_name}, {"borough_byname": borough_name}]}
        borough_datas = self.get_page(city=city, filter=filter, field=field)
        if len(borough_datas) == 1:
            exist_borough_id = borough_datas[0]['_id']
            add_dict.setdefault("master_info", {"status": -1, "borough_id": exist_borough_id,
                                                "remark": "小区id为%d存在相似名字" % exist_borough_id})
            return add_dict
        # 小区别名的检测
        byname_list = []
        byname_list.append(borough_name)
        borough_byname = data.get("borough_byname", "")
        if borough_byname.strip():
            byname_list.append(borough_byname)
        byname_list = list(set(byname_list))
        location = data.get("location", "")
        lng = location.get("lng", "")
        lat = location.get("lat", "")
        location = {"type": "Point", "coordinates": [float(lng), float(lat)]}
        bd_loc = self.gcj02_to_bd09(lng, lat)
        if bd_loc:
            bd_lng = bd_loc[0]
            bd_lat = bd_loc[1]
        else:
            bd_lng = 0
            bd_lat = 0
        loc = {"lng": bd_lng, "lat": bd_lat}
        cityarea = data.get("cityarea", "")
        borough_address = data.get("borough_address", "")
        bor_pinyin = BaseUtils.getFpy(borough_name, delimiter='')
        borough_completion = data.get("borough_completion", "")
        borough_volume = data.get("borough_volume", "")
        borough_green = data.get("borough_green", "")
        borough_area = data.get("borough_area", "")
        borough_company = data.get("borough_company", "")
        borough_developer = data.get("borough_developer", "")
        borough_costs = data.get("borough_costs", "")
        borough_property_right = data.get("borough_property_right", "")
        borough_totalnumber = data.get("borough_totalnumber", "")
        borough_totalbuilding = data.get("borough_totalbuilding", "")
        borough_buildingType = data.get("borough_buildingType", "")
        borough_info = {"borough_address": borough_address,
                        "borough_completion": borough_completion,
                        "borough_volume": borough_volume,
                        "borough_green": borough_green,
                        "borough_area": borough_area,
                        "borough_company": borough_company,
                        "borough_developer": borough_developer,
                        "borough_costs": borough_costs,
                        "borough_property_right": borough_property_right,
                        "borough_totalnumber": borough_totalnumber,
                        "borough_totalbuilding": borough_totalbuilding,
                        "borough_buildingType": borough_buildingType
                        }
        # 后续方法的改写
        borough_id = self.max_id.getMaxId(type="borough", city=city)
        current_time = BaseUtils.getIntTime()
        datas = {"borough_name": borough_name,
                 "borough_byname": byname_list,
                 "loc": loc,
                 "location": location,
                 "cityarea": cityarea,
                 "status": 1,
                 "bor_pinyin": bor_pinyin,
                 "_id": borough_id,
                 "borough_info": borough_info,
                 "borough_ctype": 8,
                 "created": current_time,
                 "updated": current_time
                 }
        # ES数据推送需要进一步改写
        es_data = {
            "cityarea_name": cityarea['cityarea']['cityarea_name'],
            "cityarea_id": cityarea['cityarea']['cityarea_id'],
            "cityarea2": cityarea['cityarea2'],
            "house_count": 0,
            "company": "",
            "pic": "",
            "borough_id": borough_id,
            "borough_name": borough_name,
            "traffic": 0,
            "subway": [],
            "price_rate": 0.00002,
            "borough_info": borough_info,
            "price": 0
        }
        # 添加小区基础数据成功
        new_borough_id = self.insert_one(datas=datas, city=city)
        if new_borough_id:
            # 添加小区列表es
            es_status = self.add_borough_list_es(borough_id=borough_id, es_data=es_data, city=city)
            # 添加小区联想词
            guess_status = self.guessword.add_guessword(city=city, guessword_list=[borough_name], type_id=3)
            # 添加小区二手房缓存
            borough_cache_status = self.update_borough_cache(city=city, master={"id": borough_id})
            # publish发布数据的消息
            message = {
                "city": city,
                "operate": "create",
                "type": "borough",
                "pms": {
                    "master": {
                        "id": borough_id,
                        "name": borough_name
                    },
                    "slave": []
                }
            }
            publish_status = self.publish_message("dm_info_change", message)
            add_dict.setdefault("master_info", {"status": 1, "borough_id": new_borough_id,
                                                "remark": "新建小区id为%s的成功" % new_borough_id,
                                                "es_status": es_status,
                                                "guess_status": guess_status,
                                                "borough_cache": borough_cache_status,
                                                "publish_status": publish_status
                                                })
        else:
            add_dict.setdefault("master_info", {"status": -1, "borough_id": False, "remark": "小区新建失败"})
        return add_dict

    def checkredis(self):
        # res = self.redis_conn.hset('jay', 'liuwei', 1)
        res = self.redis_conn.hget('jay', 'liuwei')
        # print(1111)
        print(res)
        # print(22222)

    # 删除小区
    def delete_borough(self, city, slave_ids):
        '''
        删除小区
        :return:
        '''
        delete_list = []
        for id in slave_ids:
            recycle_data = self.dao.find_one(city=city, filter={"_id": id})
            if not recycle_data:
                delete_list.append({"master_info": {"status": -1, "borough_id": id, "remark": "删除小区id为%s找不到数据" % (id)}})
                return delete_list
            del_action = self.dao.delete_by_id(city=city, filter={"_id": id})
            if del_action['n'] == 1:
                # 二手房缓存不用修改
                # 小区es列表删除
                index_name = "online_" + city + "_borough"
                if city == 'bj':
                    doc_name = "spider"
                else:
                    doc_name = "spider_" + city
                delete_status = self.essearch.delete_type(index=index_name, doc_type=doc_name, id=id)
                # 小区联想词删除
                guess_status = self.guessword.delete_guessword(city=city, guessword_list=[{"id": id}], type_id=3)
                insert_id = self.borough_recycle.insert_one(city=city, datas=recycle_data)
                delete_list.append({"master_info": {"status": 1, "borough_id": id, "remark": "删除小区id为%s的成功" % (id)}})
            else:
                delete_list.append({"master_info": {"status": -1, "borough_id": id, "remark": "删除小区id为%s的成功" % (id)}})
        return delete_list

    # 还原小区
    def reduction_borough(self, city, slave_ids):
        reduction_list = []
        for id in slave_ids:
            recycle_data = self.borough_recycle.get_one(city=city, filter={"_id": id})
            if not recycle_data:
                reduction_list.append({"master_info": {"status": -1, "borough_id": id, "remark": "还原小区id为%s找不到数据" % (id)}})
                return reduction_list
            del_action = self.borough_recycle.remove_by_id(city=city, filter={"_id": id})
            if del_action['n'] == 1:
                # 二手房缓存不用修改
                # 更新二手房小区缓存
                insert_id = self.dao.insert_one(city=city, datas=recycle_data)
                borough_cache_status = self.update_borough_cache(master={"id": id}, city=city)
                reduction_list.append({"master_info": {"status": 1, "borough_id": id, "remark": "还原小区id为%s的成功" % (id)}})
            else:
                reduction_list.append({"master_info": {"status": -1, "borough_id": id, "remark": "还原小区id为%s的成功" % (id)}})
        return reduction_list

    # 修改小区的配套
    def mod_borough_facility_by_id(self, city, borough_id, borough_info):
        master_info = {}
        current_time = BaseUtils.getIntTime()
        if borough_id and borough_info:
            borough_info_list = ["borough_developer", "borough_completion", "borough_totalarea", "borough_area",
                                 "borough_totalbuilding", "borough_totalnumber", "borough_green", "borough_volume",
                                 "borough_type", "borough_buildingType", "architectural","borough_property_right",
                                 "borough_address", "borough_tag","borough_postcode", "borough_company", "property_address",
                                 "property_phone", "borough_costs","property_desc", "parking_rate","parking_up_num",
                                 "parking_up_rent", "parking_up_price", "parking_under_num","parking_under_rent",
                                 "parking_under_price", "parking_under_address", "parking_type","entrance_mode", "door_num",
                                 "door_shunt", "door_toward","door_depth", "provide_water","provide_heating", "provide_electric",
                                 "provide_gas","provide_elevator","provide_network","hygiene","geo_addresss","baidu_address",
                                 "rim_kindergarten","rim_school","rim_university","rim_market","rim_hospital","rim_postoffice",
                                 "rim_bank","rim_other"
                                 ]
            for item in borough_info_list:
                borough_info[item] = borough_info.get(item, "")
            borough_update_action = self.dao.update(city=city, filter={'_id': borough_id}, datas={
                "$set": {"borough_info": borough_info, "borough_ctype": 8, "updated": current_time}})
            if borough_update_action['nModified'] == 1:
                master_info.setdefault("master_info", {"status": 1, "borough_id": borough_id, "remark": "修改配套信息成功"})
            else:
                master_info.setdefault("master_info", {"status": -1, "borough_id": borough_id, "remark": "修改配套信息失败"})
        return master_info

    # 根据小区id 获取房源量
    def get_borough_byname_housenum(self, city, borough_id):
        index_name = "online_" + city + "_house_sell"
        if city == "bj":
            doc_name = "spider"
        else:
            doc_name = "spider_" + city
        dsl = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "borough_id": borough_id
                            }
                        }
                    ]
                }
            },
            "size": 0,
            "sort": [],
            "aggs": {
                "count": {
                    "terms": {
                        "field": "borough_name",
                        "size": 100000
                    }
                }
            }
        }
        data = self.essearch.select_agg_one(index_name=index_name, doc_name=doc_name, dsl=dsl, agg_field='count')
        house_info = data.get("datas", '')
        house_list = []
        if house_info:
            for item in house_info:
                current_dict = {}
                current_dict.setdefault("name", item.get('key', ''))
                current_dict.setdefault("count", item.get('doc_count', ''))
                house_list.append(current_dict)
        return house_list
        # print data

    # 修改小区所属商圈
    def modifyBoroughInCommunity(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        modify_type = pms.get('modify_type')
        borough_id = pms.get('borough_id')
        community_id = pms.get('community_id')
        community = {}
        borough_filter = {'_id': borough_id}
        if modify_type == 1:
            community_filter = {'_id': community_id}
            community_info = self.communityDao.find_one(city=city, filter=community_filter)[0]
            community['community_name'] = community_info.get('name')
            community['community_id'] = community_info.get('_id')
            community['community_py'] = community_info.get('pinyin')
            borough_community = {'$set': {'community': community}}
            community = [community]
            # 修改小区的社区字段
            self.dao.update(city=city, filter=borough_filter, datas=borough_community)
        elif modify_type == -1:
            community = []
            borough_community = {'$unset': {'community': ''}}
            # 修改小区的社区字段
            self.dao.update(city=city, filter=borough_filter, datas=borough_community)
        else:
            result.setdefault('data', {"status": -1, "borough_id": borough_id, "remark": "修改类型错误"})
            return result

        # 更新小区缓存
        self.update_borough_cache(city=city, master={"id": borough_id})
        # 删除小区详情页缓存
        self.delete_borough_detail_cache(borough_id=borough_id, city=city)
        # 更新小区es社区字段
        self.mod_borough_list_es_community(city=city, borough_id=borough_id, community=community)
        # 更新房源es的社区
        self.mod_house_community(city=city, borough_id=borough_id, community=community)
        result.setdefault('data', {"status": 1, "borough_id": borough_id, "remark": "更新小区id为%s的社区成功" % (borough_id)})
        return result

    # 提供单个小区id的数据
    def get_recent_borough_data(self, city, name):
        data_info = {}
        if isinstance(name, int):
            find_borough = self.dao.find_one(city=city, filter={"_id": name})
        else:
            find_borough = self.dao.find_one(city=city,
                                             filter={'$or': [{"borough_hide": name}, {"borough_byname": name}]})
        if not find_borough:
            data_info.setdefault("status", 0)
            data_info.setdefault("message", "找不到该小区的数据")
            return data_info
        data = {}
        data['id'] = find_borough[0].get("_id", 0)
        data['name'] = find_borough[0].get("borough_name", "")
        data['loc'] = find_borough[0].get("loc", [])
        if data['name'] == "未知小区":
            data['cityarea'] = {}
            data['cityarea2'] = []
            data['subway'] = False
            data['config'] = []
            data['price'] = 0
            data_info['borough_info'] = data
            return data_info
        else:
            cityarea_id = find_borough[0]['cityarea']['cityarea']['cityarea_id']
            cityarea_name = find_borough[0]['cityarea']['cityarea']['cityarea_name']
            cityarea_py = find_borough[0]['cityarea']['cityarea']['cityarea_py']
            data['cityarea'] = {"cityarea_id": cityarea_id, "cityarea_name": cityarea_name, "cityarea_py": cityarea_py}
            data['cityarea2'] = find_borough[0]['cityarea']['cityarea2']
        if not isinstance(find_borough[0]['traffic'], dict):
            data['subway'] = False
        elif 'subway' not in find_borough[0]['traffic']:
            data['subway'] = False
        elif len(find_borough[0]['traffic']['subway']) > 0:
            data['subway'] = True
        else:
            data['subway'] = False
        if "borough_config" in find_borough[0]:
            data['config'] = find_borough[0]['borough_config']
        else:
            data['config'] = []
        data['price'] = self.price.getPriceByBoroughId(city=city, borough_id=data['id'])
        borough_byname = find_borough[0].get("borough_byname", [])
        borough_hide = find_borough[0].get("borough_hide", [])
        name_list = borough_byname + borough_hide
        data_info['status'] = 1
        data_info['borough_info'] = data
        return data_info

    def getBoroughDetail(self, city, borough_id):
        result = {}
        data = self.dao.getBoroughDetail(city=city, filter={"_id": borough_id}, borough_id=borough_id)
        result.setdefault('data', data)
        return result

    def getBoroughList(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        sort = pms.get('sort', {'id': 1})
        # 处理sort
        sort = [('_id', 1)] if sort.get('id', 1) == 1 else [('_id', -1)]
        field = pms.get('field', {'_id': 1, 'borough_name': 1, 'cityarea': 1, 'area_pinyin': 1})
        page = pms.get('page')
        index = page.get('index')
        size = page.get('size')
        filters = pms.get('filter', {})
        if 'borough_name' in filters and filters.get('borough_name', ''):
            filter = {"borough_name": {"$regex": filters.get('borough_name')}}
        elif 'cityarea_id' in filters and 'cityarea2_id' not in filters:
            filter = {"cityarea.cityarea.cityarea_id": filters.get('cityarea_id')}
        elif 'cityarea2_id' in filters:
            filter = {"cityarea.cityarea2.cityarea2_id": filters.get('cityarea2_id')}
        else:
            filter = {}
        total = self.dao.find_count(city=city, filter=filter)
        data = self.dao.getBoroughList(city=city, filter=filter, page=page, sort=sort, field=field)
        result.setdefault('data', data)
        result.setdefault('page', {"index": index, "size": size, "total": total})
        return result

    def getBoroughData(self, *args, **kwargs):
        result = {}
        info = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        filters = pms.get('filter', {})
        borough_id = filters.get('borough_id', '')
        data = self.dao.getBoroughDetail(city=city, filter={"_id": borough_id}, borough_id=borough_id)
        if data:
            data = data[0]
            info['layout_map'] = str(data.get('loc', '').get('lng', '')) + ',' + str(data.get('loc', '').get('lat', ''))
            info['addr'] = data.get('borough_info', '').get('borough_address', '')
            info['borough_name'] = data.get('borough_name', '')
            if data.get('traffic', '').get('subway', ''):
                info['line_name'] = data.get('traffic', '').get('subway', '')[0].get('subway_list_line', '')[0].get(
                    'line_name', '')
                info['station_name'] = data.get('traffic', '').get('subway', '')[0].get('subway_station_name', '')
                info['ranges'] = int(
                    data.get('traffic', '').get('subway', '')[0].get('distance', '').get('walk', '').get('time',
                                                                                                         '') / 60)
            else:
                info['line_name'] = ""
                info['station_name'] = ""
                info['range'] = ""
            info['borough_info'] = data.get('borough_info', '')
            result.setdefault('data', info)
            return result
        else:
            result.setdefault('data', {"status": -1, "borough_id": borough_id, "remark": "无此小区"})
            return result

    def getThreeLinePrice(self, *args, **kwargs):
        result = {}
        info = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        now = int(time.time())
        timeArray = time.localtime(now)
        otherStyleTime = time.strftime("%Y%m", timeArray)
        pms['filter']['endMonth'] = int(otherStyleTime) - 1
        pms['filter']['startMonth'] = int(time.strftime("%Y%m", time.localtime(int(time.time()) - 15552000)))
        pms['page'] = {"index": 1, "size": 12}
        filters = pms.get('filter', {})
        borough_id = filters.get('id', '')
        data = self.dao.getBoroughDetail(city=city, filter={"_id": borough_id}, borough_id=borough_id)
        if data:
            data = data[0]
            info['name'] = data.get('borough_name', '')
            info['cityarea'] = data.get('cityarea', '').get('cityarea', {})
            info['cityarea2'] = data.get('cityarea', '').get('cityarea2', [])
            borough_res = requests.post("http://borough.dapi.zhugefang.com/bj/borough/detail/boroughMonthPrice",
                                        data=json.dumps(pms)).content
            info['borough_price'] = eval(borough_res).get('data', '')
            if data.get('cityarea', '').get('cityarea', {}):
                pms['filter']['id'] = data.get('cityarea', '').get('cityarea', {}).get('cityarea_id')
                cityarea_res = requests.post("http://borough.dapi.zhugefang.com/bj/borough/detail/cityareaMonthPrice",
                                             data=json.dumps(pms)).content
                info['cityarea_price'] = eval(cityarea_res).get('data', '')
            if data.get('cityarea', '').get('cityarea2', []):
                pms['filter']['id'] = data.get('cityarea', '').get('cityarea2', {})[0].get('cityarea2_id')
                cityarea2_res = requests.post("http://borough.dapi.zhugefang.com/bj/borough/detail/cityarea2MonthPrice",
                                              data=json.dumps(pms)).content
                info['cityarea2_price'] = eval(cityarea2_res).get('data', '')
            result.setdefault('data', info)
            return result
        else:
            result.setdefault('data', {"status": -1, "borough_id": borough_id, "remark": "无此小区"})
            return result

    def getBoroughCacheByName(self, *args, **kwargs):
        result = {}
        pms = kwargs.get('pms')
        city = pms.get('city_en','')
        name = pms.get('name','')
        borough_key = f'{city}_borough'
        cache_data = self.redis_conn.hget(borough_key,BaseUtils.getMd5(name))
        if cache_data:
            cache_data = eval(cache_data.decode('utf-8'))
        else:
            cache_data = {"info":"缓存无该小区信息"}
        result.setdefault('data', cache_data)
        return result


    # 修改小区所属等级
    def modifyBoroughLevel(self, *args, **kwargs):
        result = {}
        pms = kwargs.get('pms')
        city = pms.get('city','')
        borough_id = pms.get('borough_id')
        level = pms.get('borough_level')
        borough_filter = {'_id': borough_id}
        if level in [0,1,2,3]:
            borough_level = {'$set': {'borough_level': level}}
            # 修改小区的社区字段
            self.dao.update(city=city, filter=borough_filter, datas=borough_level)
        else:
            result.setdefault('data', {"status": -1, "borough_id": borough_id, "remark": "修改类型错误"})
            return result

        # 更新小区缓存
        self.update_borough_cache(city=city, master={"id": borough_id})
        # 删除小区详情页缓存
        self.delete_borough_detail_cache(borough_id=borough_id, city=city)
        # 更新小区等级字段
        self.mod_borough_list_es_level(city=city, borough_id=borough_id, level=level)

        result.setdefault('data', {"status": 1, "borough_id": borough_id, "remark": "更新小区id为%s的社区成功" % (borough_id)})
        return result

    def get_nearby_borough(self, **kwargs):
        city = kwargs.get('city')
        pms = kwargs.get('pms', {})
        page = pms.get('page', {})
        borough_id = int(pms.get('borough_id', 0))
        distance = int(pms.get('distance', 0))
        result = self.dao.find_all(city=city, field=['borough_name', 'loc'], filter={"_id": borough_id})
        data = []
        if result:
            loc = result[0].get('loc', {})
            filter = {
                "location": {
                    "$near": {
                        "$geometry": {
                            "type": "Point",
                            "coordinates": [float(loc['lng']), float(loc['lat'])]
                        },
                        "$maxDistance": distance
                    }
                },
                "_id": {
                    "$ne": borough_id
                }
            }
            total = self.dao.count(city=city, filter=filter, page=page)
            data = self.dao.find_page(city=city, field=['cityarea', 'borough_name', '_id'], filter=filter, page=page)
            page.setdefault('total', total)
        return {'data': data, 'page': page}

    def get_nearby_borough_by_loc(self, **kwargs):
        city = kwargs.get("city", "")
        pms = kwargs.get("pms", {})
        page = pms.get("page", {})
        loc_lng = pms.get("loc_lng", 0.0)
        loc_lat = pms.get("loc_lag", 0.0)
        distance = int(pms.get("distance", 0))
        data = []
        if loc_lng and loc_lat and distance:
            filter = {
                "location": {
                    "$near": {
                        "$geometry": {
                            "type": "Point",
                            "coordinates": [float(loc_lng), float(loc_lat)]
                        },
                        "$maxDistance": distance
                    }
                },
            }
            total = self.dao.count(city=city, filter=filter, page=page)
            data = self.dao.find_page(city=city, field=['cityarea', 'borough_name', '_id'], filter=filter, page=page)
            page.setdefault('total', total)
        return {'data': data, 'page': page}

    def getRoomList(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        sort = pms.get('sort', {'id': 1})
        # 处理sort
        sort = [('_id', 1)] if sort.get('id', 1) == 1 else [('_id', -1)]
        field = pms.get('field', {'_id': 1, 'borough_name': 1, 'cityarea': 1, 'area_pinyin': 1})
        page = pms.get('page')
        index = page.get('index')
        size = page.get('size')
        filters = pms.get('filter', {})
        if 'borough_name' in filters and filters.get('borough_name', ''):
            filter = {"borough_name": {"$regex": filters.get('borough_name')}}
        elif 'cityarea_id' in filters and 'cityarea2_id' not in filters:
            filter = {"cityarea.cityarea.cityarea_id": filters.get('cityarea_id')}
        elif 'cityarea2_id' in filters:
            filter = {"cityarea.cityarea2.cityarea2_id": filters.get('cityarea2_id')}
        else:
            filter = {}
        total = self.dao.find_count(city=city, filter=filter)
        data = self.dao.getBoroughList(city=city, filter=filter, page=page, sort=sort, field=field)
        for borough_info in data:
            borough_id = borough_info.get('_id')
            # self.jjrxtXiaoquService.select_by_filter()
        result.setdefault('data', data)
        result.setdefault('page', {"index": index, "size": size, "total": total})
        return result




if __name__ == '__main__':
    # print(json.dumps(BoroughOnlineService(city="bj").update_borough_cache(city='bj', master={'id': 1006067})))
    exit()
