#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from utils.BaseUtils import BaseUtils
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.service.detail.CityareaService import CityareaService
from apps.config.CitySouceMapping import city_mapping
from apps.zhuge_borough.service.search.BoroughSearchService import BoroughSearchService
from apps.zhuge_borough.dao.detail.CityareaDao import CityareaDao
from apps.zhuge_borough.dao.detail.Cityarea2Dao import Cityarea2Dao
from apps.zhuge_borough.dao.detail.BoroughOnlineDao import BoroughOnlineDao
from apps.zhuge_borough.service.detail.Cityarea2RecycleService import Cityarea2RecycleService
from apps.zhuge_borough.service.detail.BoroughMonthPriceService import BoroughMonthPriceService
from apps.zhuge_borough.service.detail.BoroughMaxIdService import BoroughMaxIdService
from databases.dbfactory.dbfactory import dbfactory
from apps.config.Config import redis_conf
import json
class Cityarea2Service(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.cityarea_dao = CityareaDao(*args, **kwargs)
        self.dao = Cityarea2Dao(*args, **kwargs)
        self.boroughline_dao = BoroughOnlineDao(*args, **kwargs)
        self.cityarea_service = CityareaService()
        self.max_id = BoroughMaxIdService()
        self.price = BoroughMonthPriceService()
        # self.boroug_online = BoroughOnlineService()
        # 更新小区缓存的链接库
        redis_config = redis_conf.get('borough_info_redis')
        self.redis_conn = dbfactory.create_db(db_config=redis_config, db_type='db_redis')
        self.essearch = BoroughSearchService()
        self.cityarea2_recycle = Cityarea2RecycleService()


    # 根据指定商圈id获取数据
    def getcityarea2_data(self,city, cityarea2_id, field):
        filter = {'_id':cityarea2_id}
        data = self.get_page(city=city, field=field, filter=filter)
        return data

    # 根据指定id删除数据
    def delete_by_id(self,city, cityarea2_id):
        filter = {'_id': cityarea2_id}
        data = self.remove_by_id(city=city, filter=filter)
        return data

    def update_filterall(self,filter,datas,city):
        result = self.update_by_filterall(filter=filter, datas=datas, city=city)
        return result

    def find(self, city, filter):
        data = self.get_one(city=city, filter=filter)
        return data

    # 增加商圈
    def add_cityarea2(self, city, cityarea_id, new_cityarea2):
        add_dict = {}
        new_cityarea2_dict = {}
        current_time = BaseUtils.getIntTime()
        new_cityarea2_dict['_id'] = self.max_id.getMaxId(type="cityarea2",city=city)
        new_cityarea2_dict['loc'] = new_cityarea2.get("loc", "")
        new_cityarea2_dict['name'] = new_cityarea2.get("name", "").strip()
        new_cityarea2_dict['area2_pinyin'] = BaseUtils.getFpy(new_cityarea2_dict['name'])
        new_cityarea2_dict['polygons'] = new_cityarea2.get("polygons", "")
        new_cityarea2_dict['statu'] = 1
        new_cityarea2_dict['created'] = current_time
        new_cityarea2_dict['updated'] = current_time
        is_find = self.dao.find_one(city=city, filter={"name": {"$regex": new_cityarea2_dict['name']}})
        if is_find:
            cityarea2_id = is_find.get("_id", "")
            add_dict['master_info'] = {"status": 0, "cityarea2_id": False,"remark": "商圈id%s存在相似商圈" % (cityarea2_id)}
            return add_dict
        else:
            insert_id = self.dao.insert_one(city=city,datas=new_cityarea2_dict)
            if not insert_id:
                add_dict['master_info'] = {"status": 0, "cityarea2_id": insert_id, "remark": "商圈添加失败"}
                return add_dict
            find_cityarea = self.cityarea_dao.find_one(city=city,filter={"_id": cityarea_id})
            if not find_cityarea:
                add_dict['master_info'] = {"status": 0, "cityarea2_id": insert_id, "remark": "添加到城区失败"}
                return add_dict
            list_cityarea2 = find_cityarea[0].get("list_cityarea2", "")
            list_cityarea2[new_cityarea2_dict['name']] = new_cityarea2_dict['_id']
            cityarea_update_action = self.cityarea_dao.update(city=city,filter={'_id': cityarea_id}, datas={"$set": {"list_cityarea2": list_cityarea2, "updated": current_time}})
            if cityarea_update_action['nModified'] == 1:
                # 添加商圈联想词
                #guess_status = self.guessword.add_guessword(city=city, guessword_list=[new_cityarea2_dict['name']], type_id=2)
                add_dict['master_info'] = {"status": 1, "cityarea2_id": insert_id, "remark": "添加商圈并同步到城区表成功"}
            else:
                add_dict['master_info'] = {"status": 0, "cityarea2_id": insert_id, "remark": "添加商圈成功,同步到城区表失败"}
        return add_dict

    # 删除商圈
    def delete_cityarea2(self, city, cityarea, cityarea2):
        cityarea_id = cityarea.get("id","")
        cityarea2_id = cityarea2.get("id","")
        cityarea2_name = cityarea2.get("name", "")
        current_time = BaseUtils.getIntTime()
        update_dict = {}
        is_find = self.boroughline_dao.find_one(city=city,filter={"cityarea.cityarea2.cityarea2_id": cityarea2_id})
        if is_find:
            update_dict['master_info'] = {"status": 0, "cityarea2_id": cityarea2_id, "remark": "该商圈下有小区数据" % (cityarea2_id)}
            return update_dict
        if cityarea2_id:
            find_cityarea = self.cityarea_dao.find_one(city=city,filter={"_id": cityarea_id})
            if not find_cityarea:
                update_dict['master_info'] = {"status": 0, "cityarea_id": cityarea_id, "remark": "城区id为%s的城区找不到" % (cityarea_id)}
                return update_dict
            find_cityarea2 = self.dao.find_one(city=city,filter={"_id": cityarea2_id})
            if not find_cityarea2:
                update_dict['master_info'] = {"status": 0, "cityarea2_id": cityarea2_id, "remark": "城区id为%s的城区找不到" % (cityarea_id)}
                return update_dict
            cityarea2_action = self.dao.delete_by_id(city=city,filter={"_id":cityarea2_id})
            insert_action = self.cityarea2_recycle.insert_one(city=city, datas=find_cityarea2)
            if cityarea2_action['ok'] == 0:
                update_dict["master_info"] = {"status": 0, "cityarea2_id": cityarea2_id, "remark":"商圈id为%s的删除失败" % (cityarea2_id)}
                return update_dict
            list_cityarea2 = find_cityarea[0].get("list_cityarea2","")
            for k, v in list_cityarea2.items():
                if v == cityarea2_id:
                    list_cityarea2.pop(k)
                    break
            cityarea_update_action = self.cityarea_dao.update(city=city,filter={'_id': cityarea_id}, datas={
                "$set": {"list_cityarea2": list_cityarea2, "updated": current_time}})
            if cityarea_update_action['nModified'] == 1:
                # 删除商圈联想词
                #guess_status = self.guessword.delete_guessword(keyword_list=[cityarea2_name],type_id=2)
                # publish信息
                message = {
                    "city": city,
                    "operate": "delete",
                    "type": "cityarea2",
                    "pms": {
                        "slave": [{"id": cityarea2_id, "name": cityarea2_name}]
                    }
                }
                push_status = self.publish_message("dm_info_change", message)
                update_dict['master_info'] = {"status": 1, "cityarea2_id": cityarea2_id, "remark": "删除商圈并同步到城区表成功","push_status":push_status}
            else:
                update_dict['master_info'] = {"status": 0, "cityarea2_id": cityarea2_id, "remark": "删除商圈成功,同步到城区表失败"}
            return update_dict
        else:
            update_dict['master_info'] = {"status": 0, "cityarea2_id": False, "remark": "商圈id传的有误"}
            return update_dict

    # 修改商圈名称
    def mod_cityarea2_name(self, city, cityarea_id, cityarea2_info):
        current_time = BaseUtils.getIntTime()
        cityarea2_id = cityarea2_info.get("cityarea2_id", "")
        name = cityarea2_info.get("new_name", "")
        cityarea2_py = BaseUtils.getFpy(name)
        update_dict = {}
        if cityarea2_id:
            cityarea2_action = self.dao.update(city=city,filter={'_id': cityarea2_id},datas={"$set": {"name": name, "updated": current_time}})
            if cityarea2_action['nModified'] == 0:
                update_dict["master_info"] = {"status": 0, "cityarea2_id": cityarea2_id,
                                              "remark": "商圈id为%s的更新失败" % (cityarea2_id)}
                return update_dict
            find_cityarea = self.cityarea_dao.find_one(city=city,filter={"_id": cityarea_id})
            if not find_cityarea:
                update_dict['master_info'] = {"status": 0, "cityarea2_id": cityarea2_id,
                                              "remark": "城区id为%s的城区找不到" % (cityarea_id)}
                return update_dict
            list_cityarea2 = find_cityarea.get("list_cityarea2", "")
            for k, v in list_cityarea2.items():
                if v == cityarea2_id:
                    list_cityarea2.pop(k)
                    list_cityarea2[name] = v
                    break
            cityarea_update_action = self.cityarea_dao.update(city=city,filter={'_id': cityarea_id}, datas={
                "$set": {"list_cityarea2": list_cityarea2, "updated": current_time}})
            cityarea2_update_action = self.dao.update(city=city,filter={'_id': cityarea2_id}, datas={
                "$set": {"name": name, "area2_pinyin": cityarea2_py, "updated": current_time}})
            if cityarea_update_action['nModified'] == 1:
                # 小区的缓存修改
                cityarea2 = {'id': cityarea2_id}
                # 修改商圈的小区
                filter = {"cityarea.cityarea2.cityarea2_id": cityarea2_id}
                cityarea_name = find_cityarea.get("name", "")
                cityarea_py = find_cityarea.get("area_pinyin", "")
                cityarea_id = find_cityarea.get("_id", "")
                master_cityarea_info = {
                    "cityarea": {
                        "cityarea_name": cityarea_name,
                        "cityarea_id": cityarea_id,
                        "area_pinyin": cityarea_py,
                        "cityarea_py": cityarea_py
                    },
                    "cityarea2": [
                        {
                            "cityarea2_name": name,
                            "cityarea2_py": cityarea2_py,
                            "cityarea2_id": cityarea2_id,
                            "area2_pinyin": cityarea2_py
                        }
                    ]
                }
                find_borough_by_cityarea2 = self.boroughline_dao.find_one(city=city,filter={"cityarea.cityarea2.cityarea2_id": cityarea2_id})
                if not find_borough_by_cityarea2:
                    update_dict['master_info'] = {"status": 1, "cityarea2_id": cityarea2_id,
                                                  "remark": "更新商圈并同步到城区表成功-该商圈下无小区"}
                    return update_dict
                datas = {"$set": {"cityarea": master_cityarea_info, "borough_ctype": 8, "updated": current_time}}
                update_action = self.boroughline_dao.updateAll(city=city,filter=filter, datas=datas)
                self.update_area_cache(city=city,cityarea2=cityarea2)
                # 小区es列表页
                master_cityarea2 = [
                    {"cityarea2_id": cityarea2_id, "cityarea2_name": name, "cityarea2_py": cityarea2_py}]
                updateBody = {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "nested": {
                                        "path": "cityarea2",
                                        "query": {
                                            "bool": {
                                                "must": [
                                                    {
                                                        "term": {
                                                            "cityarea2.cityarea2_id": cityarea2_id
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            ],
                        }
                    },
                    "script": {
                        "inline": "ctx._source.cityarea2 = params.cityarea2",
                        "params": {
                            "cityarea2": master_cityarea2
                        },
                        "lang": "painless"
                    }
                }
                if city == 'bj':
                    index_doc = "spider"
                else:
                    index_doc = "spider_" + city
                borough_index = "online_" + city + "_borough"
                borough_list_status = self.essearch.update_by_query(index=borough_index, doc_type=index_doc,
                                                                           body=updateBody)
                # borough_list_status = True
                # 房源的批量修改
                house_index = "online_" + city + "_house_sell"
                house_list_status = self.essearch.update_by_query(index=house_index, doc_type=index_doc,
                                                                         body=updateBody)
                # house_list_status = True
                # 商圈联想词 变更
                # guess_status = self.guessword.modify_borough_name(keyword_dict={origin_name: master_name})
                # publish信息
                message = {
                    "city": city,
                    "operate": "update",
                    "type": "cityarea2",
                    "pms": {
                        "master": {"id": cityarea2_id, "name": name}
                    }
                }
                push_status = self.publish_message("dm_info_change", message)
                update_dict['master_info'] = {"status": 1, "cityarea2_id": cityarea2_id, "remark": "更新商圈并同步到城区表成功"}
            else:
                update_dict['master_info'] = {"status": 0, "cityarea2_id": cityarea2_id,
                                              "remark": "更新商圈成功,同步到城区表失败"}
            return update_dict
        else:
            update_dict['master_info'] = {"status": 0, "cityarea2_id": False, "remark": "商圈id传的有误"}
            return update_dict

    # 修改商圈所属城区
    def mod_cityarea2_relation(self, city, before_cityarea_info, after_cityarea_info):
        current_time = BaseUtils.getIntTime()
        update_dict = {}
        before_cityarea_id = before_cityarea_info.get("cityarea_id", "")
        before_cityarea2_id = before_cityarea_info.get("cityarea2_id", "")
        before_cityarea2_name = before_cityarea_info.get("cityarea2_name", "")
        before_cityarea2_py = BaseUtils.getFpy(before_cityarea2_name)
        after_cityarea_id = after_cityarea_info.get("cityarea_id", "")
        after_cityarea_name = after_cityarea_info.get("cityarea_name", "")
        after_cityarea_py = BaseUtils.getFpy(after_cityarea_name)
        if before_cityarea_id:
            find_before_cityarea = self.cityarea_dao.find_one(city=city,filter={"_id": before_cityarea_id})
            if not find_before_cityarea:
                update_dict['master_info'] = {"status": 0, "cityarea2_id": before_cityarea_id,
                                              "remark": "城区id为%s的城区找不到" % (before_cityarea_id)}
                return update_dict
            list_cityarea2 = find_before_cityarea.get("list_cityarea2", "")
            for k, v in list_cityarea2.items():
                if k == before_cityarea2_name:
                    list_cityarea2.pop(k)
                    break
            cityarea_update_action = self.cityarea_dao.update(city=city,filter={'_id': before_cityarea_id}, datas={
                "$set": {"list_cityarea2": list_cityarea2, "updated": current_time}})
            if cityarea_update_action['nModified'] == 0:
                update_dict['master_info'] = {"status": 0, "cityarea2_id": before_cityarea_id,
                                              "remark": "城区更新失败"}
                return update_dict
            find_after_cityarea = self.cityarea_dao.find_one(city=city, filter={"_id": after_cityarea_id})
            after_list_cityarea2 = find_after_cityarea.get("list_cityarea2", "")
            after_list_cityarea2[before_cityarea2_name] = before_cityarea2_id
            after_cityarea_update_action = self.cityarea_dao.update(filter={'_id': after_cityarea_id}, datas={
                "$set": {"list_cityarea2": after_list_cityarea2, "updated": current_time}})
            if after_cityarea_update_action['nModified'] == 1:
                # 二手房小区缓存
                # cityarea = {'id': after_cityarea_id}
                # self.borough_online.update_area_cache(cityarea=cityarea)
                updateBody = {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "nested": {
                                        "path": "cityarea2",
                                        "query": {
                                            "bool": {
                                                "must": [
                                                    {
                                                        "term": {
                                                            "cityarea2.cityarea2_id": before_cityarea2_id
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            ],
                        }
                    },
                    "script": {
                        "inline": "ctx._source.cityarea_id = params.cityarea_id;ctx._source.cityarea_name = params.cityarea_name",
                        "params": {
                            "cityarea_id": after_cityarea_id,
                            "cityarea_name": after_cityarea_name
                        },
                        "lang": "painless"
                    }
                }
                if city == 'bj':
                    index_doc = "spider"
                else:
                    index_doc = "spider_" + city
                borough_index = "online_" + city + "_borough"
                borough_list_status = self.essearch.update_by_query(index=borough_index,
                                                                           doc_type=index_doc, body=updateBody)
                # borough_list_status = True
                # 房源的批量修改
                house_index = "online_" + city + "_house_sell"
                house_list_status = self.essearch.update_by_query(index=house_index, doc_type=index_doc,
                                                                         body=updateBody)
                # 改变商圈所属城区
                filter = {"cityarea.cityarea2.cityarea2_id": before_cityarea2_id}
                master_cityarea_info = {
                    "cityarea": {
                        "cityarea_name": after_cityarea_name,
                        "cityarea_id": after_cityarea_id,
                        "area_pinyin": after_cityarea_py,
                        "cityarea_py": after_cityarea_py
                    },
                    "cityarea2": [
                        {
                            "cityarea2_name": before_cityarea2_name,
                            "cityarea2_py": before_cityarea2_py,
                            "cityarea2_id": before_cityarea2_id,
                            "area2_pinyin": before_cityarea2_py
                        }
                    ]
                }
                datas = {
                    "$set": {"cityarea": master_cityarea_info, "borough_ctype": 8, "updated": current_time}}
                update_action = self.boroughline_dao.updateAll(city=city,filter=filter, datas=datas)
                # 更新缓存
                cityarea2 = {'id': before_cityarea2_id}
                self.update_area_cache(city=city,cityarea2=cityarea2)
                update_dict['master_info'] = {"status": 1, "cityarea2_id": before_cityarea_id,
                                              "remark": "更新城区表成功"}
            else:
                update_dict['master_info'] = {"status": 0, "cityarea2_id": before_cityarea_id,
                                              "remark": "更新城区表失败"}
            return update_dict
        else:
            update_dict['master_info'] = {"status": 0, "cityarea2_id": False, "remark": "城区id传的有误"}
            return update_dict

    # 合并商圈
    def merge_cityarea(self, city, master_cityarea, slave_cityarea):
        try:
            # 商圈表 -----> 商圈的归并
            # 支持跨城区合并  同城区相当于删除商圈
            current_time = BaseUtils.getIntTime()
            delete_dict = {}
            update_dict = {}
            error_dict = {}
            mater_cityarea_id = master_cityarea.get("cityarea_id", "")
            mater_cityarea_name = master_cityarea.get("cityarea_name", "")
            mater_cityarea_py = BaseUtils.getFpy(mater_cityarea_name, delimiter='')
            mater_cityarea2_id = master_cityarea.get("cityarea2_id", "")
            mater_cityarea2_name = master_cityarea.get("cityarea2_name", "")
            mater_cityarea2_py = BaseUtils.getFpy(mater_cityarea2_name, delimiter='')
            slave_cityarea_id = slave_cityarea.get("cityarea_id", "")
            slave_cityarea_name = slave_cityarea.get("cityarea_name", "")
            slave_cityarea2_id = slave_cityarea.get("cityarea2_id", "")
            slave_cityarea2_name = slave_cityarea.get("cityarea2_name", "")
            slave_cityarea2_py = BaseUtils.getFpy(slave_cityarea2_name, delimiter='')
            master_cityarea2 = [{"cityarea2_id": mater_cityarea2_id, "cityarea2_name": mater_cityarea2_name, "cityarea2_py": mater_cityarea2_py}]
            master_cityarea_data = self.cityarea_service.find(city=city, filter={'_id':mater_cityarea_id})
            slave_cityarea_data = self.cityarea_service.find(city=city, filter={'_id':slave_cityarea_id})
            if not master_cityarea_data:
                error_dict = {"status": 1, "remark": "主城区的城区找不到数据"}
                return {"update_dict": update_dict, "delete_dict": delete_dict, "error_dict": error_dict}
            if not slave_cityarea_data:
                error_dict = {"status": 1, "remark": "从城区的城区找不到数据"}
                return {"update_dict": update_dict, "delete_dict": delete_dict, "error_dict": error_dict}
            find_cityarea2 = self.find(city=city, filter={'_id':slave_cityarea2_id})
            if find_cityarea2:
                self.cityarea2_recycle.insert_one(city=city, datas=find_cityarea2)
            delete_status = self.delete_by_id(city=city,cityarea2_id=slave_cityarea2_id)
            if delete_status['ok'] == 1:
                delete_dict = {"id": slave_cityarea2_id, "status": 1, "remark": "商圈删除成功"}
            else:
                delete_dict = {"id": slave_cityarea2_id, "status": 0, "remark": "商圈删除失败"}
            slave_list_cityarea2 = slave_cityarea_data[0].get("list_cityarea2", "")
            for k in list(slave_list_cityarea2.keys()):
                if slave_list_cityarea2[k] == slave_cityarea2_id:
                    slave_list_cityarea2.pop(k)
                    continue
            datas = {"$set": {"list_cityarea2": slave_list_cityarea2, "updated": current_time}}
            slave_cityarea_update_action = self.cityarea_service.update_filter(filter={'_id': slave_cityarea_id},city=city, datas=datas )
            if slave_cityarea_update_action['nModified'] == 1:
                update_dict = {"id":slave_cityarea_id,"status": 1, "remark": "更新slave城区表成功" }
            else:
                update_dict = {"id":slave_cityarea_id,"status": 0, "remark": "更新slave城区表失败" }
            # 更改小区的城区商圈
            master_cityarea_info = {"cityarea_id": mater_cityarea_id, "cityarea_name": mater_cityarea_name, "area_pinyin": mater_cityarea_py, "cityarea2_id": mater_cityarea2_id, "cityarea2_name": mater_cityarea2_name, "area2_pinyin": mater_cityarea2_py}
            slave_cityarea_info = {"cityarea2_id": slave_cityarea2_id, "cityarea2_name": slave_cityarea2_name,"area2_pinyin": slave_cityarea2_py}
            borough_online_status = self.change_borough_online_cityarea(master_cityarea=master_cityarea_info, slave_cityarea=slave_cityarea_info,city=city)
            if not borough_online_status:
                error_dict = {"status": 1, "remark": "borough_online的城区商圈更改失败"}
            # 更改小区缓存
            self.update_area_cache(city=city, cityarea2={"id": slave_cityarea2_id})
            # 小区列表es拼装的数据
            updateBody = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "nested": {
                                    "path": "cityarea2",
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {
                                                    "term": {
                                                        "cityarea2.cityarea2_id": slave_cityarea2_id
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                }
                            }
                        ],
                    }
                },
                "script": {
                    "inline": "ctx._source.cityarea_id = params.cityarea_id;ctx._source.cityarea_name = params.cityarea_name;ctx._source.cityarea2 = params.cityarea2",
                    "params": {
                        "cityarea_id": mater_cityarea_id,
                        "cityarea_name": mater_cityarea_name,
                        "cityarea2": master_cityarea2
                    },
                    "lang": "painless"
                }
            }
            if city == 'bj':
                index_doc = "spider"
            else:
                index_doc = "spider_" + city
            borough_index = "online_" + city + "_borough"
            borough_list_status = self.essearch.update_by_query(index=borough_index, doc_type=index_doc, body=updateBody)
            # 房源的批量修改
            house_index = "active_" + city + "_house_sell"
            house_list_status = self.essearch.update_by_query(index=house_index, doc_type=index_doc, body=updateBody)
            update_info = {"delete_dict": delete_dict, "update_dict": update_dict, "error_dict": error_dict, "borough_es": borough_list_status, "house_es": house_list_status}
            return update_info
        except Exception as e:
            print("update_fail", e)
            return False

    # 根据商圈找小区,更新小区的数据
    def change_borough_online_cityarea(self, master_cityarea,slave_cityarea,city):
        current_time = BaseUtils.getIntTime()
        mater_cityarea_id = master_cityarea.get("cityarea_id", "")
        mater_cityarea_name = master_cityarea.get("cityarea_name", "")
        mater_area_pinyin = master_cityarea.get("area_pinyin", "")
        mater_cityarea2_id = master_cityarea.get("cityarea2_id", "")
        mater_cityarea2_name = master_cityarea.get("cityarea2_name", "")
        mater_area2_pinyin = master_cityarea.get("area2_pinyin", "")
        slave_cityarea_cityarea2_id = slave_cityarea.get("cityarea2_id", "")
        filter = {"cityarea.cityarea2.cityarea2_id": slave_cityarea_cityarea2_id}
        master_cityarea_info = {
            "cityarea": {
                "cityarea_name": mater_cityarea_name,
                "cityarea_id": mater_cityarea_id,
                "area_pinyin": mater_area_pinyin,
                "cityarea_py": mater_area_pinyin
            },
            "cityarea2": [
                {
                    "cityarea2_name": mater_cityarea2_name,
                    "cityarea2_py": mater_area2_pinyin,
                    "cityarea2_id": mater_cityarea2_id,
                    "area2_pinyin": mater_area2_pinyin
                }
            ]
        }
        datas = {"$set": {"cityarea": master_cityarea_info, "borough_ctype": 8, "updated": current_time}}
        filter = {"cityarea.cityarea2.cityarea2_id": slave_cityarea_cityarea2_id}
        find_borough_by_cityarea2 = self.boroughline_dao.find_one(city=city, filter=filter)
        if not find_borough_by_cityarea2:
            return True
        update_action = self.boroughline_dao.updateAll(filter=filter, datas=datas, city=city)
        if update_action['nModified'] > 0:
            return True
        else:
            return False

    # 生成小区缓存数据
    def generate_cache_data(self, name, city):
        data_info = {}
        if isinstance(name, int):
            find_borough = self.boroughline_dao.find_one(filter={"_id": name}, city=city)[0]
        else:
            find_borough = self.boroughline_dao.find_one(filter={'borough_byname': name}, city=city)[0]
        if not find_borough:
            data_info['status'] = 0
            data_info['message'] = '找不到该小区'
            return data_info
        data_info['id'] = find_borough.get('_id', 0)
        data_info['name'] = find_borough.get('borough_name', '')
        data_info['loc'] = find_borough.get('loc', [])
        cityarea_id = find_borough['cityarea']['cityarea']['cityarea_id']
        cityarea_name = find_borough['cityarea']['cityarea']['cityarea_name']
        data_info['cityarea'] = {"cityarea_id": cityarea_id, "cityarea_name": cityarea_name}
        if 'cityarea2' in find_borough['cityarea']:
            data_info['cityarea2'] = find_borough['cityarea']['cityarea2']
        if "traffic" in find_borough:
            if not isinstance(find_borough['traffic'], dict):
                data_info['subway'] = False
            elif 'subway' not in find_borough['traffic']:
                data_info['subway'] = False
            elif len(find_borough['traffic']['subway']) > 0:
                data_info['subway'] = True
            else:
                data_info['subway'] = False
        if "borough_config" in find_borough:
            data_info['config'] = find_borough['borough_config']
        else:
            data_info['config'] = []
            data_info['price'] = self.price.getPriceByBoroughId(borough_id=data_info['id'], city=city)
        borough_byname = find_borough.get("borough_byname", [])
        borough_hide = find_borough.get("borough_hide", [])
        name_list = borough_byname + borough_hide
        data_info['name_list'] = name_list
        return data_info

    # 更新单个小区缓存
    def update_borough_cache(self, city, master={}, slave={}):
        try:
            key = "%s_borough" % city_mapping.get(city)['fpy']
            if slave == {}:
                if master.get('id', ''):
                    datas = self.generate_cache_data(name=master.get('id'), city=city)
                else:
                    datas = self.generate_cache_data(name=master.get('name'), city=city)
                name_list = datas.pop('name_list')
                for borough_name in name_list:
                    self.redis_conn.hset(key, BaseUtils.getMd5(borough_name), json.dumps(datas))
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
                datas = self.boroughline_dao.find_one(filter={"cityarea.cityarea2.cityarea2_id": cityarea2_id}, city=city)
            elif cityarea_id:
                datas = self.boroughline_dao.find_one(filter={"cityarea.cityarea.cityarea_id": cityarea_id}, city=city)
            elif cityarea_name:
                datas = self.boroughline_dao.find_one(filter={"cityarea.cityarea.cityarea_name": cityarea_name}, city=city)
            elif cityarea2_name:
                datas = self.boroughline_dao.find_one(filter={"cityarea.cityarea2.cityarea2_name": cityarea2_name}, city=city)
            else:
                datas = []
            for i in datas:
                self.update_borough_cache(city=city, master={'name': i.get('borough_name')})
        except Exception as e:
            print(e)
            return False

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

    #获取商圈列表
    def getCityarea2List(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        sort = pms.get('sort',{})
        #处理sort
        sort = [('_id', 1)] if sort.get('id', 1) == 1 else [('_id', -1)]
        page = pms.get('page')
        index = page.get('index')
        size = page.get('size')
        filter = pms.get('filter', {})
        field = {'_id': 1, 'name': 1, 'loc': 1, 'area2_pinyin': 1}  # 返回字段
        total = self.dao.find_count(city=city, pms=pms)
        data = self.dao.getCityarea2ListByCity(city=city, filter=filter, page=page, sort=sort, field=field)
        result.setdefault('data', data)
        result.setdefault('page', {"index": index, "size": size, "total": total})
        return result

    #   根据id获取商圈详情
    def getCityarea2InfoById(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        cityarea2_id = kwargs.get('id')
        cityarea_info = self.dao.getCityarea2InfoById(city=city, cityarea2_id=cityarea2_id)
        result.setdefault('data', cityarea_info)
        return result



