#!/usr/bin/env python
from apps.zhuge_borough.dao.detail.BoroughOnlineDao import BoroughOnlineDao
from apps.zhuge_borough.service.detail.CityareaService import CityareaService
from apps.zhuge_newhouse.service.search.ComplexSearchService import ComplexSearchService
from apps.zhuge_borough.service.detail.Cityarea2Service import Cityarea2Service
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.GuessWordDao import GuessWordDao
from utils.BaseUtils import BaseUtils
import time


class BoroughGuesswordService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        # city = kwargs.get('city')
        # db_name = f"newhouse_{city}"
        self.guessdao = GuessWordDao(*args, **kwargs)
        self.onlinedao = BoroughOnlineDao(*args, **kwargs)
        self.complexsearchservice = ComplexSearchService()
        self.area_list = {
            1: CityareaService(),
            2: Cityarea2Service(),
            3: self.onlinedao
        }


    def add_guessword(self, *args, **kwargs):
        city = kwargs.get('city')
        guessword_list = kwargs.get('guessword_list')
        type_id = kwargs.get('type_id')
        result = dict()
        for guessword in guessword_list:
            guessword = guessword
            borough_id = self.get_borough_id(city=city, guessword=guessword, type_id=type_id)
            if borough_id:
                guessword_status = self.get_guessword(city=city, guessword=guessword, type_id=type_id)
                # 如果联想词不存在添加联想词同步到es
                if not guessword_status:
                    self.save_mongo(city,type_id, guessword, borough_id)
                    self.push_es(city,type_id, guessword)
                    result.setdefault(guessword, True)
                else:
                    # 如果联想词存在更新联想词同步到es
                    # result.setdefault(guessword, False)
                    data = self.guessword_list(city, type_id, guessword, borough_id)
                    data.pop('created')
                    self.guessdao.update(
                        filter={'other_id': borough_id, 'keyword': guessword, 'type_id': type_id},
                        datas={'$set': {'datas': data}}, city=city)
                    self.push_es(city, type_id, guessword)
                    result.setdefault(guessword, True)
            else:
                result.setdefault(guessword, False)
        return result

    def update_guessword(self, *args, **kwargs):
        city = kwargs.get('city')
        keyword_list = kwargs.get('guessword_list')
        type_id = kwargs.get('type_id')
        result = dict()
        for keyword in keyword_list:
            keyword = keyword
            borough_id = self.get_borough_id(city=city, guessword=keyword, type_id=type_id)
            guessword_status = self.get_guessword(city=city, guessword=keyword, type_id=type_id)
            if borough_id and guessword_status:
                data = self.guessword_list(city, type_id, keyword, borough_id)
                data.pop('created')
                self.guessdao.update(
                    filter={'other_id': borough_id, 'keyword': keyword, 'type_id': type_id}, datas={'$set': {'datas': data}}, city=city)
                self.push_es(city,type_id, keyword)
                result.setdefault(keyword, True)
            else:
                result.setdefault(keyword, False)
        return result

    def delete_guessword(self, *args, **kwargs):
        city = kwargs.get('city')
        keyword_list = kwargs.get('guessword_list')
        type_id = kwargs.get('type_id')
        result = dict()
        for keyword_info in keyword_list:
            keyword = keyword_info.get("name","")
            other_id = keyword_info.get("id", 0)
            where = {'type_id': type_id}
            if other_id:
                where.setdefault('other_id', other_id,)
            else:
                where.setdefault('keyword', keyword)
            if not other_id and not keyword:
                result.setdefault(keyword, False)
                return result
            guessword_status = self.guessdao.find_one(city=city, filter=where)
            if guessword_status:
                _id = guessword_status[0]['_id']
                self.guessdao.delete_by_id(city=city, filter={'_id':_id})
                self.delete_es(city,self.complexsearchservice.online_keyword_alias, str(_id))
                result.setdefault(keyword, True)
            else:
                result.setdefault(keyword, False)
        return result

    def modify_borough_name(self, *args, **kwargs):
        city = kwargs.get('city')
        keyword_dict = kwargs.get('guessword_dict')
        result = dict()
        for old_borough_name, new_borough_name in keyword_dict.items():
            borough = self.onlinedao.find_one(city=city,filter={'borough_name': old_borough_name})
            guessword_status = self.get_guessword(city=city, guessword=old_borough_name)
            if borough and guessword_status:
                borough_id = borough[0]['_id']
                data = self.guessword_list(city=city,g_type=3, keyword=new_borough_name, other_id=borough_id)
                data.pop('created')
                self.onlinedao.update(city = city,filter={'_id': borough_id},
                                      datas={'$set': {'borough_name': new_borough_name}})
                if not self.get_guessword(city=city,guessword=new_borough_name):
                    self.guessdao.update(city=city,
                        filter={'other_id': borough_id, 'keyword': old_borough_name, 'type_id': 3},
                        datas={'$set': data})
                    self.push_es(city=city , g_type=3 , keyword=new_borough_name)
                    result.setdefault("%s->%s" % (old_borough_name, new_borough_name), '修改成功')
                else:
                    result.setdefault(new_borough_name, '联想词已存在')
            else:
                result.setdefault(old_borough_name, '联想词或者小区不存在')
        return result

    def get_borough_id(self, city, guessword, type_id):
        """
        查询联想词对应的小区id (borough_byname or borough_hide)
        :param keyword:联想词
        :return:小区id
        """
        if type_id == 3:
            where = {
                '$or': [
                    {
                        'borough_byname': guessword
                    },
                    {
                        'borough_hide': guessword
                    },
                ]
            }
        else:
            where = {
                "name": guessword
            }
        result = self.area_list[type_id].get_one(city=city,filter=where)
        if result:
            return result[0]['_id']
        else:
            return ''

    def get_guessword(self, city, guessword, type_id=3):
        """
        查询联想词是否存在
        :param keyword:联想词
        :return:true or false
        """
        where = dict()
        where.setdefault('keyword', guessword)
        where.setdefault('type_id', type_id)
        result = self.guessdao.find_one(city=city,filter=where)
        if result:
            return True
        else:
            return False

    def save_mongo(self, city,g_type, keyword, borough_id):
        try:
            result = self.guessword_list(city,g_type, keyword, borough_id)
            self.guessdao.update(city=city,datas=result)
            return '成功存入mongo'
        except:
            return '存入mongo失败'

    def guessword_list(self,city, g_type, keyword, other_id, addr=''):
        """
        生成插入mongo.guessword的数据
        :param g_type: 联想词类型 3:小区
        :param keyword: 联想词
        :param other_id: 小区id
        :param addr: 地址
        :return: 拼装好的数据
        """
        if city == "bj":
            mongo_db = "spider"
        else:
            mongo_db = "spider_%s"%(city)
        index_name = 'online_' + city + '_house_sell'
        guessword_dict = {}
        guessword_dict['type_id'] = g_type
        guessword_dict['type_name'] = self.guessdao.guessword_type[g_type]
        guessword_dict['other_id'] = other_id
        guessword_dict['keyword'] = keyword
        guessword_dict['addr'] = addr
        guessword_dict['pinyin'] = BaseUtils.getFpy(keyword, delimiter='')
        guessword_dict['logogram'] = BaseUtils.getSpy(keyword, delimiter='').lower()
        guessword_dict['count_broker'] = self.es_search(city=city,index_name=index_name, table=mongo_db,key_v=self.guessdao.guessword_key[g_type], value_search=other_id, type_id=g_type)
        guessword_dict['created'] = int(time.time())
        guessword_dict['updated'] = int(time.time())
        return guessword_dict

    def es_search(self, city, index_name, table, key_v, value_search, type_id):
        # 商圈数据结构和 小区城区不同 需要嵌套查询
        if type_id == 2:
            query_json = {
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
                                                    "term": {"cityarea2.cityarea2_id": value_search}
                                                }
                                            ]
                                        }
                                    }
                                }
                            },
                            {
                                "nested": {
                                    "path": "house_info",
                                    "query": {
                                        "bool": {
                                            "must_not": [
                                                {
                                                    "term": {"house_info.source": 10}
                                                }
                                            ]
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        else:
            query_json = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {key_v: value_search}
                            },
                            {
                                "nested": {
                                    "path": "house_info",
                                    "query": {
                                        "bool": {
                                            "must_not": [
                                                {
                                                    "term": {"house_info.source": 10}
                                                }
                                            ]
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        # import json
        # print(json.dumps(query_json))
        res = self.complexsearchservice.search_filter(city=city, dsl=query_json, index_name=index_name, doc_name=table)['total'] # 获取所有数据
        # 获取第一条数据，得分最高。
        top_10_recodes = res
        return top_10_recodes

    def push_es(self, city, g_type, keyword):
        try:
            actions = self.generate_es_data(city, g_type, self.complexsearchservice.online_keyword_alias, keyword)
            self.complexsearchservice.search_bulk(actions=actions)
            return '成功推送es'
        except Exception as e:
            return '推送es失败'

    def delete_es(self, city,alias, id):
        try:
            if city == "bj":
                mongo_db = "spider"
            else:
                mongo_db = "spider_%s" % (city)
            index_name = str(self.complexsearchservice.get_alias_es(alias=alias))
            self.complexsearchservice.del_es(index_name=index_name, doc_name=mongo_db, id=id)
            return '删除es成功'
        except:
            return '删除es失败'

    def generate_es_data(self, city, g_type, alias, keyword):
        """
        拼装插入es的数据
        :param alias:线上联想词es别名
        :param type:es type名称(相当于mysql的表)
        :return: 返回拼装好的es数据
        """
        where = dict()
        where.setdefault('keyword', keyword)
        where.setdefault('type_id', g_type)
        field = ['addr','count_broker','type_id','created','updated','keyword','logogram','other_id','pinyin','type_name']
        result = self.guessdao.find_one(city=city, filter=where,field=field)
        result = self.format_data(result)
        index = str(self.complexsearchservice.get_alias_es(alias=alias))
        actions = []
        id = result.pop("_id")
        if city == "bj":
            table = "spider"
        else:
            table = "spider_%s"%(city)
        value = {"_index": index, "_type": table, "_id": id, "_source": result}
        actions.append(value)
        return actions

    def format_data(self, data):
        format_houses = dict()
        format_houses['_id'] = str(data[0]['_id'])
        if 'addr' in data[0]:
            format_houses['addr'] = str(data[0]['addr'])
        if 'count_broker' in data[0]:
            format_houses['count_broker'] = data[0]['count_broker']
        if data[0].get('type_id'):
            format_houses['type_id'] = data[0]['type_id']
        if data[0].get('created'):
            format_houses['created'] = int(data[0]['created'])
        if data[0].get('updated'):
            format_houses['updated'] = int(data[0]['updated'])
        if data[0].get('keyword'):
            format_houses['keyword'] = str(data[0]['keyword'])
        if data[0].get('logogram'):
            format_houses['logogram'] = str(data[0]['logogram'])
        if data[0].get('other_id'):
            format_houses['other_id'] = int(data[0]['other_id'])
        if data[0].get('pinyin'):
            format_houses['pinyin'] = str(data[0]['pinyin'])
        if data[0].get('type_name'):
            format_houses['type_name'] = str(data[0]['type_name'])
        return format_houses
if __name__ == '__main__':
    print(BaseUtils.getFpy("数据",delimiter=' '))
