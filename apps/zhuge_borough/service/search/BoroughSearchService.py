#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 17-10-10 下午2:36
# @Author  : jianguo@zhugefang.com
# @desc   :  小区业务ES业务访问层
from service.BaseService.BaseEsService import BaseEsService
from apps.zhuge_borough.dao.search.BoroughSearchDao import BoroughSearchDao
class BoroughSearchService(BaseEsService):
    def __init__(self):
        super(BoroughSearchService, self).__init__(dao=BoroughSearchDao())
        pass

    def update_index(self,index, doc_name, borough_id, body):
        es_data = self.up_es(index_name=index,doc_name=doc_name,id=borough_id,body=body)
        return es_data

    def update_by_query(self,index, doc_type, body):
        update_query_data = self.up_by_query(index=index,doc_type=doc_type,body=body)
        return update_query_data

    def delete_type(self, index, doc_type, id):
        result = self.del_es(index_name=index, doc_name=doc_type, id=id)
        return result

    def insert_one(self,index_name, doc_name, id, data):
        result = self.add_es(index_name=index_name, doc_name=doc_name, id=id, data=data)
        return result

    # 检测房源es房源数量
    def checkHouseByBoroughId(self,city, borough_id):
        index_name = "active_" + city + "_house_sell"
        if city == 'bj':
            doc_name = "spider"
        else:
            doc_name = "spider_" + city
        data = self.search_filter(index_name=index_name, doc_name=doc_name, borough_id=borough_id)
        print(data)
        if data:
            total = data['total']
        else:
            total = 0
        return total