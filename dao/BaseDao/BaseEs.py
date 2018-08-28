#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 上午10:53
# @Author  : jianguo@zhugefang.com
# @Desc    :
from dao.Base import Base
from databases.dbfactory.dbfactory import dbfactory
from elasticsearch import helpers
# from elasticsearch_dsl import Search
import logging
class BaseES(Base):
    def __init__(self, *args, **kwargs):
        self.conf_name = kwargs.get("conf_name")
        self.conn = dbfactory.create_db(conf_name=self.conf_name, db_type="db_es")
        super().__init__(model=kwargs.get("model"))

# ======================================================================================================================
# -----------DDL语句-----------------------------------------------------------------------------------------------------
# ======================================================================================================================

    def create_index(self, *args, **kwargs):
        index_name = kwargs.get('index_name')
        alies = kwargs.get('alies')
        with self.conn.get_conn() as es:
            if not es.indices.exists(index_name):
                es.indices.create(index=index_name, ignore=400)
                return True
            else:
                logging.error(f"{index_name}: has already existed")
                return False

    def delete_index(self, *args, **kwargs):
        index_name = kwargs.get('index_name')
        with self.conn.get_conn() as es:
            if es.indices.exists(index_name):
                es.indices.delete(index=index_name, ignore=400)
                return True
            else:
                logging.error(f"{index_name}: does not exist")
                return False

    def add_alies(self, *args, **kwargs):
        index_name = kwargs.get('index_name')
        with self.conn.get_conn() as es:
            if es.indices.exists(index_name):
                es.indices.delete(index=index_name, ignore=400)
                return True
            else:
                logging.error(f"{index_name}: does not exist")
                return False


    def create_document(self, *args, **kwargs):
        with self.conn.get_conn() as es:
            pass

    def push_mapping(self, *args, **kwargs):
        pass

    def remove_mapping(self, *args, **kwargs):
        pass


    def del_alies(self, *args, **kwargs):
        pass

    def switch_alies(self, *args, **kwargs):
        pass


# ======================================================================================================================
# -----------DDL语句-----------------------------------------------------------------------------------------------------
# ====================a==================================================================================================


# ======================================================================================================================
# -----------DQL语句-----------------------------------------------------------------------------------------------------
# ======================================================================================================================
    def select_where(self, index_name, doc_name, dsl):
        """
        :param index_name:
        :param doc_name:
        :param dsl:
        :return:
        """
        search_data = {"code": "200", "message": "success", "datas": [], "total": 0}
        with self.conn.get_conn() as es:
            try:
                if es.indices.exists(index=index_name):
                    result = es.search(index=index_name, doc_type=doc_name, body=dsl)
                    total = result.get("hits", {}).get("total", 0)
                    content = [item['_source'] for item in result.get("hits", {}).get('hits')]
                    search_data["total"] = total
                    search_data["datas"] = content
            except Exception as e:
                search_data["message"] = e
                search_data["code"] = "500"
            return search_data
    
    def select_agg(self, index_name, doc_name, dsl):
        """
        :param index_name:
        :param doc_name:
        :param dsl:
        :return:
        """
        search_data = {"code": "200", "message": "success", "datas": [], "total": 0}
        with self.conn.get_conn() as es:
            try:
                if es.indices.exists(index=index_name):
                    result = es.search(index=index_name, doc_type=doc_name, body=dsl)
                    tmp = result.get("aggregations", {})
                    search_data["datas"] = tmp
            except Exception as e:
                search_data["message"] = e
                search_data["code"] = "500"
            return search_data

    def select_agg_one(self, index_name, doc_name, dsl, agg_field):
        """
        :param index_name:
        :param doc_name:
        :param dsl:
        :return:
        """
        search_data = {"code": "200", "message": "success", "datas": [], "total": 0}
        with self.conn.get_conn() as es:
            try:
                if es.indices.exists(index=index_name):
                    result = es.search(index=index_name, doc_type=doc_name, body=dsl)
                    content = [item for item in result.get("aggregations", {}).get(agg_field).get('buckets')]
                    search_data["datas"] = content
                    search_data['total'] = len(search_data["datas"])
            except Exception as e:
                search_data["message"] = e
                search_data["code"] = "500"
            return search_data


    def search_filter(self, *args, **kwargs):
        pass





    # def delete_by_filter(self, *args, **kwargs):
    #     pass
# ======================================================================================================================
# -----------DML语句-----------------------------------------------------------------------------------------------------
# ======================================================================================================================

# ======================================================================================================================
# -----------DML语句-----------------------------------------------------------------------------------------------------
# ======================================================================================================================

    def insert_one(self, index_name, doc_name, id, data):
        """
        ：Time    : 17-10-10 下午2:36
        :Author  : jianguo@zhugefang.com
        :desc   :  添加文档

        :arg index_name:  索引名称
        :arg doc_name:    文档名称
        :arg id: 文档唯一索引
        :arg data: 数据
        """


        status = False
        try:
            with self.conn.get_conn() as es:
                es.create(index=index_name, doc_type=doc_name, id=id, body=data)
                return True
        except Exception as e:
            print("insert_bulk", e)
        return status

    # def insert_batch(self, index_name, doc_name, datas):
    #     """
    #     ：Time    : 17-10-10 下午2:36
    #     :Author  : jianguo@zhugefang.com
    #     :desc   :  批量添加文档
    #
    #     :arg index_name:  索引名称
    #     :arg doc_name:    文档名称
    #     :arg datas: 数据
    #     """
    #
    #     status = False
    #     try:
    #         actions = [
    #                 {
    #                      '_index':index_name,
    #                      '_type': doc_name,
    #                      "_op_type": "create",
    #                      "_id": item.get("id"),
    #                      "_source": item
    #                 }
    #                     for item in datas
    #             ]
    #         print helpers.bulk(self.es,actions=actions)
    #         return True
    #     except Exception as e:
    #         print "search_all", e
    #     return status

    def bulks(self, actions):
        status = False
        try:
            with self.conn.get_conn() as es:
                helpers.bulk(es, actions=actions)
                return True
        except Exception as e:
            print("search_all", e)
        return status

    # 单条更新es
    def update_index(self, index, doc_type, id, body):
        status = False
        try:
            with self.conn.get_conn() as es:
                result = es.update(index=index, doc_type=doc_type, id=id, body={"doc":body})
                return result
        except Exception as e:
            print("update_index", e)
        return status

    # 按条件进行更新es
    def update_by_query(self, index, doc_type,body):
        try:
            with self.conn.get_conn() as es:
                result = es.update_by_query(index=index, doc_type=doc_type, body=body)
            if "updated" not in result:
                return False
            if result['updated'] > 0:
                return True
            else:
                return False
        except Exception as e:
            print("update_house", e)
            return False

    # 按条件进行删除
    def delete_by_query(self,index, doc_type,body):
        try:
            with self.conn.get_conn() as es:
                result = es.delete_by_query(index=index, doc_type=doc_type, body=body)
                if result['deleted'] > 0:
                    return True
                else:
                    return False
        except Exception as e:
            print("update_house", e)
            return False

    # 更新es字段加一
    def update_add_one(self, index, doc_type, id, body):
        try:
            with self.conn.get_conn() as es:
                result = es.update(index=index, doc_type=doc_type, id=id, body=body)
                return result
        except Exception as e:
            print("update_add_one", e)

    # 单条删除
    def delete_type(self, index, doc_type, id):
        try:
            with self.conn.get_conn() as es:
                result = es.delete(index=index, doc_type=doc_type, id=id)
                return result
        except Exception as e:
            print("delete_type", e)
            return False

    # 获取index
    def get_index_name(self, alias_name):
        try:
            with self.conn.get_conn() as es:
                result = list(es.indices.get_alias(alias_name).keys())[0]
                return result
        except Exception as e:
            print("get_index_name", e)
            return False


# ======================================================================================================================
# -----------DML语句-----------------------------------------------------------------------------------------------------
# ======================================================================================================================


