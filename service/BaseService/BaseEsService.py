# -*- coding: utf-8 -*-

class BaseEsService(object):
    def __init__(self, dao):
        self.dao = dao
    def search_agg(self, index_name, doc_name, *args, **kwargs):
        return self.dao.select_agg(index_name=index_name, doc_name=doc_name, dsl=kwargs.get("dsl"))
    def search_filter(self, index_name, doc_name, *args, **kwargs):
        # print ("==================dsl================================")
        # print (index_name + "." + doc_name + ":", kwargs.get("dsl"))
        # print ("==================dsl================================")
        return self.dao.select_where(index_name=index_name, doc_name=doc_name, dsl=kwargs.get("dsl"))

    def alias_es(self, alias_name):
        return self.dao.get_index_name(alias_name=alias_name)

    def add_es(self, index_name, doc_name, id, data):
        return self.dao.insert_one(index_name=index_name, doc_name=doc_name, id=id, data=data)

    def up_es(self, index_name, doc_name, id, body):
        return self.dao.update_index(index=index_name, doc_type=doc_name, id=id, body=body)

    def up_es_add_one(self, index_name, doc_name, id, body):
        return self.dao.update_add_one(index=index_name, doc_type=doc_name, id=id, body=body)

    def del_es(self, index_name, doc_name, id):
        return self.dao.delete_type(index=index_name, doc_type=doc_name, id=id)

    def up_by_query(self,index, doc_type, body):
        return self.dao.update_by_query(index=index, doc_type=doc_type,body=body)

    def search_bulk(self, actions):
        return self.dao.bulks(actions=actions)
