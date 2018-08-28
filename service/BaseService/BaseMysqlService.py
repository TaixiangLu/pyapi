# -*- coding: utf-8 -*-
class BaseMysqlService(object):
    def __init__(self, dao):
        self.dao = dao


    def get(self, *args, **kwargs):
        return self.dao.select_model(*args, **kwargs)

    def add(self, *args, **kwargs):
        return self.dao.insert_model(*args, **kwargs)

    def adds(self, *args, **kwargs):
        return self.dao.inserts_model(*args, **kwargs)

    def up(self, *args, **kwargs):
        return self.dao.update_model(*args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.dao.delete_model(*args, **kwargs)



    def get_by_id(self, *args, **kwarg):
        return self.dao.select_by_id(*args, **kwarg)

    def get_by_filter(self, *args, **kwarg):
        return

    def select_by_filter(self, *args, **kwargs):
        return self.dao.select_by_filter(*args, **kwargs)

    def select_by_page(self, *args, **kwargs):
        return self.dao.select_by_page(*args, **kwargs)

    def get_count(self, *args, **kwarg):
        return self.dao.select_count(*args, **kwarg)

    def insert_one(self, *args, **kwargs):
        return self.dao.insert_one(*args, **kwargs)

    def delete_by_filter(self, *args, **kwargs):
        return self.dao.delete_by_filter(*args, **kwargs)

    def get_ids(self, *args, **kwargs):
        return self.dao.get_ids(*args, **kwargs)
