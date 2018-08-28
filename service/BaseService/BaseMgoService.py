# -*- coding: utf-8 -*-

class BaseMgoService(object):
    def __init__(self, dao):
        self.dao = dao

    # 查询所有
    def get_all(self, *args, **kwargs):
        return self.dao.find_all(*args, **kwargs)

    # 根据条件获取数据
    def get_one(self, *args, **kwargs):
        return self.dao.find_one(*args, **kwargs)

    # 获取分页
    def get_page(self, *args, **kwargs):
        return self.dao.find_page(*args, **kwargs)

    # 获取分页
    def get_count(self, *args, **kwargs):
        return self.dao.find_count(*args, **kwargs)

    # 获取分页
    def aggregate(self, *args, **kwargs):
        return self.dao.aggregate(*args, **kwargs)

    # 根据id查询
    def get_by_id(self, *args, **kwarg):
        try:
            return self.dao.select_by_id(*args, **kwarg)
        except Exception as e:
            print(e)

    # 根据url查询
    def select_by_url(self, urls):
        try:
            return self.dao.select_by_url(urls=urls)
        except Exception as e:
            print(e)


    def add_index(self, index, type):
        try:
            return self.dao.create_index(index=index, type=type)
        except Exception as e:
            print(e)

    # 单条插入
    def add_one(self, city, datas):
        try:
            if datas:
                return self.dao.insert_one(datas=datas,city=city)
        except Exception as e:
            print(e)

    def add_batch(self, datas):
        try:
            if datas:
                return self.dao.insert_batch(datas=datas)
        except Exception as e:
            print(e)

    def get_filter(self, **kwargs):
        try:
            mgofilter = kwargs.get("mgofilter")
            field = kwargs.get("field", [])
            index = kwargs.get("index", 1)
            size = kwargs.get("size", None)
            return self.dao.select_mgofilter(mgofilter=mgofilter, field=field, index=index, size=size)
        except Exception as e:
            print(e)

    # 根据指定字段删除
    def remove_by_id(self, city, filter):
        try:
            return self.dao.delete_by_id(city=city, filter=filter)
        except Exception as e:
            print(e)

    # 批量删除
    def delete_by_ids(self, id):
        try:
            return self.dao.delete_by_ids(id=id)
        except Exception as e:
            print(e)

    def update_by_filter(self, filter, datas, city):
        try:
            return self.dao.update(filter=filter, datas=datas, city=city)
        except Exception as e:
            print(e)

    def update_by_filterall(self, filter, datas, city):
        try:
            return self.dao.updateAll(filter, datas=datas, city=city)
        except Exception as e:
            print(e)

    def find_aggregate(self,sql):
        try:
            return self.dao.aggregate(sql=sql)
        except Exception as e:
            print(e)

    def get_mongo_id(self):
        try:
            return self.dao.getMongoNextId()
        except Exception as e:
            print(e)

    def drop(self, city):
        try:
            return self.dao.drop(city=city)
        except Exception as e:
            print(e)

    def remove(self, city):
        try:
            return self.dao.remove(city=city)
        except Exception as e:
            print(e)

    def rename(self, city, new_name):
        try:
            return self.dao.rename(city=city, new_name=new_name)
        except Exception as e:
            print(e)

