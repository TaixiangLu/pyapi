#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 上午10:50
# @Author  : jianguo@zhugefang.com
# @Desc    :
from dao.Base import Base
from databases.dbfactory.dbfactory import dbfactory


class BaseMongo(Base):
    """
    规范:

    方法名称：以find_为前缀  find_all
    参数统一使用动态参数接收（self, *args, **kwargs）
    按条件查询 添加 by  eg: find_by_id
    参数注释：@:params
    """

    def __init__(self, *args, **kwargs):
        """
        调用父类初始化数据库连接
        :param conf_name: 配置名称
        :return
        """
        self.conn = dbfactory.create_db(conf_name=kwargs.get("conf_name"),
                                        db_type="db_mongo")
        super().__init__(model=kwargs.get("model"))

    def find_all(self, *args, **kwargs):
        """
        根据条件查询所有数据
        :param filter: 过滤条件
        :param field:  返回显示字段
        :param city:   城市缩写
        :return: []
        """
        city = kwargs.get("city")
        filter = kwargs.get("filter") or {}
        field = kwargs.get("field") or {}
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            data = table.find(filter, field)
            return list(data)

    def count(self, *args, **kwargs):
        """
        根据条件查询数量
        :param filter: 过滤条件
        :param city:   城市缩写
        :return: []
        """
        city = kwargs.get("city")
        filter = kwargs.get("filter") or {}
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            data = table.count(filter)
            return data

    def find_all_sort(self, *args, **kwargs):
        """
        根据条件查询所有数据
        :param filter: 过滤条件
        :param field:  返回显示字段
        :param city:   城市缩写
        :return: []
        """
        city = kwargs.get("city")
        filter = kwargs.get("filter") or {}
        field = kwargs.get("field") or {}
        sort = kwargs.get("sort") or []
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            data = table.find(filter, field).sort(sort)
            return list(data)

    def find_one(self, *args, **kwargs):
        """
        根据条件查询所有数据
        :param filter: 过滤条件
        :param city:   城市缩写
        :return: []
        """
        city = kwargs.get("city")
        filter = kwargs.get("filter") or {}
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            data = table.find(filter)
            return list(data)

    def find_one_dict(self, *args, **kwargs):
        """
        查询一条数据 返回字典格式
        """
        city = kwargs.get("city")
        filter = kwargs.get("filter") or {}
        field = kwargs.get("field") or {}
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            data = table.find_one(filter, field)
            return data

    def find_page(self, *args, **kwargs):
        """
        分页查询
        :param page: 分页对象
        :param filter: 过滤条件
        :param sort: 过滤条件  eg: [("_id", 1)] 按_id 升序排序
        :param field:  返回显示字段
        :param city:   城市缩写
        :param size 显示条数
        :return: []
        """
        city = kwargs.get("city")
        page = kwargs.get("page") or {"index": 1, "size": 30}
        filter = kwargs.get("filter") or {}
        sort = kwargs.get("sort") or [("_id", 1)]
        field = kwargs.get("field") or {}

        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            if field:
                data = table.find(
                    filter,
                    field).skip(
                    (page.get("index") -
                     1) *
                    page.get("size")).sort(sort).limit(
                    page.get("size"))
            else:
                data = table.find(filter).skip(
                    (page.get("index") -
                     1) *
                    page.get("size")).sort(sort).limit(
                    page.get("size"))
            return list(data)

    def find_count(self, *args, **kwargs):
        """
        根据条件统计总条数
        :param filter: 过滤条件
        :param city:   城市缩写
        :return:
        """
        city = kwargs.get("city")
        filter = kwargs.get("filter") or {}
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            total = table.find(filter).count()
            return total

    def find_by_id(self, *args, **kwargs):
        pass

    def find_by_ids(self, *args, **kwargs):
        pass

    def find_by_filter(self, *args, **kwargs):

        pass

    def insert_one(self, *args, **kwargs):
        """
        根据条件更新数据
        :param data: 插入数据
        :param city:   城市缩写
        :return:
        """
        city = kwargs.get("city")
        datas = kwargs.get('datas')
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            result = table.insert(datas)
            return result

    def insert_batch(self, *args, **kwargs):
        pass

    def update_by_filter(self, *args, **kwargs):
        pass

    def updata_by_id(self, *args, **kwargs):
        pass

    def delete_by_id(self, *args, **kwargs):
        """
        根据条件更新数据
        :param filter: 过滤条件
        :param city:   城市缩写
        :return:
        """
        city = kwargs.get("city")
        filter = kwargs.get("filter") or {}
        if not filter:
            return False
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            data = table.remove(filter)
            return data

    def delete_by_filter(self, *args, **kwargs):
        pass

    def page(self, **kwargs):
        pass

    def update(self, **kwargs):
        """
        根据条件更新数据
        :param filter: 过滤条件
        :param city:   城市缩写
        :param datas   更新的数据
        :return:
        """
        city = kwargs.get("city")
        filter = kwargs.get("filter") or {}
        datas = kwargs.get('datas') or {}
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            data = table.update(filter, datas)
            return data

    def updateAll(self, **kwargs):
        """
        :desc 更新数据
        :param filter: 表名
        :param datas: 数据集合 {}
        :return: _id
        """
        city = kwargs.get("city")
        filter = kwargs.get("filter") or {}
        datas = kwargs.get('datas') or {}
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            data = table.update(filter, datas, upsert=True, multi=True)
            return data

    def getMongoNextId(self, key):
        """
        :desc 更新数据
        :param filter: 表名
        :param datas: 数据集合 {}
        :return: _id
        """
        with self.conn.getMongoConn(city='') as client:
            table = client.get_collection(self.table)
            count = table.count()
            if count == 0:
                mongo_id = 1
            else:
                mongo_id = table.find({"%s_id" % key: {"$ne": 9999999}}).limit(1).sort('%s_id' % key, -1)[0]['%s_id' % key] + 1
            return mongo_id

    def drop(self, **kwargs):
        """
        :desc 删除集合
        """
        city = kwargs.get("city")
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            data = table.drop()
            return data

    def remove(self, **kwargs):
        """
        :desc 删除集合文档
        """
        city = kwargs.get("city")
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            data = table.remove()
            return data

    def rename(self, **kwargs):
        """
        :desc 集合重命名
        """
        city = kwargs.get("city")
        new_name = kwargs.get("new_name")
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            data = table.rename(new_name)
            return data

    def aggregate(self, **kwargs):
        """
        """
        city = kwargs.get("city")
        pipeline = kwargs.get("pipeline")
        with self.conn.getMongoConn(city=city) as client:
            table = client.get_collection(self.table)
            data = table.aggregate(pipeline)
            return data
