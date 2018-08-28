#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 上午11:28
# @Author  : jianguo@zhugefang.com
# @Desc    : dao 缓存装饰器

from databases.dbfactory.dbfactory import dbfactory
import json

class LocalCache(object):
    def __init__(self, *args, **kwargs):
        self.conf_name = kwargs.get("conf_name")  # 缓存地址
        self.key = kwargs.get("key")              # 缓存key
        self.update = kwargs.get("update", [])    # 更新数据清空缓存
        self.time = kwargs.get("time")# 缓存失效时间

    def __call__(self, func):
        """
        :param func:
        :return:
        """
        def wrapper(*args, **kwargs):
            self.city=kwargs.get("city")
            self.class_name = list(args).pop().__class__.__name__
            self.func_name = func.__name__
            # hashkey：【城市】【类名【函数名称】
            self.name = self.city+"."+self.class_name+"."+self.func_name
            cache_key = kwargs.get(self.key)
            if self.update:  # 更新清理缓存
                for key in self.update:
                    self.del_cache(name=self.city+"."+self.class_name+"."+key, keys=cache_key)
                return func(*args, **kwargs)
            cache_result = self.pull_cache(name=self.name, key=cache_key)
            if cache_result:  # 查询缓存
                return json.loads(cache_result)
            else:
                result = func(*args, **kwargs)  #查询数据
                if result:
                    #添加缓存
                    self.push_cache(name=self.name, key=cache_key, value=json.dumps(result))
                return result
        return wrapper


    def push_cache(self, name, key, value):
        """
        :param name:
        :param key:
        :param value:
        :desc 添加缓存
        :return:
        """
        try:
            redis_search = dbfactory.create_db(conf_name=self.conf_name, db_type="db_redis")
            result = redis_search.hset(name=name, key=key, value=value)
            if redis_search.ttl(name=name) < 0 and self.time:
                redis_search.expire(name=name, time=self.time)
            return result

        except Exception as e:
            return None

    def pull_cache(self, name, key):
        """
        :param name:
        :param key:
        :desc #获取缓存
        :return:
        """
        try:
            redis_search = dbfactory.create_db(conf_name=self.conf_name, db_type="db_redis")
            return redis_search.hget(name=name, key=key)
        except Exception as e:
            return None


    def del_cache(self, name, keys):
        """
        :param name:
        :param keys:
        :param 删除缓存
        :return:
        """
        try:
            redis_search = dbfactory.create_db(conf_name=self.conf_name, db_type="db_redis")
            return redis_search.hdel(name, keys)
        except Exception as e:
            return None


#测试
class bb(object):

    @LocalCache()
    def get_keyword_id(self, *args, **kwargs):
        house_name = kwargs.get("id")
        cache_key = "id"
        return {"id": "1"}

    @LocalCache(key="id", update=["bb.get_keyword_id"])
    def up_keyword_id(self, *args, **kwargs):
        house_name = kwargs.get("id")
        return True

    @LocalCache(update=["bb.get_keyword_id"])
    def del_keyword_id(self, *args, **kwargs):
        id = kwargs.get("id")
        return True


if __name__ == '__main__':
    pass
    #bb = bb()
    #bb.up_keyword_id(city="bj", id="1")
    #print(bb.get_keyword_id(city="bj", id ="1"))
