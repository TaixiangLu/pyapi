# -*- coding: utf-8 -*-
import requests
import json


def CityApi(city="", type=""):
    jsons = dict()
    if city:
        jsons.setdefault("city", city)
    if type:
        jsons.setdefault("type", type)
    data = requests.post(url="http://config.dapi.zhugefang.com/config/getCity", json=jsons)
    result = json.loads(data.content)["data"]
    return result


def BrokerCityApi():
    jsons = {
        "page": {
            "index": 1,
            "size": 100
        },
        "filter": {
            "is_b": 1
        }
    }
    b_list = dict()
    data = requests.post(url="http://config.dapi.zhugefang.com/config/getcityinfo", json=jsons)
    result = json.loads(data.content)["data"]
    for i in result:
        b_list.setdefault(i["logogram"], i["name"])
    return b_list


def CityInfo(where=""):
    jsons = dict()
    page = {
        "index": 1,
        "size": 500
    }
    jsons.setdefault("page", page)
    filter = {}
    if where:
        for i, k in where.items():
            filter.setdefault(i, k)
    jsons.setdefault("filter", filter)
    data = requests.post(url="http://config.dapi.zhugefang.com/config/getcityinfo", json=jsons)
    result = json.loads(data.content)["data"]
    return result


def CityInterval(city="bj", city_last="sansha"):
    jsons = dict()
    jsons.setdefault("city", city)
    jsons.setdefault("city_last", city_last)
    data = requests.post(url="http://config.dapi.zhugefang.com/config/getCityConfig", json=jsons)
    result = json.loads(data.content)["data"]
    return result


def all_city_ab():
    data = requests.get("http://api.zhugefang.com/API/City/getCity")
    result = json.loads(data.content)
    city_ab_list = []
    for i in result.get("data"):
        city_ab_list.append(str(i.get("city")))
    return city_ab_list


def all_city(open=True):
    def decorator(origin_func):
        def wrapper(*args, **kwargs):
            try:
                if open:
                    data = requests.get("http://api.zhugefang.com/API/City/getCity")
                    result = json.loads(data.content)
                    for i in result.get("data"):
                        kwargs["city"] = i.get("city")
                        origin_func(*args, **kwargs)
                else:
                    origin_func(*args, **kwargs)
            except Exception as e:
                print(e)
        return wrapper
    return decorator


def city_list(city_lists=[]):
    def decorator(origin_func):
        def wrapper(*args, **kwargs):
            try:
                for i in city_lists:
                    kwargs["city"] = i
                    origin_func(*args, **kwargs)
            except Exception as e:
                print(e)
        return wrapper
    return decorator


def generate_city_list(city_lists=[]):
    def decorator(origin_func):
        def wrapper(*args, **kwargs):
            try:
                for i in city_lists:
                    kwargs["city"] = i
                    yield origin_func(*args, **kwargs)
            except Exception as e:
                print(e)
        return wrapper
    return decorator


class CityConfig(object):
    @staticmethod
    def get_city(**kwargs):
        return CityApi(**kwargs)

    @staticmethod
    def get_city_interval(**kwargs):
        return CityInterval(**kwargs)

    @staticmethod
    def get_broker_city():
        return BrokerCityApi()

    @staticmethod
    def get_cityinfo(**kwargs):
        return CityInfo(**kwargs)
