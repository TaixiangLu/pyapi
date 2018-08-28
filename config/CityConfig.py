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