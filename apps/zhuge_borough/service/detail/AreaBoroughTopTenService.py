#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
import sys
sys.path.append("../../../../")
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_sell.dao.detail.HouseSellDao import HouseSellDao
from apps.config.CityConfig import CityConfig
import datetime
import time


class SortBoroughSum(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.house_sell = HouseSellDao()
        self.cities = CityConfig.get_cityinfo(where={"is_sell": 1})
        self.day1 = int(datetime.datetime.strftime(datetime.date.today(), '%Y%m%d'))
        self.today_zero_time = int(time.time() - time.time() % 86400)


    def city_borough_top(self, *args, **kwargs):
        try:
            result_list = []
            city = kwargs.get("city")
            seeds = kwargs.get("pms").get("seeds", 0)
            city_consult_sql = f"select count(*), borough_id from house_sell " \
                               f"group by borough_id order by count(*) desc limit {seeds};"
            result = self.house_sell.exe_s_sqls(city=city, sql={"sql": city_consult_sql})
            sorted(result, key=lambda result_items: result_items["count(*)"], reverse=True)
            for result_items in result:
                result_dict = {'borough_id': result_items['borough_id'], 'sum': result_items['count(*)']}
                result_list.append(result_dict)
            return {"data": result_list}
        except Exception as e:
            print(e)



    # 城区范围内获取热门小区top
    def cityarea_borough_top(self, *args, **kwargs):
        try:
            # 定义结果列表
            result_list = []
            # 获取城市简写,城区id
            city = kwargs.get('city')
            cityarea_id = kwargs.get('pms').get('cityarea_id', 0)
            seeds = kwargs.get("pms").get("seeds", 0)
            #print(city)
            # 编写sql语句并发送&接收数据
            cityarea_question_sql = f"SELECT count(*), borough_id from house_sell " \
                                    f"where cityarea_id = {cityarea_id} " \
                                    f"GROUP BY borough_id ORDER BY count(*) desc LIMIT {seeds};"
            result = self.house_sell.exe_s_sqls(city=city, sql={'sql': cityarea_question_sql})
            # 排序
            sorted(result, key=lambda result_items: result_items['count(*)'], reverse=True)
            # 加入字典
            for result_items in result:
                result_dict = {'borough_id': result_items['borough_id'], 'sum': result_items['count(*)']}
                result_list.append(result_dict)
            #print(result_list)
            # 返回数据
            return {"data": result_list}

        except Exception as e:
            print(e)

    # 城区范围内获取热门小区top10
    def cityarea2_borough_top(self, *args, **kwargs):
        try:
            # 定义结果字典
            result_list = []
            # 获取城市简写,城区id,商圈id
            city = kwargs.get('city')
            cityarea2_id = kwargs.get('pms').get('cityarea2_id', 0)
            seeds = kwargs.get("pms").get("seeds", 0)
            #print(city)
            # 编写sql语句并发送&接收所有商圈信息
            cityarea2_question_sql = f"SELECT count(*), borough_id from house_sell " \
                                     f"where " \
                                     f" cityarea2_id = {cityarea2_id} " \
                                     f"GROUP BY borough_id ORDER BY count(*) desc LIMIT {seeds};"
            result = self.house_sell.exe_s_sqls(city=city, sql={'sql': cityarea2_question_sql})
            # 排序
            sorted(result, key=lambda result_items: result_items['count(*)'], reverse=True)
            # 加入字典
            for result_items in result:
                result_dict = {'borough_id': result_items.get('borough_id'), 'sum': result_items.get('count(*)')}
                result_list.append(result_dict)
            # 返回数据
            return {"data": result_list}

        except Exception as e:
            print(e)


if __name__ == '__main__':
    print(SortBoroughSum().city_borough_top(pms={"city": "bj", "seeds": 20}))
    print(SortBoroughSum().cityarea_borough_top(pms={"city": 'bj', 'cityarea_id': 2, "seeds": 10}))
    print(SortBoroughSum().cityarea2_borough_top(pms={"city": 'sz', 'cityarea2_id': 2, "seeds": 10}))
