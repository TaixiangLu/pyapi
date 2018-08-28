#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.CityCountDao import CityCountDao
from apps.zhuge_price.dao.detail.MonthlyHouseSellPriceDistributionDao import MonthlyHouseSellPriceDistributionDao
from apps.zhuge_price.dao.detail.MonthlyHouseSellAreaDistributionDao import MonthlyHouseSellAreaDistributionDao
from apps.zhuge_price.dao.detail.DailyHouseSellAvgpriceDao import DailyHouseSellAvgpriceDao
from apps.zhuge_price.dao.detail.AnalysisLastAreaDao import AnalysisLastAreaDao
from apps.zhuge_sell.dao.detail.HouseSellGovDao import HouseSellGovDao
from apps.zhuge_sell.dao.detail.HouseSellDao import HouseSellDao
import datetime
import time


class MiniProgramService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.monthly_house_sell_price_distribution = MonthlyHouseSellPriceDistributionDao(*args, **kwargs)
        self.monthly_house_sell_area_distribution = MonthlyHouseSellAreaDistributionDao(*args, **kwargs)
        self.daily_house_sell_avgprice = DailyHouseSellAvgpriceDao(*args, **kwargs)
        self.house_sell_gov = HouseSellGovDao()
        self.analysisLastAreaDao=AnalysisLastAreaDao(*args, **kwargs)
        self.house_sell = HouseSellDao()
        self.city_count = CityCountDao(*args, **kwargs)
        # TODO 如果当天的数据没有生成的话会返回空
        self.housetype_mapping = ['一居室', '二居室', '三居室', '四居室', '五居室', '五居室以上']

    # def getPriceWave(self, *args, **kwargs):
    #     pms = kwargs.get('pms')
    #     city = kwargs.get('city')
    #     area_type = pms.get('type', 1)
    #     today = datetime.date.today()
    #     day1 = int(datetime.datetime.strftime(today, '%Y%m%d'))
    #     # 获取前一天的数量
    #     day2 = datetime.datetime.strftime((today - datetime.timedelta(days=1)), '%Y%m%d')
    #
    #     result = self.daily_house_sell_avgprice.select_model(city=city, size=9999,
    #                                                          filter=f" type={area_type} and date={day2} ",
    #                                                          field=['type', 'date', 'cityarea_id', 'cityarea2_id',
    #                                                                 'count_add', 'count_rise', 'count_reduce'])
    #     if result:
    #         return {"data": result, "page": {"total": len(result)}}

    def city_avgprice(self, *args, **kwargs):
        city = kwargs.get('city')
        sql = "select AVG(house_price) as avg_price from house_sell_gov"
        result = self.house_sell_gov.city_avgprice(city=city, sql={'sql': sql})
        if result:
            return {"data": result}

    def sell_week_change_cnt(self, *args, **kwargs):
        city = kwargs.get('city')
        # 一周
        now = time.time()
        today = now-86400
        weeknow = today-6*86400
        weeknowstr = time.strftime('%Y%m%d', time.localtime(weeknow))
        nowstr = time.strftime('%Y%m%d', time.localtime(today))
        cityarea_id =  kwargs.get("cityarea_id",0)
        cityarea2_id =  kwargs.get("cityarea2_id",0)
        cache_key = f"{cityarea_id}_{cityarea2_id}"
        itemList = self.daily_house_sell_avgprice.sell_week_change_cnt(city=city, weeknowstr=weeknowstr, nowstr=nowstr,cache_key=cache_key,cityarea_id=cityarea_id,cityarea2_id=cityarea2_id)
        itemDic = {}

        result = {}
        for i in itemList:
            itemDic[str(i.get("date"))] = i
        for i in range(7):
            itime = weeknow+i*86400
            itimeKey = time.strftime('%Y%m%d', time.localtime(itime))
            item = {
                "date":str(itimeKey),
                "count_add":0,
                "count_rise":0,
                "count_reduce":0,
            }
            result.setdefault(itimeKey,itemDic.get(itimeKey,item))
        return result

    def getHousetypeDistribution(self, *args, **kwargs):
        pms = kwargs.get('pms')
        city = kwargs.get('city')
        cityarea_id = pms.get('cityarea_id', 0)
        cityarea2_id = pms.get('cityarea2_id', 0)

        if cityarea2_id:
            filter = f" cityarea2_id={cityarea2_id} group by house_room"
            field = 'cityarea2_id, house_room, count(1)'
            cache_key = f"cityarea2_{cityarea2_id}"
            result = self.house_sell.housetype_distribution(city=city, filter=filter, field=field, cache_key=cache_key)
            if result:
                data = dict()
                for i in result:
                    if i['house_room'] == 0:
                        continue
                    if i['house_room'] >= 6:
                        data.setdefault('五居室以上', 0)
                        data['五居室以上'] += i['total']
                        continue
                    data.setdefault(self.housetype_mapping[i['house_room'] - 1], i['total'])

                return {"data": data}

        if cityarea_id:
            filter = f" cityarea_id={cityarea_id} group by house_room"
            field = 'cityarea_id, house_room, count(1)'
            cache_key = f"cityarea_{cityarea_id}"
            result = self.house_sell.housetype_distribution(city=city, filter=filter, field=field, cache_key=cache_key)
            if result:
                data = dict()
                for i in result:
                    if i['house_room'] == 0:
                        continue
                    if i['house_room'] >= 6:
                        data.setdefault('五居室以上', 0)
                        data['五居室以上'] += i['total']
                        continue
                    data.setdefault(self.housetype_mapping[i['house_room'] - 1], i['total'])

                return {"data": data}

    # 查tidb数据库
    # def getHousePriceDistribution(self, *args, **kwargs):
    #     pms = kwargs.get('pms')
    #     city = kwargs.get('city')
    #     area_type = pms.get('type', 1)
    #     today = datetime.date.today()
    #     day1 = int(datetime.datetime.strftime(today, '%Y%m%d'))
    #     result = self.monthly_house_sell_price_distribution.select_model(city=city, size=9999,
    #                                                                      filter=f" type={area_type} and date={day1} ",
    #                                                                      field=['type', 'date', 'cityarea_id',
    #                                                                             'cityarea2_id', 'price_range',
    #                                                                             'count_sell'])
    #     if result:
    #         return {"data": result, "page": {"total": len(result)}}

    # def getHouseAreaDistribution(self, *args, **kwargs):
    #     pms = kwargs.get('pms')
    #     city = kwargs.get('city')
    #     area_type = pms.get('type', 1)
    #     today = datetime.date.today()
    #     day1 = int(datetime.datetime.strftime(today, '%Y%m%d'))
    #     result = self.monthly_house_sell_area_distribution.select_model(city=city, size=9999,
    #                                                                      filter=f" type={area_type} and date={day1} ",
    #                                                                      field=['type', 'date', 'cityarea_id',
    #                                                                             'cityarea2_id', 'area_range',
    #                                                                             'count_sell'])
    #     if result:
    #         return {"data": result, "page": {"total": len(result)}}


    def sellFinishAreaDistribution(self, *args, **kwargs):
        # 成交面积分布
        city = kwargs.get('city')
        result = self.analysisLastAreaDao.sellFinishAreaDistribution(city=city)
        return result

    def sellFinishPriceDistribution(self, *args, **kwargs):
        # 成交价格分布
        city = kwargs.get('city')
        result = self.analysisLastAreaDao.sellFinishPriceDistribution(city=city)
        return result

    def sellFinishCompanyDistribution(self, *args, **kwargs):
        # 成交价格分布
        city = kwargs.get('city')
        result = self.analysisLastAreaDao.sellFinishCompanyDistribution(city=city)
        return result

    def sellFinishCityareaDistribution(self, *args, **kwargs):
        # 成交价格分布
        city = kwargs.get('city')
        result = self.analysisLastAreaDao.sellFinishCityareaDistribution(city=city)
        return result