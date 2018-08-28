#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.AnalysisDayDao import AnalysisDayDao
from apps.zhuge_borough.dao.detail.AnalysisWeekDao import AnalysisWeekDao
from apps.zhuge_borough.dao.detail.AnalysisMonthDao import AnalysisMonthDao
from apps.zhuge_borough.dao.detail.ComplexInfoDao import ComplexInfoDao
from apps.zhuge_borough.dao.detail.ComplexDao import ComplexDao
from apps.zhuge_price.dao.detail.MonthlyNewHouseDealHousetypeDistributionDao import \
    MonthlyNewHouseDealHousetypeDistributionDao
from apps.zhuge_price.dao.detail.MonthlyNewHouseDealPriceDistributionDao import MonthlyNewHouseDealPriceDistributionDao
import datetime
import time


class NewHouseMPService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.complex = ComplexDao()
        # self.analysis_day = AnalysisDayDao()
        # self.analysis_week = AnalysisWeekDao()
        self.analysis_month = AnalysisMonthDao()
        self.monthly_newhouse_deal_housetype_distributionDao = MonthlyNewHouseDealHousetypeDistributionDao()
        self.monthly_newhouse_deal_price_distributionDao = MonthlyNewHouseDealPriceDistributionDao()
        self.complex_info = ComplexInfoDao()
        # 城市分级 默认为3
        self.city_level_mapping = {
            'bj': 1,
            'sh': 1,
            'gz': 1,
            'sz': 1,
            'hz': 1,
            'nj': 2,
            'tj': 2,
            'wh': 2,
            'su': 2,
            'dg': 2,
            'zh': 2,
            'qd': 2,
            'fz': 2,
            'cz': 2,
        }

        # 不同城市对应的价格范围
        self.price_range_mapping = {
            1: ['200万以下', '200~250万', '250~300万', '300~500万', '500~1000万', '1000万以上'],
            2: ['100万以下', '100~200万', '200~300万', '300~500万', '500~1000万', '1000万以上'],
            3: ['30万以下', '30~50万', '50~80万', '80~100万', '100~150万', '150万以上']
        }

        self.housetype_mapping = ['一居室', '二居室', '三居室', '四居室', '五居室', '五居室以上']

    def get_finish_trend(self, *args, **kwargs):
        # 成交走势
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        query_time = pms.get('query_time', [])

        data = dict()
        for now_time in query_time:
            result = self.analysis_month.select_one(city=city, field=['volume_total', 'volume_price'],
                                                    filter=f'datetime={now_time}')
            if result:
                result['finish_avgprice'] = result.pop('volume_price')
                result['finish_number'] = result.pop('volume_total')
                data[str(now_time)] = result
        return {'data': data}

    def finish_top5(self, *args, **kwargs):
        today = datetime.date.today()
        day2 = int(datetime.datetime.strftime((today - datetime.timedelta(days=1)), '%Y%m%d'))
        day2 = 20180702

        city = kwargs.get('city')
        result = self.complex_info.count_union_model(city=city, field=" complex_id,borough_name,complex_total ",
                                                     filter=f'datetime={day2} and complex_id!=0 order by total desc limit 5')
        res = []
        for i in result:
            sql = "select complex_id,cityarea_name,cityarea2_name,complex_name,complex_address,developer_offer from complex " \
                  f"where complex_id ={i['complex_id']} and sale_weight >= 50 and source_id REGEXP '^2#|#2#|#2$|^2$|81'"
            data = self.complex.exe_s_sql(city=city, sql={'sql': sql})
            if data:
                res.append(data)
        return {"data": res}

    def price_distribution(self, *args, **kwargs):
        city = kwargs.get('city')
        city_level = self.city_level_mapping.get(city, 3)
        price_range = self.price_range_mapping.get(city_level)

        today = datetime.date.today()
        day2 = int(datetime.datetime.strftime((today - datetime.timedelta(days=1)), '%Y%m%d'))
        day2 = 20180703

        res = dict()
        results = self.monthly_newhouse_deal_price_distributionDao.select_model(city=city,
                                                                                field=['price_range', 'count'],
                                                                                filter=f'date={day2}')
        data = dict()
        for result in results:
            data.setdefault(price_range[result['price_range'] - 1], result['count'])
        res.setdefault('data', data)
        res.setdefault('date', day2)
        return {'data': res}

    def housetype_distribution(self, *args, **kwargs):
        city = kwargs.get('city')
        today = datetime.date.today()
        day2 = int(datetime.datetime.strftime((today - datetime.timedelta(days=1)), '%Y%m%d'))
        day2 = 20180703

        res = dict()
        results = self.monthly_newhouse_deal_housetype_distributionDao.select_model(city=city,
                                                                                    field=['house_type', 'count'],
                                                                                    filter=f'date={day2}')
        data = dict()
        for result in results:
            data.setdefault(self.housetype_mapping[result['house_type'] - 1], result['count'])
        res.setdefault('data', data)
        res.setdefault('date', day2)
        return {'data': res}
