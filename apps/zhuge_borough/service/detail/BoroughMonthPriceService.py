#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
import time
from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.BoroughMonthPriceDao import BoroughMonthPriceDao


class BoroughMonthPriceService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = BoroughMonthPriceDao(*args, **kwargs)

    def find_page(self, city, pms):
        filter = {}
        borough_ids = []
        yymm = int(time.strftime("%Y%m", time.localtime(int(time.time())))) - 1
        if pms.get("ravg_price") != '' and pms.get("bavg_price") != '':
            filter.setdefault("avg_price",
                              {"$gte": int(pms.get("ravg_price") * 10000), "$lte": int(pms.get("bavg_price")) * 10000})
        elif pms.get("ravg_price") != '':
            filter.setdefault("avg_price", {"$lte": int(pms.get("ravg_price")) * 10000})
        elif pms.get("bavg_price") != '':
            filter.setdefault("avg_price", {"$gte": int(pms.get("bavg_price")) * 10000})
        if filter:
            filter.setdefault("yymm", yymm)
            page = {"index": pms.get("index", 1), "size": 99999}
            # page = {}
            info = self.dao.find_page(city=city, page=page, field={'borough_id': 1, "_id": 0}, filter=filter)
            for index, item in enumerate(info):
                borough_ids.append(item.get("borough_id"))
        return borough_ids

    def getPriceByBoroughId(self, borough_id, city):
        filter = {'borough_id': borough_id}
        sort = [("yymm", -1)]
        page = {"index": 1, "size": 1}
        price_data = self.dao.find_page(city=city, filter=filter, field={'avg_price'}, sort=sort, page=page)
        if len(price_data) == 1:
            price = price_data[0].get('avg_price', '')
        else:
            price = 0
        return price

    def get_page(self, *args, **kwargs):
        filter = kwargs.get('filter')
        borough_id = filter.get('id')  # 起始时间
        startMonth = filter.get('startMonth')  # 起始时间
        endMonth = filter.get('endMonth')  # 结束时间
        kwargs['borough_id'] = borough_id
        kwargs['field'] = {'_id': 0, 'time': 0, 'date': 0, 'count_gov': 0, 'borough_id': 0}
        kwargs['filter'] = {'yymm': {'$gte': startMonth, '$lte': endMonth}, 'borough_id': borough_id}  # 过滤条件
        kwargs['cache_key'] = str(borough_id) + str(startMonth) + str(endMonth)  # 缓存小key
        return self.dao.getBoroughMonthPrice(*args, **kwargs)

    # 获取分页
    def get_count(self, *args, **kwargs):
        filter = kwargs.get('filter')
        borough_id = filter.get('id')  # 起始时间
        startMonth = filter.get('startMonth')  # 起始时间
        endMonth = filter.get('endMonth')  # 结束时间
        kwargs['filter'] = {'yymm': {'$gte': startMonth, '$lte': endMonth}, 'borough_id': borough_id}  # 过滤条件
        return self.dao.find_count(*args, **kwargs)

    def get_new_price(self, *args, **kwargs):
        borough_id = kwargs['borough_id']
        kwargs['page'] = {'index': 1, 'size': 1}
        kwargs['sort'] = [('date', -1)]
        kwargs['field'] = {'_id': 0, 'avg_price': 1, 'date': 1,'borough_name':1}  # 返回字段
        kwargs['filter'] = {'borough_id': borough_id}
        return self.dao.find_page(*args, **kwargs)

    def get_percent(self, *args, **kwargs):
        """
        获取涨跌百分比
        """
        nowPrice = kwargs.get("nowPrice")
        prePrice = kwargs.get("prePrice")
        if prePrice:
            percent = round((nowPrice - prePrice) / prePrice, 4)
        else:
            0
        return percent

    def get_type(self, *args, **kwargs):
        """
        获取涨跌状态
        """
        nowPrice = kwargs.get("nowPrice")
        prePrice = kwargs.get("prePrice")

        if nowPrice - prePrice:
            return 1
        else:
            return -1

    def getBoroughMonthPrice(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        sort = pms.get('sort', {})
        sort = [('yymm', 1)] if sort.get('yymm', 1) == 1 else [('yymm', -1)]
        page = pms.get('page') or {"index": 1, "size": 30}
        index = page.get('index')
        size = page.get('size')
        filter = pms.get('filter')
        total = self.get_count(city=city, filter=filter)
        data = self.get_page(city=city, sort=sort, page=page, filter=filter)
        result.setdefault('data', data)
        result.setdefault('page', {"index": index, "size": size, "total": total})
        return result


if __name__ == '__main__':
    BMP = BoroughMonthPriceService()

    res = BMP.getPriceByBoroughId(borough_id=1000001, city='bj')

    print(res)
