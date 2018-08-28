#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from numpy import percentile
from .settings import MysqlConfig, Params
from .settings import Toward, Fitment, AdjToward, AdjFitment
#from functools import reduce


__all__ = ["ProcessHouse"]

LIMIT = 60  # 分值在LIMIT一下的不考虑


class _House(object):
    """
    House类型用于存储house_sell_gov的记录，其属性即为表的字段
    """

    def __init__(self, *args):
        self.__params = Params.split(",")
        self.initalize(args)
        self.avgprice = round(float(self.house_price) /
                              float(self.house_totalarea), 4)

        assert len(self) == len(self.__dict__) - 1

    def initalize(self, args):
        assert len(self.__params) == len(args), "feilds args len" \
            " must be equal"
        for attr, value in zip(self.__params, args):
            self.__dict__[attr] = value

    def __repr__(self):
        attrs = ["{}={!r}".format(k, v) for k, v in
                 self.__dict__.items() if not k.startswith("_")]
        attrs = ",".join(attrs)
        cls_name = self.__class__.__name__
        return "{}({})".format(cls_name, attrs)

    def __len__(self):
        return len(self.__params) + 1

    def check(self):
        """
        用于校验，以免出现属性错位的情况
        """
        return len(self) == len(self.__dict__) - 1


class ProcessHouse(object):
    """
    ProcessHouse类是对房屋价格预测的主要模块，其函数主要三部分：
    1)过滤: 在函数名称中含有filter的函数
    2)评分: 函数名称中含有 grade的
    3)微调: 函数名词中含有 adjust的函数

    评分和微调各有一个汇总函数，最终的预测执行函数是process
    eg:
        prochouse = ProcessHouse()
        preprice = prochouse.process(Data, predict)

        Data: 为样本数据集， predict为带估房源
    """

    def __init__(self, Params):
        self.__params = Params
        self.__data = None
        self.__tddict = Toward
        self.__fitdict = Fitment
        self._ptn = re.compile(r"[0-9]{2}")

    def _filter(self):
        pass

    def adjust_house(self, house, predict):
        """
        微调汇总函数
        """
        floor = self.adjust_floor(
            house.house_topfloor,
            house.house_floor,
            predict.house_topfloor,
            predict.house_floor)
        area = self.adjust_area(
            house.house_totalarea,
            predict.house_totalarea)
        toward = self.adjust_toward(
            house.house_toward,
            predict.house_toward)
        fitment = self.adjust_fitment(
            house.house_fitment,
            predict.house_fitment)
        #blyear = self.adjust_built_year(
        #    house.house_built_year,
        #    predict.house_built_year)

        #tmp = ["floor", "area", "toward", "fitment", "bluildyear"]
        #adjlist = [floor, area, toward, fitment, blyear]
        #adjlist = [floor, area, toward, fitment]
        #return adjlist
        #return dict(zip(tmp, adjlist))
        #f = lambda a, b: a * b
        #return reduce(f, [floor, area, toward, fitment])
        return floor * area * toward * fitment  # * blyear

    def adjust_fitment(self, fith, fitp):
        """
        微调装修
        """
        if fith == '0' or fitp == '0':
            return 1
        adjh = AdjFitment.get(fith, 1)
        adjp = AdjFitment.get(fitp, 1)
        return adjp / adjh

    def adjust_built_year(self, blyearh, blyearp):
        """
        微调建筑年代
        """
        if not (blyearh and blyearp):
            return 1
        yh = float(blyearh)
        yp = float(blyearp)
        return 1/(1 + (yh - yp) * 0.005)

    def adjust_toward(self, twdh, twdp):
        """
        微调朝向
        """
        try:
            tdh = Toward[twdh]
            tdp = Toward[twdp]
            adjh = AdjToward[tdh]
            adjp = AdjToward[tdp]
            return adjp / adjh
        except KeyError:
            return 1

    def adjust_area(self, areah, areap):
        """
        微调面积
        """
        adjah = self._adjust_area(areah)
        adjap = self._adjust_area(areap)
        return adjap / adjah

    def _adjust_area(self, house_totalarea):
        X = 0.01
        house_totalarea = float(house_totalarea)
        if house_totalarea < 50:
            return 1-X
        elif house_totalarea >= 50 and house_totalarea < 70:
            return 1
        elif house_totalarea >= 70 and house_totalarea < 90:
            return 1+X
        elif house_totalarea >= 90 and house_totalarea < 120:
            return 1
        elif house_totalarea >= 120 and house_totalarea < 140:
            return 1-X
        else:
            return 1-X * 2

    def adjust_floor(self, tfloorh, floorh, tfloorp, floorp):
        """
        微调楼层
        """
        adjfh = self._adjust_floor(tfloorh, floorh)
        adjfp = self._adjust_floor(tfloorp, floorp)
        return adjfp / adjfh

    def _adjust_floor(self, tfloor, floor):
        floor = self._trans_floor(floor, tfloor)
        if tfloor < 8:
            mid = sum(divmod(tfloor, 2))
            return 1 - abs(floor-mid) * 0.01
        mid = tfloor // 2 + 1
        return 1 - (mid - floor) * 0.003

    def process(self, data, predict):
        """
        预测处理函数
        """
        self.__data = []
        pricelist = []
        for d in data:
            if isinstance(d, _House):
                house = d
            else:
                try:
                    if isinstance(d, dict):
                        d = [d[x] for x in Params.split(",")]
                    house = _House(*d)
                except Exception as e:
                    print(d)
                    print(e)
                    continue

            if not self.area_filter(house.house_totalarea,
                                    predict.house_totalarea):
                continue
            if not self.property_filter(house):
                continue

            pricelist.append(house.avgprice)
            self.__data.append(house)

        if not pricelist:
            return None, "pricelist is empty"
        mean = percentile(pricelist, 50)
        #lowlimit = percentile(pricelist, 1)
        #highlimit = percentile(pricelist, 99)

        gradelist = []
        for h in self.__data:
            #try:
                if not self.price_filter_mean(h.avgprice, mean):
                    continue
                grade = self.grade_house(h, predict)

                if not (grade >= LIMIT):  # LIMIT 分一下的房源不考虑
                    continue
                gradelist.append((grade, h))
            #except Exception:
            #    pass
            #if not h.check():
            #    break

        gradelist = sorted(gradelist, key=lambda x: x[0], reverse=True)[:6]
        gradelist = sorted(gradelist, key=lambda x: x[1].house_price)
        if len(gradelist) == 6:
            gradelist = gradelist[1:-1]
        elif len(gradelist) == 5:
            gradelist = gradelist[1:]

        adjtotal = 0
        for grade, house in gradelist:
            print(self.adjust_house(house, predict))
            print(house.house_price)
            # adjtotal += house.house_price * self.adjust_house(house, predict)
            adjtotal += house.avgprice * self.adjust_house(house, predict)

        if len(gradelist) == 0:
            return None, "gradelist is empty"

        adjprice = adjtotal / len(gradelist)
        #return round(adjprice, 2), gradelist
        return round(adjprice*float(predict.house_totalarea), 2), "ok"

    def grade_house(self, house, predict):

        """
        评分汇总函数
        """
        g_area = self.grade_area(
            house.house_totalarea,
            predict.house_totalarea)

        g_tfloor = self.grade_topfloor(
            house.house_topfloor,
            predict.house_topfloor)

        g_floor = self.grade_floor(
            house.house_floor,
            predict.house_floor,
            house.house_topfloor,
            predict.house_topfloor)

        g_twd = self.grade_toward(
            house.house_toward,
            predict.house_toward)

        g_rmh = self.grade_roomhall(
            (house.house_room, house.house_hall),
            (predict.house_room, predict.house_hall))

        g_fit = self.grade_fitment(
            house.house_fitment,
            predict.house_fitment)

        #glist = [g_area, g_tfloor, g_floor, g_twd, g_rmh, g_fit]
        #return sum(i for i in glist)
        return sum([g_area, g_tfloor, g_floor, g_twd, g_rmh, g_fit])

    def price_filter_mean(self, house_price, mean):
        """
        均价过滤函数
        """
        if house_price:
            return (house_price <= 1.3 * mean
                    ) and (house_price >= 0.7 * mean)

        return False

    def price_filter_percentile(self, house_price, low, high):
        """
        分位数过滤函数
        """
        if house_price:
            return (house_price) >= low and (house_price <= high)
        return False

    def property_filter(self, h):
        """
        产权过滤, 过滤掉产权存在，而非70年产权的
        """
        res = self._ptn.findall(h.property_right_years)
        if not res:
            return True
        elif "70" in set(res):
            return True
        else:
            return False

    def area_filter(self, areah, areap):
        """
        面积过滤函数
        """
        t = float(areah) / float(areap)
        #return (t >= 0.5) and (t <= 2)
        return (t >= 0.75) and (t <= 1.35)

    def grade_area(self, areah, areap):  # jian zhu mian ji
        """
        面积评分
        """
        areah = float(areah)
        areap = float(areap)
        g = 20 - abs(areah - areap) // 10 * 2
        return g if g > 0 else 0

    def grade_topfloor(self, tfloorh, tfloorp):  # zong lou cheng
        """
        总楼层评分
        """
        if tfloorh == 0:
            return 0
        g = 15 - abs(tfloorh - tfloorp) // 3 * 3
        return g if g > 0 else 0

    def _trans_floor(self, floor, tfloor):
        def __trans(floor, tfloor):
            m1 = tfloor / 3
            m2 = tfloor / 3 * 2
            if floor == "低":
                return (1 + m1) // 2
            elif floor == "中":
                return (m1 + m2) // 2
            elif floor == "高":
                return (m2 + tfloor) // 2
        if isinstance(floor,int):
            return floor
        if not floor.strip():
            return -1
        if floor in {"低", "中", "高"}:
            x = int(__trans(floor, tfloor))
            return 0 if x < 1 else x
        if float(floor) < 1:
            return 1
        return int(floor)

    def grade_floor(self, floorh, floorp, tfloorh, tfloorp):  # 所在楼层
        """
        所在楼层评分
        """
        floorh = self._trans_floor(floorh, tfloorh)
        floorp = self._trans_floor(floorp, tfloorp)
        if floorh == -1:
            return 0
        g = 15 - abs(floorh - floorp) // 3 * 1
        return g if g > 0 else 0

    def grade_toward(self, towardh, towardp):
        """
        朝向评分
        """
        if (towardh not in self.__tddict) or (
                towardp not in self.__tddict):
            return 8
        tdh = self.__tddict[towardh]
        tdp = self.__tddict[towardp]
        if tdh == tdp:
            return 15
        elif (tdh in tdp) or (tdp in tdh):
            return 10
        else:
            return 8

    def grade_build(self, buildh, buildp):
        """
        楼栋评分，没有相关字段，未实现
        """
        return NotImplementedError

    def grade_roomhall(self, rhth, rhtp):
        """
        房屋格局评分，只有室厅
        """
        if rhth == rhtp:
            return 10
        return 5

    def grade_structure(self, structh, structp):
        """
        建筑结构评分，未实现
        """
        return NotImplementedError

    def grade_fitment(self, fith, fitp):
        """
        装修评分
        """
        if fith == fitp:
            return 10
        if fith != '0':
            return 5
        return 0


def main():
    import pymysql
    # sql = "select {} from house_sell_gov where borough_id={}".format(
    """
    5769651 2200    4   2   228 0   低  6   5   1   1996    1024002 1527869566
    """

    predict = {'borough_id': 1024002,
               'house_built_year': '1996',
               'house_fitment': '5',
               'house_floor': '低',
               'house_hall': 2,
               'house_price': 2200.0,
               'house_room': 4,
               'house_topfloor': 0,
               'house_totalarea': '228',
               'house_toward': '6',
               'house_type': 1,
               'updated': 1528151762,
               'id': 5769651,
               "property_right_years": "",
               #"source_url": ""
               }

    predict = [predict[x] for x in Params.split(",")]
    predict = _House(*predict)

    sql = "select {} from house_sell_gov where borough_id={}".format(
        Params, predict.borough_id)
    process = ProcessHouse(Params)
    with pymysql.connect(**MysqlConfig) as cur:
        cur.execute(sql)
        data = cur.fetchall()
        data = [d for d in data if d[0] != predict.id]
        # print(data)

        price, glist = process.process(data, predict)

        print(price, glist)
        print(predict)


if __name__ == "__main__":
    main()
