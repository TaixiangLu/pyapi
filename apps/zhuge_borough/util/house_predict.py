#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from .settings import Params, MONGO_URL
from .settings import ZGSQLNEW, ZGSQLOLD
from apps.zhuge_sell.dao.detail.HouseSellGovDao import HouseSellGovDao
from apps.zhuge_borough.dao.detail.CityMonthListPriceDao import CityMonthListPriceDao
from apps.zhuge_borough.dao.detail.Cityarea2MonthPriceDao import Cityarea2MonthPriceDao
from apps.zhuge_borough.dao.detail.CityareaMonthPriceDao import CityareaMonthPriceDao
from apps.zhuge_borough.dao.detail.BoroughMonthPriceDao import BoroughMonthPriceDao

from .process_house import _House, ProcessHouse

pm = ProcessHouse(Params)


def query_data(Pool, citydb, selsql):
    """
    使用协程的sql执行函数
    """
    Pool.execute("use {}".format(citydb))
    Pool.execute(selsql)
    return(Pool.fetchall())


def query_govdata(city, borough_id):
    """
    查询小区的样本房源
    """
    selsql = "select {} from house_sell_gov where "\
             "status=1 and house_price!=0 and "\
             "house_totalarea!=0 and borough_id="\
             "{} and house_price is not null".format(Params, borough_id)
    govs = HouseSellGovDao().exe_s_sqls(city=city,sql={"sql":selsql})
    return govs


def query_mongo(city, borough_id, cityarea_id, cityarea2_id):
    queryb = {"borough_id": int(borough_id)}
    queryc = {"cityarea_id": int(cityarea_id)}
    queryc2 = {"cityarea2_id": int(cityarea2_id)}
    page = {"index": 1, "size": 1}
    sorts = [("yymm", -1), ("date", -1)]
    bitem = BoroughMonthPriceDao().find_page(city=city,filter=queryb,page=page,sort=sorts)
    if len(bitem):
        avg_price = bitem[0].get("avg_price")
        if avg_price:
            return {"avg_price":avg_price,"type":1}
    c2item = Cityarea2MonthPriceDao().find_page(city=city,filter=queryc2,page=page,sort=sorts)
    if len(c2item):
        avg_price = c2item[0].get("avg_price")
        if avg_price:
            return {"avg_price":avg_price,"type":2}
    c1item = CityareaMonthPriceDao().find_page(city=city,filter=queryc,page=page,sort=sorts)
    if len(c1item):
        avg_price = c1item[0].get("avg_price")
        if avg_price:
            return {"avg_price":avg_price,"type":3}
    cityitem = CityMonthListPriceDao().find_page(city=city,page=page,sort=sorts)
    if len(cityitem):
        avg_price = cityitem[0].get("avg_price")
        if avg_price:
            return {"avg_price":avg_price,"type":4}

    return {"avg_price":avg_price,"type":5}


def compute_price(city, borough_id, aim):
    """
    根据房源所属小区的房源和相似小区的房源进行股价，返回值是一个价格list
    相似度越高的小区的估算价格越靠前
    """
    aim = [aim[x] for x in Params.split(",")]
    Phouse = _House(*aim)
    datas = query_govdata(city, borough_id)
    res, reason = pm.process(datas, Phouse)
    print(res, reason)
    return [res]


def predict(city, borough_id, cityarea_id, cityarea2_id, aimhouse):
    """
    外部调用的借口，通过传入：数据库名， 小区id， 城区id， 城市id，待估房源，
    诸葛标记 返回一个估算价格，返回的优先级为：
    1 所在小区估价
    2 相似小区估价（相似度高的优先）
    3 所在小区均价
    # 4 商圈均价
    5 城区均价
    6 城市均价

    诸葛标记是指 城市的数据库在新sql(为True)还是老sql库(为False)
    """
    ress = compute_price(city, borough_id, aimhouse)
    print("ress", ress)
    for res in ress:
        if not res:
            continue
        return {"avg_price":res,"type":0}
    house_totalarea = aimhouse.get("house_totalarea", 0)
    data = query_mongo(city, borough_id, cityarea_id, cityarea2_id)
    data["avg_price"] = data["avg_price"]*float(house_totalarea)/10000
    return data

def main():
        predict1 = {
               'borough_id': 1024002,
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
        borough_id = predict1["borough_id"]
        #res = yield predict("spider", borough_id, 1, 1, predict1, False)

        res = predict("spider", borough_id, 1, 1, predict1, False)
        print(res)

if __name__ == "__main__":
    #loop.run_sync(main)
    main()
