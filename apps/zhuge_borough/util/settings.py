#!/usr/bin/env python3
# -*- coding: utf-8 -*-

####################### local mysql ################
Host = "localhost"
Port = 3306
User = "root"
Pass = "123456"
Db = "test"
Char = "utf8"


MysqlConfig = dict(
    host=Host,
    port=Port,
    user=User,
    passwd=Pass,
    db=Db,
    charset=Char
)

############## house_sell_gov #####################################
Params = "id,house_price,house_room,house_hall,house_totalarea,house_topfloor,"\
    "house_floor,house_toward,house_fitment,house_type"\
    ",borough_id,property_right_years"  # ,source_url"

################ 朝向 ###############################
Toward = {'1': '东', '10': '东西', '11': '东西南', '12': '东南北', '13': '西南北',
          '14': '东西北', '15': '东南西北', '2': '西', '3': '南', '4':
          '北', '5': '东南', '6': '西南', '7': '东北', '8': '西北', '9': '南北'}

AdjToward = {
    '东': 0.99,
    '东北': 0.97,
    '东南': 1.02,
    '东西': 1,
    '北': 0.95,
    '南': 1.03,
    '南北': 1.04,
    '西': 0.98,
    '西北': 0.96,
    '西南': 1.01
    }

################# 装修 ##################################
Fitment = dict([('1', '毛坯'), ('2', '简装修'), ('3', '中装修'),
                ('4', '精装修'), ('5', '豪华装修')])

AdjFitment = {'1': 0.98, '2': 1, '3': 1, '4': 1.02, '5': 1.04}
################ 诸葛MYSQL ###########################################
_User = "data_r"
_Pass = "BQ6Qr1*dIh%##bK3zg5p0M6x@Mkqs&hg"
_Host = "mysql.zhugefang.com"
_Port_old = 9524
_Port_new = 9512

ZGSQLOLD = {  # 诸葛老sql
    "host": _Host,
    "user": _User,
    "passwd": _Pass,
    "port": _Port_old,
    "charset": "utf8"
}

ZGSQLNEW = {  # 诸葛新sql
    "host": _Host,
    "user": _User,
    "passwd": _Pass,
    "port": _Port_new,
    "charset": "utf8"
}
######################## MongoDB uri ##########################################
MONGO_URL = "mongodb://zhuge:7UgfAWbUtTKeMVGrSLwsHjB9xGjumnck@dds"\
    "-2ze2f6d07e237dd41500-pub.mongodb.rds.aliyuncs.com:3717"
