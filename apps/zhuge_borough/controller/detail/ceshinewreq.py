#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 17-10-11 下午7:49
# @Author  : jianguo@zhugefang.com
# @Desc    :
import json


import requests
if __name__ == '__main__':

    # respor = requests.post("http://127.0.0.1:8080/broker/focus/borough/info", json=pms)
    pms = {
       "city":"heb",
       "master_cityarea":{
            "cityarea_id": 3,
            "cityarea_name": "南岗",
            "cityarea2_id":2,
            "cityarea2_name":"保健路"
        },
       "slave_cityarea":{
                "cityarea_id": 3,
                "cityarea_name": "南岗",
                "cityarea2_id": 448,
                "cityarea2_name": "保健"
        }
    }
    respor = requests.post("http://127.0.0.1:3301/Borough/Api/merge/cityarea2", json=pms)

    print(respor.content)


