#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/5/15 下午5:44
# @Author  : janguo@zhugefang.com
# @Site    : 
# @File    : BaseDao.py
# @Software: PyCharm

#
from apps.config.common import fm

def hfm(fun):
    def wrapper(*args, **kwargs):
        datas = (fun(*args, **kwargs))
        if isinstance(datas, dict):
            for k, v in datas.items():
                if k in fm:
                    field = fm.get(k)
                    if isinstance(v, (int, str)):
                        datas[k] = field.get(v)
                    elif isinstance(v, list):
                        datas[k] = [field.get(i) for i in v]
        elif isinstance(datas, list):
            for data in datas:
                for k, v in data.items():
                    if k in fm:
                        field = fm.get(k)
                        if isinstance(v, (int, str)):
                            data[k] = field.get(v)
                        elif isinstance(v, list):
                            data[k] = [field.get(i) for i in v]
        return datas
    return wrapper

@hfm
def mmm():
    return {"house_toward": 1, "tag_list": ["1", "2"],"house_info": {"house_toward": 1}}

if __name__ == '__main__':

    print(mmm())

