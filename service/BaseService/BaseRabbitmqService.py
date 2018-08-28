# -*- coding: utf-8 -*-
from dao.BaseDao.BaseRabbitmq import BaseRabbitmq

class BaseRabbitmqService(object):
    def __init__(self):
        pass

    def producer(self, *args, **kwargs):
        return BaseRabbitmq(conf_name=kwargs.get("conf_name")).basic_publish(*args, **kwargs)

    def consumer(self, *args, **kwargs):
        return BaseRabbitmq(conf_name=kwargs.get("conf_name")).basic_consume(*args, **kwargs)

if __name__ == '__main__':
    # import json
    # house_number = "122114"
    # gov_id = 133
    # house_id = 122
    # city = "heb"
    # a = {"house_number": house_number, "gov_id": gov_id, "house_id": house_id, "city": city}
    # BaseRabbitmqService().producer(conf_name='sell', queue="info", body=json.dumps(str(a)))
    BaseRabbitmqService().consumer(conf_name='sell', queue="info")