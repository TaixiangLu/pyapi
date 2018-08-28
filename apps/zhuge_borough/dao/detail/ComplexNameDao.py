# -*- coding: utf-8 -*-
from dao.BaseDao.BaseMysql import BaseMysql
from apps.zhuge_borough.model.ComplexName import ComplexName
from cache.LocalCache import LocalCache


class ComplexNameDao(BaseMysql):
    def __init__(self, *args, **kwargs):
        self.model = ComplexName()
        super().__init__(conf_name=kwargs.get("conf_name", "appraisal"),
                         model=kwargs.get("model", self.model))

    @LocalCache(conf_name="borough_api", key="borough_name", time=3600)
    def getBoroughNameByQuery(self, *args, **kwargs):
        return self.exe_s_sqls(*args, **kwargs)
