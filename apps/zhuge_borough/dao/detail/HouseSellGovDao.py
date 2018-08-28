from dao.BaseDao.BaseMysql import BaseMysql
from apps.zhuge_borough.model.HouseSellGov import HouseSellGov

class HouseSellGovDao(BaseMysql):
    def __init__(self, *args, **kwargs):
        super().__init__(conf_name=kwargs.get("conf_name", "sell"), model=kwargs.get("model", HouseSellGov))


