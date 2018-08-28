from dao.BaseDao.BaseMysql import BaseMysql
from apps.zhuge_borough.model.CitizensHousePrice import CitizensHousePrice
class CitizensHousePriceDao(BaseMysql):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", CitizensHousePrice), conf_name=kwargs.get("conf_name", "sell_analysis"))
