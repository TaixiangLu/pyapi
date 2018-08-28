from dao.BaseDao.BaseMongo import BaseMongo
from apps.zhuge_borough.model.Cityarea2Recycle import Cityarea2Recycle
class Cityarea2RecycleDao(BaseMongo):
    def __init__(self, *args, **kwargs):
        super().__init__(model=kwargs.get("model", Cityarea2Recycle), conf_name=kwargs.get("conf_name", "borough"))
