from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.Cityarea2RecycleDao import Cityarea2RecycleDao
class Cityarea2RecycleService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = Cityarea2RecycleDao(*args, **kwargs)

    def insert_one(self, city, datas):
        result = self.add_one(city=city, datas=datas)
        return result
