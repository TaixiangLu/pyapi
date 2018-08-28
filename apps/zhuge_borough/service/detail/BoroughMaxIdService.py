from service.BaseService.BaseMgoService import BaseMgoService
from apps.zhuge_borough.dao.detail.BoroughMaxIdDao import BoroughMaxIdDao
class BoroughMaxIdService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.dao = BoroughMaxIdDao(*args, **kwargs)

    def find(self, city, filter):
        data = self.get_one(city=city, filter=filter)
        return data

    # 更新mongo数据
    def update_filter(self, filter, datas, city):
        result = self.update_by_filter(filter=filter, datas=datas, city=city)
        return result

    def getMaxId(self, type, city):
        find_item = self.get_one(filter={"city": city}, city='bj')[0]
        if not find_item:
            return False
        if type == "cityarea":
            max_cityarea_id = int(find_item['cityarea_maxid']) + 1
            update_status = self.update_by_filter(filter={"city": city}, datas={'$set': {'cityarea_maxid': max_cityarea_id}}, city='bj')
            if update_status['nModified'] == 1:
                return max_cityarea_id
            else:
                return False
        elif type == "cityarea2":
            max_cityarea2_id = int(find_item['cityarea2_maxid']) + 1
            update_status = self.update_by_filter(filter={"city": city}, datas={'$set': {'cityarea2_maxid': max_cityarea2_id}}, city='bj')
            if update_status['nModified'] == 1:
                return max_cityarea2_id
            else:
                return False
        elif type == "borough":
            max_borough_id = int(find_item['borough_maxid']) + 1
            update_status = self.update_by_filter(filter={"city": city},datas={'$set': {'borough_maxid': max_borough_id}}, city='bj')
            if update_status['nModified'] == 1:
                return max_borough_id
            else:
                return False

