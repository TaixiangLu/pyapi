from controller.BaseController import Route
route = Route()
from controller.BaseController import catch_exception
from apps.zhuge_borough.service.detail.BoroughGuesswordService import BoroughGuesswordService
from controller.BaseController import BaseController
import json

"""
@email hanbingde@zhugefang.com
@desc  小区联想词业务控制层
@class_name NewController
"""

@route("/(?P<city>\w*)/borough/detail/addGuessword")
class BoroughGuesswordAdd(BaseController):
    """
    联想词添加接口
    """
    def initialize(self):
        pass

    @catch_exception  # 异常装饰器
    def post(self, *args, **kwargs):
        result = {"message": "success", "code": 200}
        pms = json.loads(self.request.body)
        city = kwargs.get('city')
        self.service = BoroughGuesswordService()
        guessword_list = pms.get("guessword_list", [])
        type_id = pms.get("type_id")
        data = self.service.add_guessword(city=city,guessword_list=guessword_list, type_id=type_id)
        result["data"] = data
        self.write(result)


@route("/(?P<city>\w*)/borough/detail/updateGuessword")
class BoroughGuesswordUpdate(BaseController):
    """
    联想词添加接口
    """
    def initialize(self):
        pass

    @catch_exception  # 异常装饰器
    def post(self, *args, **kwargs):
        result = {"message": "success", "code": 200}
        pms = json.loads(self.request.body)
        city = kwargs.get('city')
        self.service = BoroughGuesswordService()
        guessword_list = pms.get("guessword_list", [])
        type_id = pms.get("type_id")
        data = self.service.update_guessword(city=city,guessword_list=guessword_list, type_id=type_id)
        result["data"] = data
        self.write(result)

@route("/(?P<city>\w*)/borough/detail/modify")
class BoroughGuesswordModify(BaseController):
    def initialize(self):
        pass

    @catch_exception  # 异常装饰器
    def post(self, *args, **kwargs):
        result = {"message": "success", "code": 200}
        pms = json.loads(self.request.body)
        city = kwargs.get('city')
        self.service = BoroughGuesswordService()
        guessword_dict = pms.get('guessword_dict', {})
        data = self.service.modify_borough_name(city=city,guessword_dict=guessword_dict)
        result["data"] = data
        self.write(result)

@route("/(?P<city>\w*)/borough/detail/delete")
class BoroughGuesswordDelete(BaseController):
    def initialize(self):
        pass
    @catch_exception  # 异常装饰器
    def post(self, *args, **kwargs):
        result = {"message": "success", "code": 200}
        pms = json.loads(self.request.body)
        city = kwargs.get('city')
        self.service = BoroughGuesswordService()
        guessword_list = pms.get("guessword_list", [])
        type_id = pms.get("type_id")
        data = self.service.delete_guessword(city=city,guessword_list=guessword_list, type_id=type_id)
        result["data"] = data
        self.write(result)
