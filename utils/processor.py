# -*- coding: utf-8 -*-
__author__ = 'zhangjg'
import re
import base64

"""
字符处理器
"""


class Processor(object):
    def __init__(self, *args, **kwargs):
        pass

    # 365tf价格处理
    def not_unit(self, *args, **kwargs):
        data = str(kwargs.get('data'))
        after_data = re.findall("\d+", data)[0]
        if after_data.isdigit():
            if int(after_data) > 6000:
                return after_data + "元/㎡"
            else:
                return after_data + "万元/套"
        else:
            return data

    """
    #提取所有处理
    """

    def extract_all2(self, *args, **kwargs):
        data_list = re.findall(kwargs.get("regex"), str(kwargs.get("data")))
        # data_list = re.search(kwargs.get("regex"), str(kwargs.get("data")))
        value = len(data_list) > 0 and '|'.join(data_list)
        return value

    def processor(self):
        value_handle = self.parm
        data = self.data

        for i in range(len(value_handle)):
            name = value_handle[i].get("name", "")
            value = value_handle[i].get("value", "")
            regex = value_handle[i].get("regex", "")
            try:
                if i == 0:
                    data = getattr(self, name)(data=data, value=value, regex=regex)
                else:
                    data = getattr(self, name)(data=data, value=value, regex=regex)
            except Exception as e:
                print(e)
        return data

    """
    #前缀处理
    """

    def prefix(self, *args, **kwargs):
        return str(kwargs.get("value")) + str(kwargs.get("data"))

    """
    #后缀处理
    """

    def postfix(self, *args, **kwargs):
        return str(kwargs.get("data")) + str(kwargs.get("value"))

    """
    #替换处理
    """

    def replace(self, *args, **kwargs):
        data = re.sub(kwargs.get("regex"), str(kwargs.get("value")), str(kwargs.get("data")))
        if kwargs.get("data") == data:
            data = re.sub(kwargs.get("regex"), str(kwargs.get("value")), kwargs.get("data"))
        return data
    """
    #提取处理
    """

    def extract(self, *args, **kwargs):
        data_list = re.findall(kwargs.get("regex"), str(kwargs.get("data")))
        # data_list = re.search(kwargs.get("regex"), str(kwargs.get("data")))
        value = len(data_list) > 0 and data_list.pop() or ""
        return value

    """
    #提取所有处理
    """

    def extract_all(self, *args, **kwargs):
        data_list = re.findall(kwargs.get("regex"), str(kwargs.get("data")))
        # data_list = re.search(kwargs.get("regex"), str(kwargs.get("data")))
        value = len(data_list) > 0 and ','.join(data_list) or ""
        return value

    """
    #提取处理'    预计2018年7月'
    """

    def extractU(self, *args, **kwargs):
        re.compile(kwargs.get("regex"))
        data_list = re.findall(kwargs.get("regex"), kwargs.get("data"))
        # data_list = re.search(kwargs.get("regex"), str(kwargs.get("data")))
        value = len(data_list) > 0 and data_list.pop() or ""
        return value

    """
    #提取处理
    """

    def base64decode(self, *args, **kwargs):
        data = kwargs.get("data")
        missing_padding = 4 - len(data) % 4
        if missing_padding:
            data += b'=' * missing_padding
        return base64.b64decode(data)

    """
    将数据按给定规则分裂regex
    """

    def fission(self, *args, **kwargs):
        regex = kwargs.get("regex", ",")
        data = kwargs.get("data")
        listdata = data.split(regex)
        return listdata

    """
    将数据按给定正则分裂regex
    """

    def preg_fission(self, *args, **kwargs):
        regex = kwargs.get("regex", ",")
        data = kwargs.get("data")

        listdata = data.split(regex)
        return listdata

    """
    #规定值
    """

    def static(self, *args, **kwargs):
        data = kwargs.get("data")
        return str(kwargs.get("value"))

    def not_extract_postfix(self, *args, **kwargs):
        data = str(kwargs.get("data"))
        regex = kwargs.get("regex")
        value = str(kwargs.get("value"))
        val = self.extract(data=data,regex=regex)
        if not val:
            return self.postfix(data=data,regex=regex,value=value)
        else:
            return data

if __name__ == '__main__':
    value = """1.2"""
    a = Processor()
    # # # (room_id\=\".*\"|owner_id\=\".*\")
    print(a.not_extract_postfix(data=value, regex='\d+\.?\d+(.+)', value="123123"))





