# -*- coding: utf-8 -*-
import re
import logging

class StringUtils:
    def __init__(self):
        pass

    # 去掉前后空格
    @staticmethod
    def to_strip(sd):
        return sd.strip()

    @staticmethod
    def to_strip_sign(sd):
        sd = re.sub(r"[,，.:：;；\s]+$", '', unicode(sd))
        sd = re.sub(r"^[,，.。:：;；\s]+", '', sd)
        return sd

    # 分割
    @staticmethod
    def to_split(sd, sep=","):
        return sd.split(sep)

    # 字符串转化为int
    @staticmethod
    def to_int(sd):
        try:
            if sd:
                if isinstance(sd, str):
                    try:
                        sd = int(sd)
                    except Exception as e:
                        logging.error('to_int', e)
            else:
                sd = 0
        except Exception as e:
            logging.error('to_int', e)
        return sd

    #字符串转化为float
    @staticmethod
    def to_float(sd):
        try:
            if sd:
                if isinstance(sd, str):
                    try:
                        sd = float(sd)
                    except Exception as e:
                        logging.error('to_float', e)
            else:
                sd = 0
        except Exception as e:
            logging.error('to_float', e)
        return sd

    #字符串转为list
    @staticmethod
    def to_list(sd,sep=","):
        try:
            if sd and isinstance(sd, str):
                list_data = sd.split(sep)
        except Exception as e:
            logging.error('to_list', e)
        return list_data

    #字符串md5加密
    @staticmethod
    def getMd5(value):
        import hashlib
        m2 = hashlib.md5()
        m2.update(value.encode())
        return m2.hexdigest()

    # 拼接图片&视频上传七牛路径
    @staticmethod
    def rule_join(*args, **kwargs):
        rule = ''
        pic_url = StringUtils.getMd5(kwargs.get("pic_source_url"))
        if kwargs.get("theme") == 'sell':
            if type != 'video':
                rule = '/sell/' + str(kwargs.get("type")) + '/' + str(kwargs.get("city_id")) + '_' + str(kwargs.get("source")) + '_' + str(kwargs.get("gov_id")) + '_' + str(pic_url) + '.jpg'
            else:
                rule = '/sell/' + str(kwargs.get("type")) + '/' + str(kwargs.get("city_id")) + '_' + str(kwargs.get("source")) + '_' + str(kwargs.get("gov_id")) + '_' + str(pic_url)
        elif kwargs.get("theme") == 'rent':
            rule = '/rent/' + str(kwargs.get("type")) + '/' + str(kwargs.get("city_id")) + '_' + str(kwargs.get("source")) + '_' + str(kwargs.get("gov_id")) + '_' + str(pic_url) + '.jpg'
        return rule