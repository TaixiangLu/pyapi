#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
import time
from utils.BaseUtils import BaseUtils
from service.BaseService.BaseMgoService import BaseMgoService
from service.BaseService.BaseMysqlService import BaseMysqlService
from apps.zhuge_borough.dao.detail.BoroughOnlineDao import BoroughOnlineDao
from apps.zhuge_borough.dao.detail.CommunityDao import CommunityDao
from apps.zhuge_borough.dao.detail.HouseSellGovDao import HouseSellGovDao
class CommunityService(BaseMgoService):
    def __init__(self, *args, **kwargs):
        self.borough_online_dao = BoroughOnlineDao(*args, **kwargs)
        self.community_dao = CommunityDao(*args, **kwargs)
        self.house_sell_gov_dao = HouseSellGovDao(*args, **kwargs)

    # 根据指定的城区id查询数据
    def get_community_list(self, *args, **kwargs):
        result = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        sort = pms.get('sort')
        page = pms.get('page')
        index = page.get('index')
        size = page.get('size')
        filter = pms.get('filter', {})
        filters = {'name': {'$regex': filter.get('name', '')}}
        field = {'_id': 1, 'name': 1, 'loc': 1, 'pinyin': 1, 'created': 1, 'updated': 1}  # 返回字段
        total = self.community_dao.find_count(city=city, filter=filters)
        data = self.community_dao.find_page(city=city, filter=filters, page=page, sort=sort, field=field)
        for community_info in data:
            boroughList = []
            id = community_info.get('_id')
            filter = {'community.community_id': id}
            #获取gov表里所属社区的房源数量
            gov_filter = 'community_id = '+str(id)
            community_info['gov_total'] = self.house_sell_gov_dao.select_count(city=city, filter=gov_filter)
            borough_infos = self.borough_online_dao.get_one(city=city, filter=filter)
            for borough_info in borough_infos:
                borough = {}
                borough['id'] = borough_info.get('_id')
                borough['name'] = borough_info.get('borough_name')
                boroughList.append(borough)
            community_info['boroughList'] = boroughList
        result.setdefault('data', data)
        result.setdefault('page', {"index": index, "size": size, "total": total})
        return result

    def create_community(self, *args, **kwargs):
        result = {}
        community_info = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        name = pms.get('community_name')
        community_page = {"index": 1, "size": 1}
        community_sort = [("_id", -1)]
        community = self.community_dao.find_page(city=city, sort=community_sort, page=community_page)[0]
        community_max_id = community.get('_id')
        community_info['_id'] = community_max_id + 1
        community_info['name'] = name
        community_info['pinyin'] = BaseUtils.getPiyin(value=name)
        community_info['created'] = int(time.time())
        community_info['updated'] = int(time.time())
        self.community_dao.insert_one(city=city, datas=community_info)
        result.setdefault('data', {"status": 1, "comunity_id": community_info['_id'], "remark": "新建社区id为%s的社区成功" % (community_info['_id'])})
        return result

    def deleteCommunity(self, *args, **kwargs):
        result = {}
        community_info = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        community_id = pms.get('community_id')
        borough_info = self.borough_online_dao.find_one(city=city,filter={"community.community_id": community_id})
        #判断社区里是否还有小区
        if borough_info:
            result.setdefault('data', {"status": -1, "comunity_id": community_id,"remark": "社区里存在小区无法删除"})
        else:
            self.community_dao.delete_by_id(city=city, filter={'_id':community_id})
            result.setdefault('data', {"status": 1, "comunity_id": community_id, "remark": "社区删除成功"})
        return result

    def modifyCommunityName(self, *args, **kwargs):
        result = {}
        community_info = {}
        borough_community_info = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        id = pms.get('community_id')
        community_info['name'] = pms.get('community_name')
        community_info['pinyin'] = BaseUtils.getPiyin(value=community_info['name'])
        community_info['updated'] = int(time.time())
        #修改社区表信息
        self.community_dao.update(city=city, filter={'_id': id},datas={'$set':community_info})
        #修改小区表里社区信息
        borough_community_info['community.community_name'] = pms.get('community_name')
        borough_community_info['community.community_py'] = community_info['pinyin']
        self.borough_online_dao.update(city=city, filter={'community.community_id':id},datas={'$set':borough_community_info})
        # 更新小区缓存
        # self.update_borough_cache(city=city, master={"id": borough_id})
        # 删除小区详情页缓存
        # self.delete_borough_detail_cache(borough_id=borough_id, city=city)
        # 更新小区es社区

        # 更新房源的社区

        result.setdefault('data', {"status": 1, "comunity_id": id, "remark": "社区主名修改成功"})
        return result

