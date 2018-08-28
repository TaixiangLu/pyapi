
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-1-13 下午2:30
# @Author  : jianguo@zhugefang.com
# @Desc    :
from dao.BaseDao.BaseMysql import BaseMysql
from apps.zhuge_borough.dao.detail.AnalysisDayDao import AnalysisDayDao
import time

class Complex_Analysis_dayService(BaseMysql):
    def __init__(self, *args, **kwargs):
        self.dao = AnalysisDayDao(*args, **kwargs)

    #l列表分割
    def list_of_groups(self, init_list, childern_list_len):
        list_of_groups = zip(*(iter(init_list),) * childern_list_len)
        end_list = [list(i) for i in list_of_groups]
        count = len(init_list) % childern_list_len
        end_list.append(init_list[-count:]) if count != 0 else end_list
        return end_list


    def get_house_total_totalarea(self, *args, **kwargs):
        '''
        :param args:       现房网上签约-总套数   exist_online_sign_total
        :param kwargs:     现房网上签约-总面积   exist_online_sign_totalarea
        :return:            时间           created

        昨天的成交面积  导49天数据,7天一个均值面积  每天的成交面积   近7天的成交总套数
                     30   天 --每天往前导7天 然后求均值
        '''

        all_dict = {}
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        times = pms.get('filter')[0]
        all_time = []

        timemap = str(times[0:4]) + '-' + str(times[4:6])
        timeArray = time.strptime(timemap, "%Y-%m")
        timeStamp = int(time.mktime(timeArray))
        now_timeStamp = timeStamp - 86400
        for x in range(7):
            old_time = timeStamp - 86400
            all_time.append(old_time)
            timeStamp = old_time
        ##37天前
        sq_time = now_timeStamp - 3196800

        #数量         返回total
        sql = {'sql': f"""select exist_online_sign_total,exist_online_sign_totalarea,created from thor_bj.analysis_day where created >= %s group by created order by created desc""" % sq_time}

        datas = self.dao.exe_s_sqls(city=city, sql=sql)
        data = self.list_of_groups(datas, 7)
        # 昨天的成交面积
        now_totalarea = data[0][0].get("exist_online_sign_totalarea")
        all_dict['now_totalarea'] = now_totalarea
        ## 更新时间
        now_time = data[0][0].get("created")
        all_dict['now_time'] = now_time
        # 每天的成交面积
        all_totalarea = []
        for day in datas:
            all_totalarea.append({day.get('created') : day.get("exist_online_sign_totalarea")})
        all_dict['all_totalarea'] = all_totalarea

        #  7天一个均值面积 30个值
        # weeks_totalarea = self.list_of_groups(all_totalarea,7)
        week_totalarea = []
        # for week in weeks_totalarea:
        #     nums = sum(week) / 7
        #     week_totalarea.append(nums)
        # all_dict['week_totalarea'] = week_totalarea
        data_len = len(all_totalarea)
        if data_len >= 37:
            for total in range(30):
                nums = 0
                for num in range(7):
                    nums += int(all_totalarea[num].values()[0])
                date = all_totalarea.pop(0).keys()[0]

                week_totalarea[date] = nums / 7
        all_dict['week_totalarea'] = week_totalarea

        #  近7天的平均成交套数
        one_week_total = []
        for week in data[0]:
            sign = week.get("exist_online_sign_total")
            print(sign)
            if sign != ' ':
                to = int(week.get("exist_online_sign_total"))
            else:
                to = 0
            one_week_total.append(to)
        week_total = int(float(sum(one_week_total) / 7))
        all_dict['week_total'] = week_total

        return all_dict