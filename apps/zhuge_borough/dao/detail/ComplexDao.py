# -*- coding: utf-8 -*-
from dao.BaseDao.BaseMysql import BaseMysql
from apps.zhuge_borough.model.Complex import Complex


class ComplexDao(BaseMysql):
    def __init__(self, *args, **kwargs):
        self.model = Complex()
        super().__init__(conf_name=kwargs.get("conf_name", "complex"),
                         model=kwargs.get("model", self.model))

    def get_complex_id(self, city, complex_name):
        sql = f"SELECT complex_id FROM {self.table} WHERE complex_name='%s'" % (complex_name)
        data = self.exe_s_sql(city=city, sql={'sql': sql})
        return data

    def get_dinstinct_by_filter(self, *args, **kwargs):
        pms = kwargs.get('pms')
        filters = pms.get("filter", {})
        filters_str = ''
        for i, v in filters.items():
            filters_str += "and %s='%s'" % (i, v)
        sql = f"SELECT DISTINCT cityarea_name,cityarea_id FROM {self.table} WHERE 1=1 and cityarea_name !='' %s" % (filters_str)
        data = self.exe_s_sqls(city=kwargs.get('city'), sql={'sql': sql})
        return data

    def get_dinstinct_cityarea(self, *args, **kwargs):
        filter = kwargs.get('filter', '1=1')
        field = ",".join(kwargs.get('field', ['*']))
        sql = f"SELECT DISTINCT {field} FROM {self.table} WHERE {filter}"
        data = self.exe_s_sqls(city=kwargs.get("city"), sql={'sql': sql})
        return data

    def get_developer_offer(self, *args, **kwargs):
        cityarea_id = kwargs.get('cityarea_id')
        field = ['complex_id', 'developer_offer']
        filter = f"cityarea_id={cityarea_id} AND developer_offer LIKE '%%元/平%%'"
        data = self.select_by_filter(city=kwargs.get('city'), field=field, filter=filter)
        return data

    def get_info_by_filter(self, *args, **kwargs):
        city = kwargs.get('city')
        pms = kwargs.get('pms')
        fields = ['complex_name','complex_id','cityarea_name','cityarea_id','cityarea2_name','cityarea2_id']
        field = ",".join(fields)
        page = pms.get('page')
        index = page.get('index', 1)
        size = page.get('size', 30)
        form = int((index - 1))*int(size)
        sort = pms.get('sort',{})
        sortfield = sort.get('field','utime')
        sorttypes = sort.get('type','desc')
        filters = pms.get("filter", {})
        filters_str = ''
        for i,v in filters.items():
            filters_str += "and %s='%s'" % (i,v)
        sql = f"SELECT {field} from {self.table} where 1=1 {filters_str} ORDER BY {sortfield} {sorttypes} LIMIT {form}, {size}"
        where = '1=1 %s' % filters_str
        count = self.select_count(city=city, filter=where)
        data = self.exe_s_sqls(city=city, sql={'sql': sql})
        data.append(count)
        return data

    def get_complex_page(self, *args, **kwargs):
        index = kwargs.get('index', 2)
        size = kwargs.get('size')
        field = ",".join(kwargs.get('field', ['*']))
        if not field:
            field = '*'
        filter = kwargs.get('filter', {})
        complex_name = filter.get('complex_name', {})
        complex_names = '%%' + str(complex_name) + '%%'
        complex_id = filter.get('complex_id', {})
        cityarea_id = filter.get('cityarea_id', {})
        cityarea2_id = filter.get('cityarea2_id', {})
        ravg_price = filter.get('ravg_price', {})
        bavg_price = filter.get('bavg_price', {})

        sql = f"SELECT {field} FROM {self.table} WHERE 1=1"
        count_sql = f"SELECT COUNT(1) AS total FROM {self.table} WHERE 1=1"

        if complex_names != '%%{}%%':
            sql += " and complex_name LIKE '%s' " % (complex_names)
            count_sql += " and complex_name LIKE '%s' " % (complex_names)
        if complex_id:
            sql += " and complex_id in (%s) " % (complex_id)
            count_sql += " and complex_id in (%s) " % (complex_id)
        if cityarea_id:
            sql += " and cityarea_id = %s " % (cityarea_id)
            count_sql += " and cityarea_id = %s " % (cityarea_id)
        if cityarea2_id:
            sql += " and cityarea2_id = %s " % (cityarea2_id)
            count_sql += " and cityarea2_id = %s " % (cityarea2_id)
        if ravg_price:
            sql += " and developer_price >= %s " % (ravg_price)
            count_sql += " and developer_price >= %s " % (ravg_price)
        if bavg_price:
            sql += " and developer_price <= %s " % (bavg_price)
            count_sql += " and developer_price <= %s " % (bavg_price)

        sql += " and sale_weight >= 50 and source_id REGEXP '^2#|#2#|#2$|^2$' "
        count_sql += " and sale_weight >= 50 and source_id REGEXP '^2#|#2#|#2$|^2$' "
        if index >= 0 :
            sql += " limit %s,%s " % (index,size)
        data = self.exe_s_sqls(city=kwargs.get("city"), sql={'sql': sql})
        count = self.exe_s_sqls(city=kwargs.get("city"), sql={'sql': count_sql})
        if field == '*':
            if data:
                for i in range(len(data)):
                    data[i]['lng']=float(data[i].pop('lng'))
                    data[i]['lat']=float(data[i].pop('lat'))
                data.append(count)
        else:
            if data:
                data.append(count)
        return data

from decimal import Decimal
if __name__ == '__main__':
    v= Decimal('0E-10')

    print(type(v))
