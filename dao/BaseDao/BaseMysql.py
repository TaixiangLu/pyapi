
# @Time    : 18-1-13 上午10:49
# @Author  : jianguo@zhugefang.com
# @Desc    :
from dao.Base import Base
from databases.dbfactory.dbfactory import dbfactory
import logging


class BaseMysql(Base):
    def __init__(self, *args, **kwargs):
        self.model = kwargs.get("model")
        self.conn = dbfactory.create_db(
            conf_name=kwargs.get("conf_name"),
            db_type="db_mysql")
        super().__init__(model=self.model)

    def exe_s_sql(self, *args, **kwargs):
        """
            @Author  : jianguo@zhugefang.com
            @Time    : 18-3-14 上午10:49
            @Desc    : 执行原生语句SELECT语句 返回单条数据
            :param  city: 城市缩写
            :param  sql:  语句
            :param  link_type: 指定连接实例（默认default配置）
            :return: 字典类型

        """
        city = kwargs.get("city")
        link_type = kwargs.get('link_type', 'default')
        sql = kwargs.get("sql").get("sql")
        pms = kwargs.get("sql").get("pms")
        with self.conn.get_conn(link_type=link_type, city=city) as cursor:
            cursor.execute(sql, pms)
            res = cursor.fetchone() or {}
            return res

    def exe_s_sqls(self, *args, **kwargs):
        """
           @Author  : jianguo@zhugefang.com
           @Time    : 18-3-14 上午10:49
           @Desc    : 执行原生语句SELECT语句 返回多条数据
           :param  city: 城市缩写
           :param  link_type: 指定连接实例（默认default配置）
           :return: 字典类型

        """
        city = kwargs.get("city")
        link_type = kwargs.get('link_type', 'default')
        sql = kwargs.get("sql").get("sql")
        pms = kwargs.get("sql").get("pms", ())
        with self.conn.get_conn(city=city, link_type=link_type) as cursor:
            cursor.execute(sql, pms)
            res = cursor.fetchall()
            return res

    def exe_i_sql(self, *args, **kwargs):
        """
           @Author  : jianguo@zhugefang.com
           @Time    : 18-3-14 上午10:49
           @Desc    : 执行原生语句INSERT语句 返回单条数据ID
           :param  city: 城市缩写
           :param  link_type: 指定连接实例（默认default配置）
           :return: 字典类型

        """
        city = kwargs.get("city")
        sql = kwargs.get("sql").get("sql")
        pms = kwargs.get("sql").get("pms", ())
        with self.conn.get_conn(city=city) as cursor:
            cursor.execute(sql, pms)
            res = cursor.lastrowid
            return res

    def exe_i_sqls(self, *args, **kwargs):
        """
           @Author  : jianguo@zhugefang.com
           @Time    : 18-3-14 上午10:49
           @Desc    : 执行原生语句SELECT语句 返回单条数据
           :param  city: 城市缩写
           :param  link_type: 指定连接实例（默认default配置）
           :return: 字典类型

        """
        city = kwargs.get("city")
        sql = kwargs.get("sql").get("sql")
        pms = kwargs.get("sql").get("pms", ())
        with self.conn.get_conn(city=city) as cursor:
            res = cursor.executemany(sql, pms)
            return res

    def exe_t_i_sqls(self, *args, **kwargs):
        """
        执行事务语句
        :param args:   [{'sql':sql,'pms':pms}，{'sql':sql,'pms':pms}]
        :param kwargs:
        :return:
        """
        city = kwargs.get("city")
        datas = kwargs.get("datas")
        with self.conn.get_conn(city=city) as cursor:
            for data in datas:
                sql = data.get("sql")
                pms = data.get("pms", ())
                res = cursor.execute(sql, pms)
            return True

    def exe_u_sql(self, *args, **kwargs):
        """
           @Author  : jianguo@zhugefang.com
           @Time    : 18-3-14 上午10:49
           @Desc    : 执行原生语句SELECT语句 返回单条数据
           :param  city: 城市缩写
           :param  link_type: 指定连接实例（默认default配置）
           :return: 字典类型

        """
        city = kwargs.get("city")
        sql = kwargs.get("sql").get("sql")
        pms = kwargs.get("sql").get("pms", ())
        with self.conn.get_conn(city=city) as cursor:
            res = cursor.execute(sql, pms)
            return res

    def exe_d_sql(self, *args, **kwargs):
        """
           @Author  : jianguo@zhugefang.com
           @Time    : 18-3-14 上午10:49
           @Desc    : 执行原生语句SELECT语句 返回单条数据
           :param  city: 城市缩写
           :param  link_type: 指定连接实例（默认default配置）
           :return: 字典类型

        """
        city = kwargs.get("city")
        link_type = kwargs.get('link_type', 'default')
        filter = kwargs.get('filter')
        if not filter:
            logging.error('delete is not filter')
            return False
        sql = f"DELETE FROM {self.table} WHERE {filter}"
        try:
            with self.conn.get_conn(city=city, link_type=link_type) as cursor:
                cursor.execute(sql)
                return True
        except Exception as e:
            logging.error(e)
            return False

    """
    弃用弃用弃用弃用弃用弃用弃用弃用弃用
    """

    def select_by_page(self, *args, **kwargs):
        """
            @Author  : jianguo@zhugefang.com
            @Time    : 18-3-14 上午10:49
            @Desc    : 执行查询条件分页语句
            :param  city: 城市缩写
            :param  index: 开始的条数
            :param  size: 每次显示的条数
            :param  filter: where条件 ‘1=1’
            :param  link_type: 指定连接实例（默认default配置）
            :return: 字典类型
        """
        city = kwargs.get("city")
        link_type = kwargs.get('link_type', 'default')
        index = kwargs.get('index', 0)
        size = kwargs.get('size', 10)
        filter = kwargs.get('filter')
        field = ",".join(kwargs.get('field', ['*']))
        sql_select = f"SELECT {field} FROM {self.table} LIMIT {index}, {size}"
        sql = f"SELECT {field} FROM {self.table} WHERE {filter} LIMIT {index}, {size}" if filter else sql_select
        data = self.exe_s_sqls(
            link_type=link_type, city=city, sql={
                'sql': sql})
        return data

    def select_count(self, *args, **kwargs):
        """
            @Author  : jianguo@zhugefang.com
            @Time    : 18-3-14 上午10:49
            @Desc    : 执行查询总数语句
            :param  city: 城市缩写
            :param  filter: where条件 ‘1=1’
            :param  link_type: 指定连接实例（默认default配置）
            :return: 字典类型
        """
        city = kwargs.get("city")
        link_type = kwargs.get('link_type', 'default')
        filter = kwargs.get('filter')
        sql_count = f"SELECT COUNT(1) AS total FROM {self.table}"
        sql = f"{sql_count} WHERE {filter}" if filter else f"{sql_count}"
        return self.exe_s_sql(link_type=link_type, city=city, sql={'sql': sql})

    """
    弃用弃用弃用弃用弃用弃用弃用弃用弃用
    """

    def select_by_filter(self, *args, **kwargs):
        """
            @Author  : jianguo@zhugefang.com
            @Time    : 18-3-14 上午10:49
            @Desc    : 执行查询条件语句（不分页直接显示全部）
            :param  city: 城市缩写
            :param  filter: where条件 ‘1=1’
            :param  link_type: 指定连接实例（默认default配置）
            :return: 字典类型
        """
        city = kwargs.get("city")
        link_type = kwargs.get('link_type', 'default')
        filter = kwargs.get('filter', '1=1')
        field = ",".join(kwargs.get('field', ['*']))
        sql = f"SELECT {field} FROM {self.table} WHERE {filter}"
        data = self.exe_s_sqls(
            link_type=link_type, city=city, sql={
                'sql': sql})
        return data

    def count_model(self, *args, **kwargs):
        """
           @Author  : jianguo@zhugefang.com
           @Time    : 18-3-14 上午10:49
           @Desc    : 执行查询总数语句
           :param  city: 城市缩写
           :param  filter: where条件 ‘1=1’
           :param  link_type: 指定连接实例（默认default配置）
           :return: 字典类型
        """
        city = kwargs.get("city")
        link_type = kwargs.get('link_type', 'default')
        filter = kwargs.get('filter')
        field = kwargs.get('field', 'COUNT(1)')
        sql_count = f"SELECT {field} AS total FROM {self.table}"
        sql = f"{sql_count} WHERE {filter}" if filter else f"{sql_count}"
        return self.exe_s_sql(link_type=link_type, city=city, sql={'sql': sql})

    def count_union_model(self, *args, **kwargs):
        """
           @Author  : jianguo@zhugefang.com
           @Time    : 18-3-14 上午10:49
           @Desc    : 执行查询总数语句
           :param  city: 城市缩写
           :param  filter: where条件 ‘1=1’
           :param  link_type: 指定连接实例（默认default配置）
           :return: 字典类型
        """
        city = kwargs.get("city")
        link_type = kwargs.get('link_type', 'default')
        filter = kwargs.get('filter')
        field = kwargs.get('field', 'COUNT(1)')
        sql_count = f"SELECT {field} AS total FROM {self.table}"
        sql = f"{sql_count} WHERE {filter}" if filter else f"{sql_count}"
        return self.exe_s_sqls(link_type=link_type, city=city, sql={'sql': sql})

    def select_model(self, *args, **kwargs):
        link_type = kwargs.get('link_type', 'default')
        city = kwargs.get("city")
        filter = kwargs.get('filter')
        field = kwargs.get('field')
        index = kwargs.get('index')
        size = kwargs.get('size')
        sql = self.model.get_select_sql(field=field, index=index, size=size, filter=filter)
        data = self.exe_s_sqls(link_type=link_type, sql=sql, city=city)
        return data

    def select_one(self, *args, **kwargs):
        link_type = kwargs.get('link_type', 'default')
        city = kwargs.get("city")
        filter = kwargs.get('filter')
        field = kwargs.get('field')
        index = kwargs.get('index')
        size = kwargs.get('size')
        sql = self.model.get_select_sql(field=field, index=index, size=size, filter=filter)
        data = self.exe_s_sql(link_type=link_type, sql=sql, city=city)
        return data

    def update_model(self, *args, **kwargs):
        link_type = kwargs.get('link_type', "default")
        city = kwargs.get("city")
        filter = kwargs.get('filter')
        data = kwargs.get("data")
        sql = self.model.get_update_sql(city=city, filter=filter, data=data)
        result = self.exe_u_sql(link_type=link_type, city=city, sql=sql)
        return result

    def insert_model(self, *args, **kwargs):
        link_type = kwargs.get('link_type', "default")
        city = kwargs.get("city")
        data = kwargs.get("data")
        sql = self.model.get_insert_sql(city=city, data=data)
        result = self.exe_i_sql(link_type=link_type, city=city, sql=sql)
        return result

    def inserts_model(self, *args, **kwargs):
        link_type = kwargs.get('link_type', "default")
        city = kwargs.get("city")
        data = kwargs.get("data")
        sql = self.model.get_insert_sqls(city=city, data=data)
        result = self.exe_i_sqls(city=city, sql=sql)
        return result

    def insert_update_model(self, *args, **kwargs):
        link_type = kwargs.get('link_type', "default")
        city = kwargs.get("city")
        data = kwargs.get("data")
        yes_up = kwargs.get("yes_up", [])
        adjust = kwargs.get("adjust", {})
        sql = self.model.get_insert_update_sqls(data=[data], yes_up=yes_up, adjust=adjust)
        result = self.exe_i_sqls(link_type=link_type, city=city, sql=sql)
        return result

    def delete_model(self, *args, **kwargs):
        link_type = kwargs.get('link_type')
        city = kwargs.get("city")
        filter = kwargs.get("filter")
        sql = self.model.get_delete_sql(city=city, filter=filter)
        result = self.exe_d_sql(link_type=link_type, city=city, sql=sql)
        return result

    """
    弃用弃用弃用弃用弃用弃用弃用弃用弃用
    """

    def insert_one(self, *args, **kwargs):
        """
            @Author  : jianguo@zhugefang.com
            @Time    : 18-3-14 上午10:49
            @Desc    : 执行单条插入语句
            :param  city: 城市缩写
            :param  data: 插入数据字典（键名为字段名字）
            :return:
        """
        city = kwargs.get("city")
        data = kwargs.get("data")
        m_data = self.check_dataType(datas=[data])[0]
        sql = f"INSERT INTO {self.table}({','.join(m_data.keys())}) " \
              f"VALUES ({','.join(['%s' for i in range(len(m_data.keys()))])})"
        pms = tuple(m_data.values())
        print("sql", sql)
        print("pms", pms)
        result = self.exe_i_sql(city=city, sql={'sql': sql, "pms": pms})
        return result
    """
    弃用弃用弃用弃用弃用弃用弃用弃用弃用
    """

    def get_insert_one_sql(self, *args, **kwargs):
        data = kwargs.get("data")
        if len(data) > 0:
            m_data = self.check_dataType(datas=[data])[0]
            keys = [f"`{key}`" for key in m_data.keys()]
            sql = f"INSERT INTO {self.table}({','.join(keys)}) " \
                  f"VALUES ({','.join(['%s' for i in range(len(m_data.keys()))])})"
            pms = tuple(m_data.values())
            return {"sql": sql, "pms": pms}
        return {'sql': {}, 'pms': ()}
    """
    弃用弃用弃用弃用弃用弃用弃用弃用弃用
    """

    def insert_batch(self, *args, **kwargs):
        """
            @Author  : jianguo@zhugefang.com
            @Time    : 18-3-14 上午10:49
            @Desc    : 执行批量插入语句
            :param  city: 城市缩写
            :param  data: 插入数据字典列表（列表的每一项是一个字典，键名为字段名字）
            :return:
        """

        city = kwargs.get("city")
        datas = kwargs.get("datas")
        if len(datas) > 0:
            datas = self.check_dataType(datas=datas)
            m_data = datas[0]
            sql = f"INSERT INTO {self.table}({','.join(m_data.keys())}) " \
                  f"VALUES ({','.join(['%s' for i in range(len(m_data.keys()))])})"
            pms = [tuple(data.values()) for data in datas]
            res = self.exe_i_sqls(city=city, sql={'sql': sql, 'pms': pms})
            return res

    """
    弃用弃用弃用弃用弃用弃用弃用弃用弃用
    """

    def insert_update_batch(self, *args, **kwargs):
        """
            @Author  : jianguo@zhugefang.com
           @Time    : 18-3-14 上午10:49
           @Desc    : 根据唯一索引判重是否该数据存在，如果存在则更新，不存在则插入
           :param  city: 城市缩写
           :param  yes_up: 需要指定的更新字段  类型 []
           :param  no_up:  指定不需要更新字段  类型 []
           :param  adjust: 指定的操作  {count=count+1} 指定count字段在原有值基础上计数
           :return: 字典类型

        """
        city = kwargs.get("city")
        datas = kwargs.get("datas")
        if len(datas) > 0:
            datas = self.check_dataType(datas=datas)
            keys = datas[0].keys()
            no_up = kwargs.get('no_up', [])
            yes_up = kwargs.get('yes_up', keys)
            adjust = kwargs.get('adjust', {})
            up_date = [f"`{key}`={adjust.get(key)}" if key in adjust else f'`{key}`=VALUES(`{key}`)'
                       for key in yes_up if key not in no_up]
            sql = f"INSERT INTO {self.table}({','.join(keys)}) " \
                  f"VALUES ({','.join(['%s' for i in range(len(keys))])}) " \
                  f"ON DUPLICATE KEY UPDATE {','.join(up_date)} "
            pms = [tuple(data.values()) for data in datas]
            res = self.exe_i_sqls(city=city, sql={'sql': sql, 'pms': pms})
            return res

    """
    弃用弃用弃用弃用弃用弃用弃用弃用弃用
    """

    def check_dataType(self, *args, **kwargs):
        """
            @Author  : jianguo@zhugefang.com
            @Time    : 18-3-14 上午10:49
            @Desc    : 根据model验证传入参数类型
            :param  datas: 数据字典列表
            :return:
        """
        datas = kwargs.get('datas')
        ok_datas = []
        for data in datas:
            m_data = {}
            for k, v in data.items():
                if k in self.model.fields and isinstance(
                        self.model.fields.get(k), type(v)):
                    m_data.setdefault(k, v)
            ok_datas.append(m_data)
        return ok_datas

    """
    弃用弃用弃用弃用弃用弃用弃用弃用弃用
    """

    def update(self, *args, **kwargs):
        """
            @Author  : jianguo@zhugefang.com
            @Time    : 18-3-14 上午10:49
            @Desc    : 执行更新语句
            :param  data: 数据字典
            :param  city: 城市缩写
            :param  filter: where条件 ‘1=1’
            :return:
        """
        city = kwargs.get("city")
        data = kwargs.get("data")
        filter = kwargs.get('filter')
        m_data = self.check_dataType(datas=[data])[0]
        str_list = []
        for k, v in m_data.items():
            str_list.append(f"`{k}`='{v}'")
        str = ','.join(str_list)
        sql = f"UPDATE {self.table} SET {str} WHERE {filter}"
        result = self.exe_u_sql(city=city, sql={'sql': sql})
        return result

    """
    弃用弃用弃用弃用弃用弃用弃用弃用弃用
    """

    def get_update_sql(self, *args, **kwargs):
        data = kwargs.get("data")
        filter = kwargs.get('filter')
        m_data = {}
        if len(data) > 0:
            for k, v in data.items():
                if isinstance(self.model.fields.get(k), type(v)):
                    m_data.setdefault(k, v)
            str_list = []
            for k, v in m_data.items():
                str_list.append("`" + k + "`" + "=" + "'%s'" % v)
            str = ','.join(str_list)
            sql = f"UPDATE {self.table} SET %s WHERE {filter}" % (str)
            return {'sql': sql, 'pms': ()}
        return {'sql': {}, 'pms': ()}

    """
    弃用弃用弃用弃用弃用弃用弃用弃用弃用
    """

    def delete_by_filter(self, *args, **kwargs):
        """
            @Author  : jianguo@zhugefang.com
            @Time    : 18-3-14 上午10:49
            @Desc    : 执行删除语句
            :param  city: 城市缩写
            :param  filter: where条件 ‘1=1’
            :return:
        """
        city = kwargs.get("city")
        link_type = kwargs.get('link_type', 'default')
        filter = kwargs.get('filter')
        if not filter:
            logging.error('delete is not filter')
            return False
        sql = f"DELETE FROM {self.table} WHERE {filter}"
        try:
            with self.conn.get_conn(city=city, link_type=link_type) as cursor:
                cursor.execute(sql)
                return True
        except Exception as e:
            logging.error(e)
            return False

    def get_delete_by_filter_sql(self, *args, **kwargs):
        city = kwargs.get("city")
        link_type = kwargs.get('link_type', 'default')
        filter = kwargs.get('filter')
        if not filter:
            logging.error('delete is not filter')
            return False
        sql = f"DELETE FROM {self.table} WHERE {filter}"
        return {"sql": sql, "pms": ()}

    def get_ids(self, *args, **kwargs):
        complex_id = kwargs.get('complex_id')
        sql = f"SELECT id FROM {self.table} WHERE complex_id='{complex_id}'"
        result = self.exe_s_sql(city=kwargs.get("city"), sql={'sql': sql})
        if result and len(result) > 0:
            return result.get("id")
        else:
            i_sql = f"INSERT INTO {self.table} (`complex_id`) VALUES ('{complex_id}')"
            i_data = self.exe_i_sql(
                city=kwargs.get("city"), sql={
                    'sql': i_sql})
            if i_data:
                return i_data


if __name__ == '__main__':
    # from apps.zhuge_newhouse.model.detail.Complex import Complex
    # bb = BaseMysql(conf_name='complex_rw', model=Complex)
    # count = bb.update(city='bj')
    # print(count)

    from apps.zhuge_brokerhouse.model.detail.sell.BrokerhouseSellSurvey import BrokerhouseSellSurvey
    bb = BaseMysql(conf_name='brokerhouse', model=BrokerhouseSellSurvey)
    # datas = [
    #     {
    #         "house_id":100000115,
    #         "title_img":"xxx",
    #         "company_id":20
    #
    #     },
    #     {
    #         "house_id":100000121,
    #         "title_img":"www",
    #         "company_id": 20
    #     }
    # ]
    # noup = ['house_id']
    # adjust = {'company_id': 'company_id+1'}
    # print(bb.insert_update_batch(city='bj',datas=datas,no_up=noup,adjust=adjust))
    # [{'sql': sql, 'pms': pms}，{'sql': sql, 'pms': pms}]

    # def exe_t_i_sqls(self, *args, **kwargs):
    #     """
    #     执行事务语句
    #     :param args:   [{'sql':sql,'pms':pms}，{'sql':sql,'pms':pms}]
    #     :param kwargs:
    #     :return:
    #     """
    #     city = kwargs.get("city")
    #     datas = kwargs.get("datas")
    #
    #     with self.conn.get_conn(city=city) as cursor:
    #         for data in datas:
    #             sql = data.get("sql").get("sql")
    #             pms = data.get("sql").get("pms", ())
    #             cursor.executemany(sql, pms)
    #         return True
    pms = [
        {
            'sql': {
                'sql': f"insert into brokerhouse_sell_id_copy(house_id,status) VALUES(%s,%s)", 'pms': [
                    (24164, 1), (7967, 1)]}}, {
            'sql': {
                'sql': f"insert into brokerhouse_survey_copy(house_id,title_img) VALUES(%s,%s)", 'pms': [
                    (3456, 'zzzzzzzzzz'), (976, '42zzc')]}}]
    print(bb.exe_t_i_sqls(city='bj', datas=pms))
