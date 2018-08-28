# -*- coding: utf-8 -*-
class Base(object):

   def get_select_sql(self, *args, **kwargs):
      filter = kwargs.get('filter') or {}
      field = kwargs.get('field') or []
      field = self.check_field(datas=field)
      index = kwargs.get("index") or 0 #mysql 索引默认从0开始
      size = kwargs.get("size") or 30
      sql_select = f"SELECT {field} FROM {self.__tablename__} LIMIT {index}, {size}"
      sql = f"SELECT {field} FROM {self.__tablename__} WHERE {filter} LIMIT {index}, {size}" if filter else sql_select
      return {"sql": sql, "pms": ()}


   def get_insert_sql(self, *args, **kwargs):
      data = kwargs.get('data', {})
      m_data = self.check_dataType(datas=[data])[0]
      sql = f"INSERT INTO {self.__tablename__}({','.join(m_data.keys())}) " \
            f"VALUES ({','.join(['%s' for i in range(len(m_data.keys()))])})"
      pms = tuple(m_data.values())
      return {"sql": sql, "pms": pms}

   def get_insert_sqls(self, *args, **kwargs):
      datas = kwargs.get('data', [])
      if len(datas) > 0:
         datas = self.check_dataType(datas=datas)
         m_data = datas[0]
         sql = f"INSERT INTO {self.__tablename__}({','.join(m_data.keys())}) " \
               f"VALUES ({','.join(['%s' for i in range(len(m_data.keys()))])})"
         pms = [tuple(data.values()) for data in datas]
         return {"sql": sql, "pms": pms}

   def get_insert_update_sqls (self, *args, **kwargs):
      datas = kwargs.get('data', [])
      if len(datas) > 0:
         datas = self.check_dataType(datas=datas)
         keys = list(datas[0].keys())
         no_up = kwargs.get('no_up', [])
         adjust = kwargs.get('adjust', {})
         yes_up = kwargs.get('yes_up', keys)+list(adjust.keys())
         up_date = [f"`{key}`={adjust.get(key)}" if key in adjust else f'`{key}`=VALUES(`{key}`)'
                    for key in yes_up if key not in no_up]
         sql = f"INSERT INTO {self.__tablename__}({','.join(keys)}) " \
               f"VALUES ({','.join(['%s' for i in range(len(keys))])}) " \
               f"ON DUPLICATE KEY UPDATE {','.join(up_date)}"
         pms = [tuple(data.values()) for data in datas]
         return {'sql': sql, 'pms': pms}

   def get_update_sql(self, *args, **kwargs):
      data = kwargs.get("data", {})
      filter = kwargs.get('filter')
      if len(data) > 0 and filter:
         m_data = self.check_dataType(datas=[data])[0]
         keys = list(m_data.keys())
         no_up = kwargs.get('no_up', [])
         adjust = kwargs.get('adjust', {})
         yes_up = kwargs.get('yes_up', keys)+list(adjust.keys())
         up_key = [f"`{key}`={adjust.get(key)}" if key in adjust else f"`{key}`=%s" for key in yes_up if key not in no_up]
         up_data = [m_data.get(key) for key in yes_up if (key not in no_up and key not in adjust)]
         pms =tuple(up_data)
         sql = f" UPDATE {self.__tablename__} SET {','.join(up_key)} WHERE {filter}"
         return {'sql': sql, 'pms': pms}


   def get_delete_sql(self, *args, **kwargs):
      filter = kwargs.get('filter')
      if filter:
         sql = f"DELETE {self.__tablename__} WHERE {filter}"
         return {"sql": sql, "pms": ()}

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
         for k, v in sorted(data.items()):
            if k in self.fields and isinstance(self.fields.get(k), type(v)):
               m_data.setdefault(k, v)
         ok_datas.append(m_data)
      return ok_datas



   def check_field(self, datas):
      fields = list(self.fields.keys())
      fields = [data for data in datas if data in fields] or fields
      return ",".join(fields)



