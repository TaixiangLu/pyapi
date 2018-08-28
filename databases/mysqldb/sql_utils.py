import unittest

class TestPython():
    def test_page(self):
        field = '*'
        table = 'complex'
        filter = '1=1'
        index = 1
        size = 10
        ob = 'complex_id'
        sql = f'SELECT {field} FROM {table} WHERE {filter} ORDER BY {ob}  LIMIT {index},{size}'

        print(sql)

    @staticmethod
    def test_insert(data_list):
        sql_list = []
        for data_dict in data_list:
            for table, data in data_dict.items():
                sql = f'INSERT INTO {table}'
                sql_values = ''
                params = []
                sql_dic = {}
                if data and isinstance(data, dict):
                    ks, ps, vs = [], [], []
                    for k, v in data.items():
                        ks.append(k)
                        ps.append('%s')
                        vs.append(v)
                    sql_values += '(' + ','.join(ks) + ')' + ' VALUES ' + '(' + ','.join(ps) + ')'

                    params = tuple(vs)
                elif data and isinstance(data, list):
                    for item in data:
                        ks, ps, vs = [], [], []
                        for k, v in item.items():
                            ks.append(k)
                            ps.append('%s')
                            vs.append(v)
                        if not sql_values:
                            sql_values = '(' + ','.join(ks) + ')' + ' VALUES ' + '(' + ','.join(ps) + ')'
                        params.append(tuple(vs))
                sql += sql_values
                if sql_values:
                    sql_dic.setdefault('sql', sql)
                    sql_dic.setdefault('params', params)
                    sql_list.append(sql_dic)
        return sql_list

    @staticmethod
    def get_insert_sql(model, data):


        pass
if __name__ == '__main__':
    data = [{"test": {'id': 2}}]
    sql = TestPython.test_insert(data_list=data)
    print(sql)