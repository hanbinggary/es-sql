from elasticsearch import Elasticsearch

from es_sqlparser import parse_handle, do_test
from es_sql.request import SQLCLASS

do_test()

class Client:

    def __init__(self, **kwargs):
        self.es = Elasticsearch(**kwargs)

    def sql_format(self, sql):
        if not sql.endswith(';'):
            sql = sql + ';'
        return sql

    def parse(self, sql):
        return parse_handle(sql)

    def execute(self, sql):
        sql = self.sql_format(sql)
        parsed = self.parse(sql)
        print(parsed)

        method = parsed['method']

        return SQLCLASS[method](self.es, parsed)


c = Client(host='10.68.120.106', port=9204)

# print(c.execute('select _index,_type,_id,* from test1es.base'))
# print(c.execute('select * from test1es.base'))
# print(c.execute('select count(*) from test1es.base'))
# print(c.execute('select count(distinct(city)) from test1es.base'))
# print(c.execute('select * from test1es.base where city!="杭州" and city!="兰州"'))
# print(c.execute('select count1 from test1es where count1 between 2 and 10'))
# print(c.execute('select city,count(*) from test1es.base group by city having count(*)>1'))
# print(c.execute('select * from test1es limit 0,4'))
# print(c.execute('select * from test1es.base where city="杭州" or (city= "兰州" or count2="5") order by city'))
# print(c.execute('select count1,count2,count(city) from test1es.base group by city, count1, count2'))
# print(c.execute('select count(city) from test1es.base group by city'))
# print(c.execute('select city,count1,count(*) from test1es.base group by city,count1 having count(city)>0'))
# print(c.execute('select sum(count2) from test1es where city="杭州"'))
# print(c.execute('select * from test1es order by count1'))

# for i in c.execute('scan _index,* from test1es'):
#     print(i)
# for i in c.execute('scan * from test1es limit 4'):
# #     print(i)
# for i in c.execute('scan city from test1es limit 4'):
#     print(i)
# for i in c.execute('scan * from test1es where city=兰州'):
#     print(i)
# for i in c.execute('scan * from test1es where city=杭州 order by count2 desc'):
#     print(i)

c.execute('create table test5es(user keyword {analyzer=english},addr keyword/text) with 5,1')



