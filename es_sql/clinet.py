from elasticsearch import Elasticsearch

from sqlparser import parse_handle
from es_sql.request import SQLCLASS


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

        return SQLCLASS[parsed['method']](self.es, parsed)


c = Client(host='10.68.120.106', port=9204)
# c.execute('select city,count(*) from test1es.base group by city having count(*)>1')

# c.execute('select * from test1es.base where city!="杭州" and city!="兰州"')
# c.execute('select _id,_type from test1es.base')
# c.execute('select count1 from test1es where count1 between 2 and 10')
c.execute('select count1 from test1es where count1 >=2')
# c.execute('select count(*) from test1es.base')
# c.execute('select count(distinct(city)) from test1es.base')
# c.execute('select * from test1es.base where city="上" and count1=1')
# c.execute('select * from test1es.base where city="杭州" and (city= "兰州" or count2="5")')
# c.execute('select count(city) from test1es.base group by city, count1, count2')
# c.execute('select count(city) from test1es.base group by city')
# c.execute('select city,count1,count(*) from test1es.base group by city,count1 having count(city)>0')
# c.execute('select sum(count2) from test1es where city="杭州"')
# c.execute('select * from test1es order by count1')




