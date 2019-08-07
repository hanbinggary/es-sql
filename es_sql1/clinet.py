from .elasticsearch1 import Elasticsearch

from es_sqlparser import parse_handle
from .request import SQLCLASS


class Client:

    def __init__(self, hosts, **kwargs):
        self.es = Elasticsearch(hosts, **kwargs)

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





