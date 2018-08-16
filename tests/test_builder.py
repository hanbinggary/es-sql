# -*- coding: utf-8 -*-

import json

from esql import grammar
from esql.builder import BuildSelect


def test_builder():
    # sql = "select username,age from users where username like '%cj%' and age>'cj' group by name having count(*)>1 order by age ,username desc limit 10;"
    # sql = 'select * from ipisevilip_lable where info_time = 12314  or info_from != "情报扩线";'
    sql = 'select * from ipisevilip_lable where (info_time = 1533784175000 or info_time=1533797433000) and (info_time = 1533784175000 or info_time=1533797433000) or info_time=1533797433000 ;'
    build_list = grammar.parse_handle(sql)

    print(build_list)

    column = build_list['column']
    table = build_list['table']
    where = build_list['where']
    group = build_list['group']
    having = build_list['having']
    order = build_list['order']
    limit = build_list['limit']

    builder = BuildSelect('',column,table,where,group,having,order,limit)

    builder.build()

    print(json.dumps(builder._dsl))



if __name__=='__main__':
    test_builder()