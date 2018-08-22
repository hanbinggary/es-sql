# -*- coding: utf-8 -*-

import json

from esql import grammar
from esql.builder import SelectBuilder


def test_builder():
    sql = "select count(subject),subject,srcip,dstip,max(srcport),min(dstport) from formattedlog where 'count' >= 112 and subject like '*HTTP_whisker_HEAD_*' and endtime>=1528019609000 and endtime<=1530611609000 group by subject,srcip,dstip,dstmac,dstport having COUNT(subject)=6862 or count(subject)=6801 and count(subject)=6862 and sum(srcport)<10 limit 1000;"
    # sql = 'select * from ipisevilip_lable where (info_time = 1533784175000 or info_time=1533797433000) and (info_time = 1533784175000 or info_time=1533797433000) or info_time like "15337974%" ;'
    # sql = 'select * from evil_ip1 where info_value = "103.216.216.145";'
    build_list = grammar.parse_handle(sql)

    print(build_list)

    column = build_list['column']
    table = build_list['table']
    where = build_list['where']
    group = build_list['group']
    having = build_list['having']
    order = build_list['order']
    limit = build_list['limit']

    builder = SelectBuilder(column,table,where,group,having,order,limit)

    builder.build()

    print(json.dumps(builder._dsl))
    # print(builder._structure.func_columns)
    # print(builder._structure._show_columns)




if __name__=='__main__':
    test_builder()