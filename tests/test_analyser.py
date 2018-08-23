# -*- coding: utf-8 -*-

from esql.client import ESQL

def test_analyser():
    # sql = "select count(*),subject,srcip,dstip,max(srcport),min(dstport) from formattedlog where 'count' >= 112 and subject like '*HTTP_whisker_HEAD_*' and endtime>=1528019609000 and endtime<=1530611609000 group by subject,srcip,dstip,dstmac,dstport having COUNT(subject)=6862 or count(subject)=6801 and count(subject)=6862 and sum(srcport)<10 order by subject limit 1000;"
    # sql = 'select distinct info_area from evil_ip1 limit 100;'
    sql = 'select distinct info_area from evil_ip1;'
    host = '10.68.120.106:9204'
    e = ESQL(host)
    print(e.execute(sql))

if __name__=='__main__':
    test_analyser()