# -*- coding: utf-8 -*-

from esql.client import ESQL

def test_analyser():
    # sql = "select count(*),subject,srcip,dstip,max(srcport),min(dstport) from formattedlog where 'count' >= 112 and subject like '*HTTP_whisker_HEAD_*' and endtime>=1528019609000 and endtime<=1530611609000 group by subject,srcip,dstip,dstmac,dstport having COUNT(subject)=6862 or count(subject)=6801 and count(subject)=6862 and sum(srcport)<10 order by subject limit 1000;"
    sql = 'select * from evil_ip2 where info_value in ("124.72.192.81","49.79.54.3") and info_value="49.79.54.3";'
    # sql = "select count(*) from formattedlog where subject like 'UDP*' or subject like 'ICMP*';"
    host = 'ip:port'
    e = ESQL(host)
    print(e.execute(sql))

if __name__=='__main__':
    test_analyser()