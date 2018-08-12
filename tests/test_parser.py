# -*- coding: utf-8 -*-

from esql import grammar


def test_parser():
    # sql1 = "select * from users;"
    # sql2 = "select username,age from users;"
    # sql3 = "select username,age from users where username='cj' and age=22;"
    # sql4 = "select username,age from users where username='cj' and age=22 group by name;"
    # sql5 = "select username,age from users where username='cj' and age=22 group by name order by age desc;"
    sql6 = "select username,age from users where username like '%cj%' and age>'cj' or  a='tt' and v=1   group by name order by age desc limit 0,10;"

    # print(grammar.parse_handle(sql1))
    # print(grammar.parse_handle(sql2))
    # print(grammar.parse_handle(sql3))
    # print(grammar.parse_handle(sql4))
    # print(grammar.parse_handle(sql5))
    print(grammar.parse_handle(sql6))


if __name__=='__main__':
    test_parser()