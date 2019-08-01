from es_sql2 import Client
from es_sqlparser import do_test

# do_test()
c = Client('10.68.120.106:9204')

## create table
# 创建索引不带doc_type时默认为people.base
# keyword/text 默认会创建 user.raw 表示查询整词
# print(c.execute('create table people(user keyword/text, age integer,addr text,createtime date,weight double) with 1,1'))


### insert
# print(c.execute('insert into people(_id,user,age,addr,createtime,weight) values(3,"王五",29,"中国杭州",1564627410924,111.50),(4,"赵六",37,"中国宁波",1564611701824,109.77)'))


### select
# print(c.execute('select _index,_type,_id,* from people'))
# print(c.execute('select * from people.base'))
# print(c.execute('select user,age from people.base'))
# print(c.execute('select count(*) from people'))
# print(c.execute('select count(distinct(user.raw)) from people'))

# print(c.execute('select * from people where age=43'))
# print(c.execute('select * from people where user.raw="张三"'))
# print(c.execute('select * from people where age=43 and user.raw="王天"'))
# print(c.execute('select age from people where age>=32'))
# print(c.execute('select * from people where user.raw like "张*"'))
# print(c.execute('select * from people where user.raw in (张三,李四)'))
# print(c.execute('select * from people where user.raw not in (张三,李四)'))
# print(c.execute('select age from people where age between 20 and 40'))
# print(c.execute('select * from people where age < 26 or (age<40 and user.raw="王天")'))

# print(c.execute('select age,count(*) from people group by age'))
# print(c.execute('select user.raw,age,count(*) from people group by age,user.raw'))
# print(c.execute('select age,count(*) from people group by age having count(*)=1'))
# print(c.execute('select age,count(*) from people group by age having count(*)>1'))

# print(c.execute('select * from people limit 1'))
# print(c.execute('select * from people limit 1,1'))
# print(c.execute('select age from people order by age'))
# print(c.execute('select age from people order by age desc'))
# print(c.execute('select sum(age) from people'))
# print(c.execute('select count(age) from people'))
# print(c.execute('select * from people where id=1'))

### scan
# for i in c.execute('scan _index,* from people'):
#     print(i)
# for i in c.execute('scan * from people limit 4'):
#     print(i)
# for i in c.execute('scan * from people where age<22'):
#     print(i)
# for i in c.execute('scan * from people order by age desc'):
#     print(i)


### update
# print(c.execute('update people set user="ll",addr="广州" where id="1"'))

### delete
# print(c.execute('delete from people where id="1"'))

### drop table
# print(c.execute('drop table people'))

### desc
# print(c.execute('desc people'))

### show
# print(c.execute('show tables'))
# print(c.execute('show tables like "*people"'))