from es_sql2 import Client


c = Client(host='10.68.120.106', port=9204)

### select
# print(c.execute('select _index,_type,_id,* from test1es'))
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
# print(c.execute('select * from test56es where addr.raw="中国上海"'))
# print(c.execute('select * from test56es'))

### scan
# for i in c.execute('scan _index,* from test5666es'):
#     print(i)
# for i in c.execute('scan * from test1es limit 4'):
# #     print(i)
# for i in c.execute('scan city from test1es limit 4'):
#     print(i)
# for i in c.execute('scan * from test1es where city=兰州'):
#     print(i)
# for i in c.execute('scan * from test1es where city=杭州 order by count2 desc'):
#     print(i)

### insert
# c.execute('insert into test56es.base2(user,age,addr) values("cj",14,"中国杭州"),("cj2",15,"中国上海")')
# print(c.execute('insert into test56es(_id,user,age,addr) values("awsaf","cj",14,"中国杭州"),("adfgasdf","cj2","SF","中国上海")'))

### update
# print(c.execute('update test56es set user="hh",addr="北" where id="adfgasdqrf"'))

### delete
# print(c.execute('delete from test56es where id="adfgasdf"'))

## create table
# print(c.execute('create table test56es(user text {analyzer=english}, age long,addr keyword/text) with 1,1'))

### drop table
# print(c.execute('drop table test59es'))
# print(c.execute('drop table test57es,test58es'))

### desc
# print(c.execute('desc test90es'))

### show
# print(c.execute('show tables'))
# print(c.execute('show tables like "%test%"'))