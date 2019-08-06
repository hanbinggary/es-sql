es-sql
============

Elasticsearch SQL

es_sql2 => es2.x
es_sql5 => es5.x
es_sql6 => es6.x

Installation
------------
.. code-block:: bash

    $ git clone https://github.com/yasinasama/es-sql.git
    $ cd es-sql
    $ python setup install


Getting Started
---------------
.. code-block:: pycon

    >>> from es_sql2 import Client
    >>> sql = 'select * from test1es;'
    >>> esql = Client('10.68.120.106:9204')
    >>> esql.execute(sql)

SQL syntax supported
---------------------

select
~~~~~~~~

.. code-block:: sql

    # 支持的语法

    # select查询（默认最多只能取1W条数据）
    # 查询语法默认只查询文档内字段，不包括（_index,_type,_id）这三个字段
    # 可通过显式指定以上字段进行查询（如果需要的话）
    # 查询的表可以是索引也可以是索引下的文档（通过 . 连接）
    select * from test1es
    select _index,_type,_id,* from test1es
    select _index,_type,_id,* from test1es.base

    # 直接count(*)可统计索引内文档数
    # count(distinct(city))可统计去重后的文档数
    select count(*) from test1es
    select count(distinct(city)) from test1es')

    # 支持多条件查询 and or 连接
    # 支持 >/>=/</<=/!=/=/like/in/not in/between/is/is not
    select * from test1es.base where city!="杭州" and city!="兰州"
    select count1 from test1es where count1 between 2 and 10

    # 支持聚合查询
    select city,count(*) from test1es.base group by city having count(*)>1'

    # 支持排序/limit
    select * from test1es order by count1
    select * from test1es limit 0,4


    # scan查询（通过es scroll取数据）
    # 支持条件查询/limit/排序
    # 注意：scan查询返回的是一个生成器，需要for循环迭代取数据
    # 比如：
    # for i in c.execute('scan * from test1es limit 4'):
    #   print(i)
    scan * from test1es where city=杭州 order by count2 desc
    scan * from test1es limit 4


    # insert插入
    insert into test56es.base2(user,age,addr) values("cj",14,"中国杭州"),("cj2",15,"中国上海")
    insert into test56es(_id,user,age,addr) values("awsaf","cj",14,"中国杭州"),("adfgasdf","cj2","SF","中国上海")


    # update更新
    # 只能根据id更新
    update test56es set user="hh",addr="北" where id="adfgasdqrf"


    # delete删除
    # 只能根据id删除
    delete from test56es where id="adfgasdf"


    # create建索引
    # 建索引的规则：
    # 字段名 字段类型 分词类型
    # 字段类型包括 keyword(不分词) text(分词) long/bool/integer等 还有一个 keyword/text 这个类型表示该字段既可以分词查询又可以整词查询 通过 字段名.raw 表示整词查询
    # with 后面 表示  分片数和副本数
    create table test56es(user text {analyzer=english}, age long,addr keyword/text) with 1,1


    # drop索引
    # 逗号分隔可drop多个索引
    drop table test59es
    drop table test57es,test58es


    # desc索引
    desc test1es


    # show
    show tables
    show tables like "%test%"