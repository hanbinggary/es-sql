esql-python
==========

Elasticsearch SQL parser

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
    >>> sql = 'select * from blog;'
    >>> esql = Client('10.68.120.106:9204')
    >>> print(esql.execute(sql))

SQL syntax supported
-------------------
.. code-block:: sql

    SELECT columns FROM index
    WHERE conditions
    GROUP BY column
    HAVING conditions
    ORDER BY column
    LIMIT number

    CREATE TABLE IF NOT EXISTS blog.blog(name text,age short);

