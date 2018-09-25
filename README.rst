esql-python
==========

Elasticsearch SQL parser

Installation
------------
.. code-block:: bash

    $ git clone https://github.com/yasinasama/esql-python.git
    $ cd esql-python
    $ python setup install


Getting Started
---------------
.. code-block:: pycon

    >>> from esql.client import ESQL
    >>> sql = 'select * from blog;'
    >>> esql = ESQL('127.0.0.1:9200')
    >>> print(esql.execute(sql))

    # If you just want to get the DSL sended
    # query will not be executed actually
    >>> print(esql.execute(sql,debug=True))


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

