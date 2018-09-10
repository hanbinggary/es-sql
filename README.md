# esql-python

Elasticsearch SQL parser

## HOW TO USE

> git clone https://github.com/yasinasama/esql-python.git

> cd esql-python

> python setup install


## demo

````
from esql.client import ESQL

sql = 'select * from blog;'
host_port = '127.0.0.1:9200'

esql = ESQL(host_port)
print(esql.execute(sql))

# If you just want to get the DSL sended
# query will not be executed actually
print(esql.execute(sql,debug=True))
````

## SQL syntax supported
````
SELECT columns FROM index
WHERE conditions
GROUP BY column
HAVING conditions
ORDER BY column
LIMIT number
````

