# esql-python

Elasticsearch SQL parser

## HOW TO USE

> git clone https://github.com/yasinasama/esql-python.git

> cd esql-python

> python setup install


## demo

```python
	from esql.client import ESQL

	sql = 'select * from blog;'
	host_port = '127.0.0.1:9200'

	esql = ESQL(host_port)
	print(esql.execute(sql))
```

