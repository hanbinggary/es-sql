# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch

from .grammar import parse_handle
from .builder import SelectBuilder
from .analyser import Analyser

class ESQL(object):

	def __init__(self,host):
		self._es = Elasticsearch(host)

	def execute(self,sql,debug=False):
		parse = parse_handle(sql)
		dtype = parse['type']
		if dtype == 'SELECT':
			c = parse['column']
			if isinstance(c, dict):
				distinct = True
				column = c['distinct']
			else:
				distinct = False
				column = c
			table = parse['table']
			where = parse['where']
			group = parse['group']
			having = parse['having']
			order = parse['order']
			limit = parse['limit']

			select = SelectBuilder(distinct,column,table,where,group,having,order,limit)
			dsl_body = select.dsl
			if debug:
				return dsl_body
			response = self._es.search(index=table,body=dsl_body)
			analyser = Analyser(response,group,column,distinct)
			analyser.analyse()
			return analyser.result



