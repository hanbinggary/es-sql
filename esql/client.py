# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch

from .grammar import parse_handle
from .builder import SelectBuilder
from .analyser import Analyser

class ESQL(object):

	def __init__(self,host):
		self._es = Elasticsearch(host)
		self.dsl_body = ''

	def execute(self,sql):
		parse = parse_handle(sql)
		dtype = parse['type']
		if dtype == 'SELECT':
			distinct = parse['distinct']
			column = parse['column']
			table = parse['table']
			where = parse['where']
			group = parse['group']
			having = parse['having']
			order = parse['order']
			limit = parse['limit']

			self.dsl_body = SelectBuilder(distinct,column,table,where,group,having,order,limit).dsl
			response = self._es.search(index=table,body=self.dsl_body)
			analyser = Analyser(response,group,column,distinct)
			analyser.analyse()
			return analyser.result

	@property
	def dsl(self):
		return self.dsl_body


