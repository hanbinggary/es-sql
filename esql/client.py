# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch

from .grammar import parse_handle
from .builder import SelectBuilder
from .analyser import Analyser

class ESQL(object):

	def __init__(self,host):
		self.es = Elasticsearch(host)

	def _init_select_dsl(self,column,table,where,group,having,order,limit):
		dsl = SelectBuilder(column,table,
							where,group,
							having,order,limit).dsl
		return dsl

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

			dsl_body = SelectBuilder(distinct,column, table,where,
									 group, having, order, limit).dsl
			response = self.es.search(index=table,body=dsl_body)
			analyser = Analyser(response,group,column,distinct)
			analyser.analyse()
			return analyser.result
