# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import bulk

from esql.exceptions import CreateException,TableExistsException
from .grammar import parse_handle
from .builder import SelectBuilder,DeleteBuilder,CreateBuilder,UpdateBuilder
from .analyser import Analyser


class Select:
	def __init__(self,es,parse):
		c = parse['column']
		if isinstance(c, dict):
			self.distinct = True
			self.column = c['distinct']
		else:
			self.distinct = False
			self.column = c
		self.table = parse['table']
		self.where = parse['where']
		self.group = parse['group']
		self.having = parse['having']
		self.order = parse['order']
		self.limit = parse['limit']

		self._es = es

	def execute(self,debug):
		select = SelectBuilder(self.distinct, self.column,
							   self.table, self.where,
							   self.group, self.having,
							   self.order, self.limit)
		dsl_body = select.dsl
		if debug:
			return dsl_body
		response = self._es.search(index=self.table, body=dsl_body)
		analyser = Analyser(response, self.group, self.column, self.distinct)
		analyser.analyse()
		return analyser.result

class Insert:
	def __init__(self, es, parse):
		self.table = parse['table']
		self.column = parse['column']
		self.values = parse['values']

		self._es = es

	def execute(self, debug):
		try:
			_index, _type = self.table.split('.')
		except ValueError:
			raise CreateException('table error!')

		m = {'_index':_index,'_type':_type}

		def gendata(m):
			for value in self.values:
				source = dict(zip(self.column,value),**m)
				yield source

		response = bulk(self._es,gendata(m))
		print(response)


class Update:
	def __init__(self,es, parse):
		self.table = parse['table']
		self.column = parse['column']
		self.where = parse['where']

		self._es = es

	def execute(self,debug):
		update = UpdateBuilder(self.column,self.where)

		dsl_body = update.dsl
		if debug:
			return dsl_body
		response = self._es.update_by_query(index=self.table, body=dsl_body)
		return response

class Delete:
	def __init__(self, es, parse):
		self.table = parse['table']
		self.where = parse['where']

		self._es = es

	def execute(self, debug):
		delete = DeleteBuilder(self.table, self.where)
		dsl_body = delete.dsl
		if debug:
			return dsl_body
		response = self._es.delete_by_query(index=self.table, body=dsl_body)
		print(response)


class Create:
	def __init__(self, es, parse):
		self.table = parse['table']
		self.column = parse['column']

		self._es = es

	def execute(self, debug):
		try:
			_index, _type = self.table.split('.')
		except ValueError:
			raise CreateException('table error!')

		create = CreateBuilder(_type, self.column)
		dsl_body = create.dsl
		if debug:
			return dsl_body

		if not IndicesClient(self._es).exists(_index):
			response = IndicesClient(self._es).create(index=_index, body=dsl_body)
			print(response)

class Drop:
	def __init__(self, es, parse):
		self.table = parse['table']

		self._es = es

	def execute(self,debug):
		if IndicesClient(self._es).exists(self.table):
			response = IndicesClient(self._es).delete(index=self.table)
			print(response)


class Desc:
	def __init__(self, es, parse):
		self.table = parse['table']

		self._es = es

	def execute(self,debug):
		if IndicesClient(self._es).exists(self.table):
			response = IndicesClient(self._es).get(index=self.table)

			result = []
			mappings = response[self.table]['mappings']
			for tp in mappings.keys():
				properties = mappings[tp]['properties']
				for field in properties.keys():
					pp = dict()
					pp['_type'] = tp
					pp['field'] = field
					pp['type'] = properties[field]['type']
					result.append(pp)
			return result


ACTIONS = {
	'SELECT':Select,
	'INSERT':Insert,
	'UPDATE':Update,
	'DELETE':Delete,
	'CREATE':Create,
	'DROP'	:Drop,
	'DESC'	:Desc
}

class ESQL:

	def __init__(self,host):
		self._es = Elasticsearch(host)

	def execute(self,sql,debug=False):
		parse = parse_handle(sql)
		dtype = parse['type']

		action = ACTIONS.get(dtype)
		result = action(self._es,parse).execute(debug)
		return result



