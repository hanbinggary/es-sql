# -*- coding: utf-8 -*-

from .grammar import parse_handle
from .builder import SelectBuilder

class ESQL(object):

	def __init__(self,sql):
		self._sql = sql
		self._parse = parse_handle(self._sql)

	def _init_dsl(self):
		self._dtype = self._parse['type']

		if self._dtype == 'SELECT':
			column = self._parse['column']
			table = self._parse['table']
			where = self._parse['where']
			group = self._parse['group']
			having = self._parse['having']
			order = self._parse['order']
			limit = self._parse['limit']
			self._dsl = SelectBuilder(column,table,
									  where,group,
									  having,order,limit).dsl