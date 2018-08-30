# -*- coding: utf-8 -*-

import json

from .common import Structure

class Builder(object):
    def __init__(self):
        self.dsl = {}


class SelectBuilder(object):
    def __init__(self,column, table, where,
                 group, having, order, limit):
        if isinstance(column,dict):
            self._distinct = True
            self._column = column['distinct']
        else:
            self._distinct = False
            self._column = column
        self._table = table
        self._where = where
        self._group = group
        self._having = having
        self._order = order
        self._limit = limit

        self._dsl = {}
        self._structure = Structure()

        self._show_columns = []

    def _b_column(self):
        self._show_columns = self._structure.struct_column(self._column)
        self._dsl['_source'] = self._show_columns

    def _b_where(self):
        if len(self._where) > 0:
            _bool=self._structure.struct_where(self._where)
            self._dsl['query'] = _bool

    def _b_group(self):
        aggs = {}
        group = self._group[:]
        if self._distinct:
            group = self._show_columns[:]
        if len(group) > 0:
            self._structure.struct_group(group,self._having,aggs)
        else:
            self._structure.struct_func_column(aggs)
        if len(aggs) > 0:
            self._dsl['aggs'] = aggs

    def _b_order(self):
        if len(self._order) > 0:
            _sorts = []
            for sort in self._order:
                k,v = sort['name'],sort['type']
                _sorts.append({k:v})
            self._dsl['sort'] = _sorts

    def _b_limit(self):
        if len(self._limit) > 0:
            _from, _size = self._limit
            self._dsl['from'] = _from
            self._dsl['size'] = _size

    def build(self):
        self._b_column()
        self._b_where()
        self._b_group()
        self._b_order()
        self._b_limit()

    @property
    def dsl(self):
        self.build()
        print(json.dumps(self._dsl))
        return self._dsl



