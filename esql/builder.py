# -*- coding: utf-8 -*-

import json

from .common import Structure

class Builder(object):
    def __init__(self):
        self.dsl = {}


class SelectBuilder(object):
    def __init__(self,distinct=None, column=None, table=None, where=None,
                 group=None, having=None, order=None, limit=None):
        self._distinct = distinct or 'N'
        self._column = column or []
        self._table = table or []
        self._where = where or []
        self._group = group or []
        self._having = having or []
        self._order = order or []
        self._limit = limit or []

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
        if self._distinct == 'Y':
            group = self._show_columns[:]
        if len(self._group) > 0:
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



