# -*- coding: utf-8 -*-

import json

from .exceptions import *
from .common import Structure

class Builder:
    def __init__(self):
        self.dsl = {}


class SelectBuilder:
    def __init__(self,distinct,column, table, where,
                 group, having, order, limit):

        self._distinct = distinct
        self._column = column
        self._table = table
        self._where = where
        self._group = group
        self._having = having
        self._order = order
        self._limit = limit

        self._dsl = {}
        self._structure = Structure()

    def _b_column(self):
        self._structure.struct_column(self._column)
        self._dsl['_source'] = self._structure.source

    def _b_where(self):
        if len(self._where) > 0:
            _bool=self._structure.struct_where(self._where)
            self._dsl['query'] = _bool

    def _b_group(self):
        aggs = {}
        group = self._group[:]
        if self._distinct:
            group = self._structure.source[:]
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


class DeleteBuilder:
    def __init__(self, table, where):
        self._table = table
        self._where = where

        self._dsl = {}
        self._structure = Structure()

    def _b_where(self):
        if len(self._where) > 0:
            _bool=self._structure.struct_where(self._where)
            self._dsl['query'] = _bool

    def build(self):
        self._b_where()

    @property
    def dsl(self):
        self.build()
        print(json.dumps(self._dsl))
        return self._dsl

class CreateBuilder:
    def __init__(self,table,column):
        self._table = table
        self._column = column

        self._dsl = {}

    def _b_column(self):
        try:
            _index,_type = self._table.split('.')
        except ValueError:
            raise CreateException('table error!')

        properties = {}
        for c in self._column:
            _colname = c['name']
            _coltype = c['type']
            properties[_colname] = {'type':_coltype}
        self._dsl['mapping'] = {_type:{'properties':properties}}

    def build(self):
        self._b_column()

    @property
    def dsl(self):
        self.build()
        print(json.dumps(self._dsl))
        return self._dsl

