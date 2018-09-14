# -*- coding: utf-8 -*-

from .common import Structure

class Builder:
    def __init__(self):
        self._dsl = {}

    @property
    def dsl(self):
        return self._dsl


class SelectBuilder(Builder):
    def __init__(self,distinct,column, table, where,
                 group, having, order, limit):
        super(SelectBuilder,self).__init__()
        self._distinct = distinct
        self._column = column
        self._table = table
        self._where = where
        self._group = group
        self._having = having
        self._order = order
        self._limit = limit

        self._structure = Structure()

        self._build()

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

    def _build(self):
        self._b_column()
        self._b_where()
        self._b_group()
        self._b_order()
        self._b_limit()


class DeleteBuilder(Builder):
    def __init__(self, table, where):
        super(DeleteBuilder, self).__init__()
        self._table = table
        self._where = where

        self._structure = Structure()

        self._build()

    def _b_where(self):
        if len(self._where) > 0:
            _bool=self._structure.struct_where(self._where)
            self._dsl['query'] = _bool

    def _build(self):
        self._b_where()


class CreateBuilder(Builder):
    def __init__(self,type,column):
        super(CreateBuilder, self).__init__()
        self._type = type
        self._column = column

        self._build()

    def _b_column(self):
        properties = {}
        for c in self._column:
            _colname = c['name']
            _coltype = c['type']
            properties[_colname] = {'type':_coltype}
        self._dsl['mappings'] = {self._type:{'properties':properties}}

    def _build(self):
        self._b_column()


class UpdateBuilder(Builder):
    def __init__(self,column,where):
        super(UpdateBuilder, self).__init__()
        self.column = column
        self._where = where

        self._structure = Structure()

        self._build()

    def _b_update_columns(self):
        script = self._structure.struct_update_script(self.column)
        self._dsl['script'] = script

    def _b_where(self):
        if len(self._where) > 0:
            _bool=self._structure.struct_where(self._where)
            self._dsl['query'] = _bool

    def _build(self):
        self._b_update_columns()
        self._b_where()
