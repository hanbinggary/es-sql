# -*- coding: utf-8 -*-

from .common import Structure

class Builder(object):
    def __init__(self):
        self.dsl = {}


class SelectBuilder(object):
    def __init__(self,column=None, table=None, where=None,
                 group=None, having=None, order=None, limit=None):
        self._column = column or []
        self._table = table or []
        self._where = where or []
        self._group = group or []
        self._having = having or []
        self._order = order or []
        self._limit = limit or []

        self._dsl = {}
        self._structure = Structure()

    def _b_column(self):
        _source = self._structure.struct_column(self._column)
        self._dsl['_source'] = _source

    def _b_where(self):
        if len(self._where) > 0:
            _bool=self._structure.struct_where(self._where)
            self._dsl['query'] = _bool

    def _b_group(self):
        aggs = {}
        if len(self._group) > 0:
            self._structure.struct_group(self._group[:],self._having,aggs)
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
        return self._dsl