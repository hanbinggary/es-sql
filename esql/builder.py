# -*- coding: utf-8 -*-

from .common import Structure

class BuildDSL(object):
    def __init__(self):
        self.dsl = {}


class BuildSelect(object):
    def __init__(self,
                 dtype='SELECT', column=None, table=None, where=None,
                 group=None, having=None, order=None, limit=None):

        column = [] if column is None else column
        table = [] if table is None else table
        where = [] if where is None else where
        group = [] if group is None else group
        having = [] if having is None else having
        order = [] if order is None else order
        limit = [] if limit is None else limit

        self._type = dtype
        self._column = column
        self._table = table
        self._where = where
        self._group = group
        self._having = having
        self._order = order
        self._limit = limit

        self._dsl = {}

    def _b_where(self):
        _struct = Structure()
        _bool = {}
        _struct.struct(self._where,_bool)
        self._dsl['query'] = {'bool':_bool}

    def _b_group(self):
        pass

    def _b_having(self):
        pass


    def _b_order(self):
        if len(self._order) > 0:
            _sorts = []
            for sort in self._order:
                k,v = sort['name'],sort['type'].lower()
                _sorts.append({k:v})
            self._dsl['sort'] = _sorts

    def _b_limit(self):
        if len(self._limit) > 0:
            _from, _size = self._limit
            self._dsl['from'] = _from
            self._dsl['size'] = _size


    def build(self):
        self._b_where()
        self._b_group()
        self._b_having()
        self._b_order()
        self._b_limit()