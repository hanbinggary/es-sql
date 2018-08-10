# -*- coding: utf-8 -*-

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

        self._dsl = {'query': {}}

    def b_where(self):
        if len(self._where) > 0:
            bools = []
            conds = []
            for i, v in enumerate(self._where):
                if i % 2 != 0:
                    bools.append(v)
                else:
                    conds.append(v)

    def b_group(self):
        pass

    def b_having(self):
        pass

    def b_order(self):
        pass

    def b_limit(self):
        pass


    def build(self):
        pass