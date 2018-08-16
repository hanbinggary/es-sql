# -*- coding: utf-8 -*-

from itertools import groupby

from .model import Model

class Structure(object):
    COMP = {
        '>': 'gt',
        '>=': 'gte',
        '<': 'lt',
        '<=': 'lte'
    }

    COMB = {
        'AND':'must',
        'OR':'should',
        'NOT':'must_not',
    }

    def __init__(self):
        self._model = Model()

    def struct(self,conditions,result):
        if isinstance(conditions, list) and len(conditions) == 1:
            conditions = conditions[0]
        if 'OR' in conditions or 'AND' in conditions:
            if 'OR' in conditions:
                comb_k = 'OR'
            else:
                comb_k = 'AND'
            comb_v = Structure.COMB[comb_k]
            subconds = self._split_list(conditions,comb_k)
            for subcond in subconds:
                temp = self._model.bool_query
                result['bool'][comb_v].append(self.struct(subcond,temp))
        else:
            subquery, comb = self._subquery(conditions, 'must')
            result['bool'][comb].append(subquery)
        return result

    def _split_list(self,source,wd):
        return [list(g) for k, g in groupby(source, lambda x: x == wd) if not k]

    def _subquery(self,cond,comb):
        name, func, right, compare = cond['left']['name'],\
                                     cond['left']['func'],\
                                     cond['right'],\
                                     cond['compare']

        if compare == 'LIKE':
            right = right.replace('%', '*')
            subquery = {'wildcard':{name:right}}
        elif compare == '=':
            subquery = {'term': {name: right}}
        elif compare in ('<>','!='):
            comb = 'must_not'
            subquery = {'term': {name: right}}
        else:
            comp = Structure.COMP[compare]
            subquery = {'range':{name:{comp:right}}}
        return subquery,comb
