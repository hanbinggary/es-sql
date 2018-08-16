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
        self._quere = []

    def struct(self,conditions,result):
        if isinstance(conditions, list) and len(conditions) == 1:
            conditions = conditions[0]
        if 'OR' in conditions:
            split_by_or = [list(g) for k, g in groupby(conditions, lambda x: x == 'OR') if not k]
            for item in split_by_or:
                temp = self._model.bool_query
                if isinstance(item,list) and len(item)==1:
                    item=item[0]
                # print(self.struct(item, temp))
                result['bool']['should'].append(self.struct(item,temp))
        elif 'AND' in conditions:
            split_by_and = [list(g) for k, g in groupby(conditions, lambda x: x == 'AND') if not k]
            for item in split_by_and:
                temp = self._model.bool_query
                if isinstance(item,list) and len(item)==1:
                    item=item[0]
                result['bool']['must'].append(self.struct(item, temp))
        else:
            subquery, comb = self._subquery(conditions, 'must')
            result['bool'][comb].append(subquery)
        return result


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


    def _pop(self):
        return self._quere.pop()