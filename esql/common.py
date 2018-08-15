# -*- coding: utf-8 -*-


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
        self._quere = []

    def struct(self,conditions,query):
        still_or = True
        if len(conditions) == 1:
            self._quere.append('AND')
        else:
            self._quere.append(conditions[1])

        for condition in conditions:
            if isinstance(condition,str):
                self._quere.append(condition)
            elif isinstance(condition,dict):
                name,func,right,compare = self._unpack_col(condition)
                comb_k = self._pop()
                if compare == 'LIKE':
                    right = right.replace('%', '*')
                    subquery = {'wildcard':{name:right}}
                elif compare == '=':
                    subquery = {'term': {name: right}}
                elif compare in ('<>','!='):
                    comb_k = 'NOT'
                    subquery = {'term': {name: right}}
                else:
                    comp = Structure.COMP[compare]
                    subquery = {'range':{name:{comp:right}}}
                comb_v = Structure.COMB[comb_k]

                temp = dict(**query)
                query.clear()
                if temp:
                    query[comb_v] = [{'bool': temp}, subquery]
                else:
                    query[comb_v] = [subquery]
            else: # list need recurse
                comb_k = self._pop()
                comb_v = Structure.COMB[comb_k]
                recurse = {}
                temp = dict(**query)
                query.clear()
                print(comb_v)
                if temp:
                    query[comb_v] = [{'bool': temp}, {'bool':recurse}]
                else:
                    query[comb_v] = [{'bool': recurse}]
                self.struct(condition,recurse)

    def _unpack_col(self,col):
        return (col['left']['name'],
                col['left']['func'],
                col['right'],
                col['compare'])

    def _pop(self):
        return self._quere.pop()