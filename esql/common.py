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
        'NOT':'must_not'
    }

    def __init__(self):
        self._quere = ['AND']

    def struct(self,conditions,query):
        conds = [cond for cond in conditions[::2]]
        combs = [comb for comb in conditions[1::2]]







        for condition in conditions:
            if isinstance(condition,str):
                self._quere.append(condition)
            if isinstance(condition,dict):
                name,func,right,compare = self._pack_col(condition)
                comb_k = self._pop()
                comb_v = Structure.COMB[comb_k]
                if compare == 'LIKE':
                    right = right.replace('%', '*')
                    subquery = {'wildcard':{name:right}}
                elif compare == '=':
                    subquery = {'term': {name: right}}
                elif compare in ('<>','!='):
                    subquery = {'must_not': {'term': {name: right}}}
                else:
                    comp = Structure.COMP[compare]
                    subquery = {'range':{name:{comp:right}}}
                if query.get(comb_v):
                    query[comb_v].append(subquery)
                else:
                    query[comb_v] = [subquery]
                print(query)
            else: # list need recurse
                print(query)
                comb_k = self._pop()
                comb_v = Structure.COMB[comb_k]
                recurse = []
                if query.get(comb_v):
                    query[comb_v].append(recurse)
                else:
                    query[comb_v] = recurse
                query[comb_v] = []
                self._quere.append('AND')
                self.struct(condition,query[comb_v])

    def _pack_col(self,col):
        return (col['left']['name'],
                col['left']['func'],
                col['right'],
                col['compare'])

    def _pop(self):
        return self._quere.pop()