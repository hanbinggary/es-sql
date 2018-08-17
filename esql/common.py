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
        self.func_columns = []
        self._show_columns = []
        self._model = Model()

    def struct_where(self,conditions):
        result = self._model.bool_query
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
                result['bool'][comb_v].append(self.struct_where(subcond))
        else:
            subquery, comb = self._subquery(conditions, 'must')
            result['bool'][comb].append(subquery)
        return result

    def struct_group(self, groups,havings, aggs):
        group = groups.pop()
        name = group['name']
        if len(groups) == 0 and len(havings)>0:
            subaggs = {'having':{'bucket_selector':self.struct_having(havings)}}
        else:
            subaggs = {}
        aggs[name] = {
            'aggs':subaggs,
            'terms': {'field': name}
        }
        if len(groups)>0:
            self.struct_group(groups,havings,subaggs)

    def struct_having(self,havings):
        selector = {'buckets_path':{},'script':''}
        for having in havings:
            if isinstance(having,dict):
                name, func, right, compare = having['left']['name'], \
                                             having['left']['func'].lower(), \
                                             having['right'], \
                                             having['compare']
                path_value = func+'_'+name
                if path_value not in self.func_columns:
                    self.func_columns.append(path_value)
                path_name = 'val_'+path_value
                selector['buckets_path'][path_name] = path_value
                if compare == '=':
                    compare = '=='
                if compare == '<>':
                    compare = '!='
                selector['script']+=' (%s %s %s) '%(path_name,compare,right)
            elif isinstance(having,str):
                if having == 'AND':
                    comb = '&&'
                else:
                    comb = '||'
                selector['script']+=' %s '%comb
            else:# list
                subhaving = self.struct_having(having)
                selector['buckets_path'].update(subhaving['buckets_path'])
                selector['script']+=' (%s) '%subhaving['script']
        return selector



    def struct_column(self,columns):
        for column in columns:
            name = column['name']
            func = column['func'].lower()
            if func:
                n = '%s_%s'%(name,func)
                self.func_columns.append(n)
                self._show_columns.append(n)
            else:
                self._show_columns.append(name)



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


