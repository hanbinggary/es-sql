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
        self._func_columns = []
        self._model = Model()

        self.source = []

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
        name = groups.pop(0)
        subaggs = {}
        if len(groups) == 0:
            if len(havings)>0:
                subaggs['having'] = {'bucket_selector':self.struct_having(havings)}
            self.struct_func_column(subaggs)
        aggs[name] = {
            'aggs':subaggs,
            # TODO size is not accurate
            'terms': {'field': name,'size':'1000'}
        }
        if len(groups)>0:
            self.struct_group(groups,havings,subaggs)

    def struct_having(self,havings):
        selector = self._model.selector_query
        for having in havings:
            if isinstance(having,dict):
                name, func, right, compare = having['left']['name'], \
                                             having['left']['func'], \
                                             having['right'], \
                                             having['compare']
                path_value = func+'_'+name
                if (name,func) not in self._func_columns:
                    self._func_columns.append((name,func))
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

    def struct_func_column(self,aggs):
        if len(self._func_columns) > 0:
            for name,func in self._func_columns:
                if name == '*':
                    name = '_index'
                    metric_name = func
                else:
                    metric_name = func+'_'+name
                if func == 'count':
                    func = 'value_count'
                metric = {func:{'field':name}}
                aggs[metric_name] = metric

    def struct_column(self,columns):
        for column in columns:
            name = column['name']
            func = column['func']
            if func:
                self._func_columns.append((name,func))
            else:
                self.source.append(name)
        if '*' in self.source:
            self.source = []

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
        elif compare == 'IN':
            subquery = {'terms': {name: right}}
        elif compare == 'IS':
            subquery = {right: {'field': name}}
        elif compare == '=':
            subquery = {'term': {name: right}}
        elif compare in ('<>','!='):
            comb = 'must_not'
            subquery = {'term': {name: right}}
        else:
            comp = Structure.COMP[compare]
            subquery = {'range':{name:{comp:right}}}
        return subquery,comb


