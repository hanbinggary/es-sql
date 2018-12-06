# -*- coding: utf-8 -*-

# from .common import split_list
from itertools import groupby
def split_list(source,wd):
    return [list(g) for k, g in groupby(source, lambda x: x == wd) if not k]

class Aggregation:
    def __init__(self,columns,groups,havings):
        self.columns = columns
        self.groups = groups
        self.havings = havings

        self.fcolumns = [] # function columns
        self.scolumns = [] # need to show 
        self.aggs = {}

        self._init_columns()

    def _init_columns(self):
        for column in self.columns:
            name = column['name']
            func = column['func']
            if func:
                self.fcolumns.append((name,func))
            else:
                self.scolumns.append(name)
        if '*' in self.scolumns:
            self.scolumns = []


    def _make_aggs_func(self,aggs):
        for name,func in self.fcolumns:
            if name == '*':
                name = '_index'
                metric_name = func
            else:
                metric_name = func+'_'+name
            if func == 'count':
                func = 'value_count'
            metric = {func:{'field':name}}
            aggs[metric_name] = metric


    def make_dsl_group(self,index=0,aggs=None):
        name = self.groups[index]
        gl = len(self.groups)
        subaggs = {}

        if aggs is None:
            aggs = self.aggs

        aggs[name] = {
            'aggs':subaggs,
            # TODO size is not accurate
            'terms': {'field': name,'size':'1000'}
        }

        # latest group
        if index == gl-1:
            if self.havings:
                subaggs['having'] = {'bucket_selector':self._make_dsl_having(self.havings)}
            # all funcion columns collected
            self._make_aggs_func(subaggs)
        else:
            self.make_dsl_group(index+1,subaggs)


    def _make_dsl_having(self,havings):
        selector = {'buckets_path':{},'script':''}
        for having in havings:
            if isinstance(having,dict):
                name, func, right, compare = having['left']['name'], \
                                             having['left']['func'], \
                                             having['right'], \
                                             having['compare']

                path_value = func+'_'+name
                path_name = 'val_'+path_value
                selector['buckets_path'][path_name] = path_value

                if (name,func) not in self.fcolumns:
                    self.fcolumns.append((name,func))
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
                subhaving = self._make_dsl_having(having)
                selector['buckets_path'].update(subhaving['buckets_path'])
                selector['script']+=' (%s) '%subhaving['script']
        return selector


def make_dsl_where(conditions):
    '''
    dsl: select * from test where tt = 123 and tt = 11;
    {
        "bool": 
            {
                "must": 
                    [
                        {
                            "bool": 
                                {
                                    "must": 
                                        [
                                            {
                                                "term": {"tt": 123}
                                            }
                                        ], 
                                    "should": [], 
                                    "must_not": []
                                }
                        }, 
                        {
                            "bool": 
                                {
                                    "must": 
                                        [
                                            {
                                                "term": {"tt": 11}
                                            }
                                        ], 
                                    "should": [], 
                                    "must_not": []
                                }
                        }
                    ], 
                "should": [], 
                "must_not": []
            }
    }
    '''
    result = {
        'bool':{
            'must':[],
            'should':[],
            'must_not':[]
        }
    }
    # filter 
    while isinstance(conditions, list) and len(conditions) == 1:
        conditions = conditions[0]

    # `AND` has high priority,so we should split `OR` first
    l = ['OR' in conditions,'AND' in conditions]
    if any(l):
        if l[0]:
            _conn,_bool = 'OR','should'
        else:
            _conn,_bool = 'AND','must'
        subconds = split_list(conditions,_conn)
        for subcond in subconds:
            result['bool'][_bool].append(make_dsl_where(subcond))
    else:
        left = conditions['left']['name']
        right = conditions['right']
        compare = conditions['compare']

        _bool = 'must'

        if compare == 'LIKE':
            comp_dsl = make_dsl_like(left,right)
        elif compare == 'IN':
            comp_dsl = make_dsl_in(left,right)
        elif compare == '=':
            comp_dsl = make_dsl_equal(left,right)
        elif compare in ('<>','!='):
            _bool = 'must_not'
            comp_dsl = make_dsl_not_equal(left,right)
        else:
            comp_dsl = make_dsl_range(left,right,compare)

        result['bool'][_bool].append(comp_dsl)
    return result


def make_dsl_orderby(dsl,orderby):
    '''
    #TODO sort mode option
    dsl
    {
        "sort":[
            {"name":"asc"},
            {"age":"desc"}
        ]
    }
    '''
    if len(orderby) > 0:
        _sorts = []
        for sort in orderby:
            k,v = sort['name'],sort['type']
            _sorts.append({k:v})
        dsl['sort'] = _sorts


def make_dsl_limit(dsl,limit):
    '''
    from defaults to 0, and size defaults to 10.
    
    Note that from + size can not be more than the index.max_result_window index setting which defaults to 10,000. 
    See the Scroll or Search After API for more efficient ways to do deep scrolling.

    dsl
    {
        "from":0,
        "size":100
    }
    '''
    if len(limit) > 0:
        _from,_size = limit
        dsl.setdefault('from',_from)
        dsl.setdefault('size',_size)


def make_dsl_like(left,right):
    '''
    notice:
        only support for [not analyzed] fields
    * which matches any character sequence (including the empty one) 
    ? which matches any single character

    dsl
    {
        "wildcard": { "user" : "ki*y" }
    }
    '''
    return {'wildcard': { left: right} }


def make_dsl_in(left,right):
    '''
    notice:
        only support for [not analyzed] fields

    dsl
    {
        "terms": { "user" : ["one","two"] }
    }
    '''
    return {'terms': {left: right} }

def make_dsl_equal(left,right):
    '''
    query for exact values

    dsl
    {
        "term": { "user" : "one" }
    }
    '''
    return {'term': {left: right} }


def make_dsl_range(left,right,compare):
    '''
    dsl
    {
        "range": { "age" : { "gte":10 } }
    }
    '''
    compare_mapping = {
        '>': 'gt',
        '>=': 'gte',
        '<': 'lt',
        '<=': 'lte'
    }
    comp = compare_mapping[compare]
    return {'range':{ left: {comp: right} } }

def make_dsl_not_equal(left,right):
    '''
    dsl:

    must_not
    {
        "term": { "user" : "one" }
    }
    '''
    return {'term': {left: right} }


if __name__=='__main__':
    c =  [{'name': '*', 'func': ''}]
    g= ['a', 'b', 'c']
    h = [[[{'left': {'name': 'a', 'func': 'count'}, 'right': 10, 'compare': '>'}, 'AND', {'left': {'name': 'b', 'func': 'avg'}, 'right': 5, 'compare': '<'}]]]
    a=Aggregation(c,g,h)
    a.make_dsl_group()
    print(a.aggs)

