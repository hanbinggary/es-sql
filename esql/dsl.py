# -*- coding: utf-8 -*-


class Aggregation:
    def __init__(self):
        self.fcolumns = 
        pass

    

def make_dsl_where(dsl,conditions):
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




