# -*- coding: utf-8 -*-

class Analyser(object):
    def __init__(self,response,group,column,distinct):
        self.response = response
        self.group = group
        self._column = column
        self._distinct = distinct
        self.result = []

    def hits_analyse(self):
        sources = self.response['hits']['hits']
        if len(sources) > 0:
            for source in sources:
                self.result.append(source['_source'])

    def aggs_analyse(self,bucket,views,data,pregroup,count):
        if count <= len(self.group)-1:
            group = self.group[count]
            count += 1
            if pregroup is not None:
                key = bucket['key']
                if pregroup in views:
                    data[pregroup] = key
            else:
                data = {}
            bks = bucket[group]['buckets']
            if len(bks) > 0:
                for bk in bks:
                    self.aggs_analyse(bk,views,data.copy(),group,count)
        else: # innermost bucket
            if len(bucket) > 0:
                data[pregroup] = bucket['key']
                keys = [i for i in bucket if i not in ('key','doc_count')]
                for key in keys:
                    if key in views:
                        data[key] = bucket[key]['value']
                self.result.append(data)

    def analyse(self):
        views = []
        need_aggs = False
        if 'aggregations' in self.response:
            need_aggs = True
        for column in self._column:
            name = column['name']
            func = column['func']
            if func:
                if name == '*':
                    views.append(func)
                else:
                    views.append('%s_%s'%(func,name))
            else:
                views.append(column['name'])
        if self._distinct:
            self.group = views[:]
        if need_aggs:
            bucket = self.response['aggregations']
            if len(self.group) > 0:
                self.aggs_analyse(bucket,views,None,None,0)
            else:
                for bk in bucket:
                    self.result.append({bk:bucket[bk]['value']})
        else:
            self.hits_analyse()

