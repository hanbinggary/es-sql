# -*- coding: utf-8 -*-

class Analyser(object):
    def __init__(self,response,group,column):
        self.response = response
        self.group = group
        self.column = column
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
                # bk = bucket[0]
                data[pregroup] = bucket['key']
                keys = [i for i in bucket if i not in ('key','doc_count')]
                for key in keys:
                    if key in views:
                        data[key] = bucket[key]['value']
                self.result.append(data)

    def analyse(self):
        views = []
        need_aggs = False
        for column in self.column:
            if column['func']:
                views.append(('%s_%s'%(column['func'],column['name'])))
                need_aggs = True
            else:
                views.append(column['name'])
        if need_aggs:
            bucket = self.response['aggregations']
            if len(self.group) > 0:
                self.aggs_analyse(bucket,views,None,None,0)
            else:
                for bk in bucket:
                    self.result.append({bk:bucket[bk]['value']})
        else:
            self.hits_analyse()

