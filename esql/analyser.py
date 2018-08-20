# -*- coding: utf-8 -*-

class Analyser(object):
    def __init__(self,response,aggs_result):
        self.response = response
        self.aggs_result = aggs_result

    def analyse(self):
        took = self.response['took']
        hits = self.response['hits']

        total = hits['total']
        sources = hits['hits']
        result = []
        if len(sources)>0:
            for source in sources:
                result.append(source['_source'])

        return took,total,result

    def aggs_analyse(self,aggs):
        data = {}
        key = [i for i in list(aggs.keys()) if i not in ('key','doc_count')]
        value = aggs.get('key')
        if value:
            data[key] = value

