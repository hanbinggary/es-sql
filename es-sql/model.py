# -*- coding: utf-8 -*-

class Model(object):

    @property
    def bool_query(self):
        m = {
            'bool':{
                'must':[],
                'should':[],
                'must_not':[]
            }
        }
        return m

    @property
    def aggs_query(self):
        m = {'aggs':{}}
        return m

    @property
    def selector_query(self):
        m = {'buckets_path':{},'script':''}
        return m