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