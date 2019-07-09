from es_sql.utils import getkv
from es_sql.model import *


class QueryDSL:

    def __init__(self, conditions):
        self.conditions = conditions

    @property
    def combine(self):
        return {
            'AND': operator.and_,
            'OR': operator.or_
        }

    def build(self, cond):
        cond_k, cond_v = getkv(cond)
        if cond_k in self.combine.keys():
            return self.combine[cond_k](self.build(cond_v[0]), self.build(cond_v[1]))
        else:
            fname, value = getkv(cond_v)
            field = Field(fname)
            return Query(field, cond_k, value).bool()

    def to_dict(self):
        if self.conditions:
            query = self.build(self.conditions).to_dict()
            return {'query': query}
        return {}


class HQueryDSL(QueryDSL):

    hbkfield = set()

    def __init__(self, hconditions):
        super(HQueryDSL, self).__init__(hconditions)

    def build(self, hcond):
        hcond_k, hcond_v = getkv(hcond)
        if hcond_k in self.combine.keys():
            return self.combine[hcond_k](self.build(hcond_v[0]), self.build(hcond_v[1]))
        else:
            fname, v = getkv(hcond_v)
            name, value = getkv(v)
            field = AggField(name, fname)
            self.hbkfield.add(field)
            return HQuery(field, hcond_k, value).bool()

    def to_dict(self):
        if self.conditions:
            query = self.build(self.conditions).to_dict()
            return query
        return {}


class AggDSL:

    def __init__(self, aggfield, hconditions):
        self.aggfield = aggfield
        self.hconditions = hconditions

    def build(self, bkfield):
        hbool = HQueryDSL(self.hconditions).to_dict()

        hbkfield = HQueryDSL.hbkfield
        bkfields = bkfield.union(hbkfield)

        bks = [Bucket(field).to_dict() for field in bkfields]

        return Aggs(self.aggfield).to_dict(hbool, bks)

    def to_dict(self, bkfield):
        return self.build(bkfield)


class Select:

    def __init__(self, parsed):
        self.parsed = parsed

        self.return_rows = False

    def fields(self):
        column = self.parsed['column']
        group = self.parsed['group']

        fields = []
        aggfields = set()

        if '*' in column:
            return ['_id', '*'], set(), group

        for col in column:
            if isinstance(col, str):
                fields.append(col)
            else:
                func, name = getkv(col)

                if isinstance(name, dict):
                    f, v = getkv(name)
                    self.return_rows = True
                    return [], set(), v

                aggfields.add(AggField(name, func))
        return fields, aggfields, group

    def agg(self, bkfield):
        aggfields = [Agg(field) for field in self.fields()[-1]]
        hconditions = self.parsed['having']
        return AggDSL(aggfields, hconditions).to_dict(bkfield)

    def query(self):
        conditions = self.parsed['where']
        return QueryDSL(conditions).to_dict()

    def index(self):
        tables = self.parsed['table']
        return Index([table.split('.')[0] for table in tables])

    def doc_type(self):
        tables = self.parsed['table']
        return DocType([table.split('.')[1] for table in tables if '.' in table])

    def sort(self):
        order = self.parsed['order']
        return Sort(order).to_dict()

    def offset(self):
        offset = self.parsed['limit']
        if offset:
            return Offset(offset[0]).to_dict()
        return Offset().to_dict()

    def size(self):
        size = self.parsed['limit']
        if len(size) == 2:
            return Size(size[1]).to_dict()
        return Size().to_dict()