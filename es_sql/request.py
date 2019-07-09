from es_sql.response import *
from es_sql.dsl import *


class Request:

    common_fields = ('_index', '_type', '_id')

    @staticmethod
    def select(es, parsed):
        s = Select(parsed)

        fields, aggfields, group = s.fields()

        sfields = [field for field in fields if field not in Request.common_fields]
        cfields = [field for field in fields if field in Request.common_fields]
        source = {'_source': sfields}
        dsl = {**source, **s.query(), **s.agg(aggfields), **s.sort(), **s.offset(), **s.size()}

        import json
        print(json.dumps(dsl))
        text = es.search(
            index=s.index().name,
            doc_type=s.doc_type().name,
            body=dsl
        )
        resp = Response(text, aggfields, sfields, cfields, group, s.return_rows)
        print(resp.query_result())


SQLCLASS = {
    'SELECT': Request.select
}