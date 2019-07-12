from elasticsearch.helpers import scan

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

        text = es.search(
            index=s.index().name,
            doc_type=s.doc_type().name,
            body=dsl
        )
        resp = Response(text, aggfields, sfields, cfields, group, s.return_rows)
        return resp.query_result()

    @staticmethod
    def scan(es, parsed):
        '''
        这里scan的处理参考elasticsearch-dsl中的实现，由调用方从生成器中迭代数据做处理。
        毕竟scan操作性能耗费比较大
        '''
        s = Scan(parsed)

        fields = s.fields()
        sfields = [field for field in fields if field not in Request.common_fields]
        cfields = [field for field in fields if field in Request.common_fields]
        source = {'_source': sfields}
        size = s.size()
        sort = s.sort()
        if sort['sort']:
            dsl = {**source, **s.query(), **s.sort()}
            preserve_order = True
        else:
            dsl = {**source, **s.query()}
            preserve_order = False

        text = scan(
            es,
            query=dsl,
            index=s.index().name,
            doc_type=s.doc_type().name,
            preserve_order=preserve_order
        )

        resp = Hits(text)
        return resp.iter_source(sfields, cfields, size)

    @staticmethod
    def create(es, parsed):
        pass


SQLCLASS = {
    'SELECT': Request.select,
    'SCAN': Request.scan,

    'CREATE': Request.create
}