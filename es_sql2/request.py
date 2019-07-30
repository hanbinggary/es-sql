import re

from elasticsearch.helpers import scan, bulk

from es_sql2.response import *
from es_sql2.dsl import *
from es_sql2.utils import error_handle


class Request:

    common_fields = ('_index', '_type', '_id')

    @staticmethod
    def sep_fileds(fields):
        sf = []
        cf = []
        for field in fields:
            if field in Request.common_fields:
                cf.append(field)
            else:
                sf.append(field)
        return sf, cf

    @staticmethod
    @error_handle
    def select(es, parsed):
        s = Select(parsed)

        fields, aggfields, group = s.fields()
        sf, cf = Request.sep_fileds(fields)
        source = {'_source': sf}
        dsl = {
            **source,
            **s.query(),
            **s.agg(aggfields),
            **s.sort(),
            **s.offset(),
            **s.size()
        }

        text = es.search(
            index=s.index().name,
            doc_type=s.doc_type().name,
            body=dsl
        )
        resp = Response(text, aggfields, sf, cf, group, s.return_rows)
        return resp.query_result()

    @staticmethod
    def scan(es, parsed):
        '''
        这里scan的处理参考elasticsearch-dsl中的实现，由调用方从生成器中迭代数据做处理。
        毕竟scan操作性能耗费比较大
        '''
        s = Scan(parsed)

        fields = s.fields()
        sf, cf = Request.sep_fileds(fields)
        source = {'_source': sf}
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
        return resp.iter_source(sf, cf, size)

    @staticmethod
    @error_handle
    def insert(es, parsed):
        s = Insert(parsed)

        columns = s.fields()
        values = s.values()

        datas = [dict(zip(columns, value)) for value in values]

        def iteritem(items):
            for item in items:
                sour = {
                    '_index': s.index().name,
                    '_type': s.doc_type().name
                }

                if '_id' in item:
                    sour['_id'] = item['_id']
                    del item['_id']

                sour['_source'] = item

                yield sour

        text = bulk(es, iteritem(datas))
        return text

    @staticmethod
    @error_handle
    def delete(es, parsed):
        s = Delete(parsed)

        index = s.index().name
        doc_type = s.doc_type().name
        id = s.id()

        text = es.delete(
            index=index,
            doc_type=doc_type,
            id=id
        )
        return text

    @staticmethod
    @error_handle
    def update(es, parsed):
        # todo 支持update_by_query
        s = Update(parsed)

        index = s.index().name
        doc_type = s.doc_type().name
        id = s.id()

        text = es.update(
            index=index,
            doc_type=doc_type,
            id=id,
            body={'doc': s.reset_value()}
        )
        return text

    @staticmethod
    @error_handle
    def create(es, parsed):
        s = Create(parsed)

        index = s.index().name
        doc_type = s.doc_type().name

        body = {
            'mappings': {
                doc_type: {
                    'properties': s.fields()
                }
            },
            'settings': {
                'number_of_shards': s.shards(),
                'number_of_replicas': s.replicas()
            }
        }

        text = es.indices.create(
            index=index,
            body=body
        )

        return text

    @staticmethod
    @error_handle
    def drop(es, parsed):
        s = Drop(parsed)

        text = es.indices.delete(
            index=s.index().name
        )

        return text

    @staticmethod
    @error_handle
    def desc(es, parsed):
        s = Desc(parsed)

        index = s.index().name
        doc_type = s.doc_type().name

        text = es.indices.get_mapping(
            index=index,
            doc_type=doc_type
        )
        return MappingRes.to_result(text, index)

    @staticmethod
    @error_handle
    def show(es, parsed):
        s = Show(parsed)

        opt = s.opt()
        reg = s.reg()

        if opt == 'tables':
            text = es.cat.indices()
            if reg:
                pattern = re.compile(reg)
                return list(filter(lambda x: re.search(pattern, x['index']), text))
            return text


SQLCLASS = {
    'SELECT': Request.select,
    'SCAN': Request.scan,
    'INSERT': Request.insert,
    'DELETE': Request.delete,
    'UPDATE': Request.update,

    'CREATE': Request.create,
    'DROP': Request.drop,
    'DESC': Request.desc,

    'SHOW': Request.show
}