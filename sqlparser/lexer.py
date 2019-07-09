from ply import lex

from .exceptions import IllegalCharacterException


class Lexer:
    reserved = {
        'select': 'SELECT',
        'scan' : 'SCAN',
        'distinct': 'DISTINCT',
        'from'  : 'FROM',
        'where' : 'WHERE',
        'group' : 'GROUP',
        'by'    : 'BY',
        'having': 'HAVING',
        'order' : 'ORDER',
        'desc'  : 'DESC',
        'asc'   : 'ASC',
        'limit' : 'LIMIT',
        'insert': 'INSERT',
        'into'  : 'INTO',
        'values': 'VALUES',
        'update': 'UPDATE',
        'set'   : 'SET',
        'delete': 'DELETE',

        'create': 'CREATE',
        'table' : 'TABLE',
        'drop'  : 'DROP',
        'show'  : 'SHOW',
        'with'  : 'WITH',

        'as'    : 'AS',
        'and'   : 'AND',
        'or'    : 'OR',
        'in'    : 'IN',
        'like'  : 'LIKE',
        'between': 'BETWEEN',
        'is'    : 'IS',
        'not'   : 'NOT',
        'null'  : 'NULL',
        'count' : 'COUNT',
        'sum'   : 'SUM',
        'avg'   : 'AVG',
        'min'   : 'MIN',
        'max'   : 'MAX',

        'text'  : 'TEXT',
        'keyword': 'KEYWORD',
        'long'  : 'LONG',
        'integer': 'INTEGER',
        'short' : 'SHORT',
        'type'  : 'TYPE',
        'double': 'DOUBLE',
        'float' : 'FLOAT',
        'date'  : 'DATE',
        'boolean': 'BOOLEAN',
        'binary': 'BINARY'
    }

    tokens = (
        'COMPARISON',
        'NAME',
        'NUMBER',
        'STRING',
        'END',
        'COMMA',
    ) + tuple(set(reserved.values()))

    literals = '(){}@%.*[]:-^'
    t_COMPARISON = r'<>|!=|>=|<=|=|>|<'
    t_END = r';'
    t_COMMA = r','
    t_ignore = ' \t\n'

    def __init__(self,
                 debug=False):

        self.lexer = lex.lex(module=self,
                             debug=debug)

    def t_NAME(self, t):
        r"[_a-zA-Z][_a-zA-Z0-9]*|[\u4e00-\u9fa5]+"
        t.type = Lexer.reserved.get(t.value.lower(), 'NAME')
        if t.type != 'NAME':
            t.value = t.value.upper()
        return t

    def t_STRING(self, t):
        r"('[^\']*')|(\"[^\"]*\")"
        t.value = t.value[1:-1]
        return t

    def t_NUMBER(self, t):
        r"\d+(\.\d+)?"
        return t

    def t_error(self, t):
        raise IllegalCharacterException("Illegal character {}.".format(t.value[0]))

