from ply import yacc

from .lexer import Lexer
from .exceptions import SyntaxException


class Parser:
    def __init__(self,
                 lexer=None,
                 debug=False):

        self.debug = debug
        if not lexer:
            lexer = Lexer(self.debug)
        self.tokens = lexer.tokens
        self.lexer = lexer.lexer

        self.parser = yacc.yacc(module=self,
                                debug=self.debug)

    def parse(self, sql):
        return self.parser.parse(input=sql,
                                 lexer=self.lexer,
                                 debug=self.debug)

    def p_expression(self, p):
        """ expression : dml END
                       | ddl END
        """
        p[0] = p[1]


    def p_dml(self, p):
        """ dml : select
                | scan
                | insert
                | update
                | delete
        """
        p[0] = p[1]


    def p_ddl(self, p):
        """ ddl : create
                | drop
                | desc
        """
        p[0] = p[1]


    ###################################################
    ############         update            ############
    ###################################################
    def p_update(self, p):
        """ update : UPDATE table SET sets where
        """
        p[0] = {
            'method': p[1],
            'table': p[2],
            'column': p[4],
            'where': p[5]
        }


    def p_sets(self, p):
        """ sets : item COMPARISON item
                 | sets COMMA sets
        """
        if ',' in p:
            p[0] = p[1] + p[3]
        else:
            p[0] = [{'name': p[1], 'value':p[3]}]


    ###################################################
    ############         insert            ############
    ###################################################
    def p_insert(self, p):
        """ insert : INSERT INTO table "(" items ")" VALUES values
        """
        p[0] = {
            'method': p[1],
            'table': p[3],
            'column': p[5],
            'values': p[8]
        }

    def p_values(self, p):
        """ values : "(" items ")"
                   | values COMMA values
        """
        if "," in p:
            p[0] = p[1] + p[3]
        else:
            p[0] = [p[2]]


    ###################################################
    ############         delete            ############
    ###################################################
    def p_delete(self, p):
        """ delete : DELETE FROM tables where
        """
        p[0] = {
            'method': p[1],
            'table': p[3],
            'where': p[4]
        }

    ###################################################
    ############         scan              ############
    ###################################################
    def p_scan(self, p):
        """ scan : SCAN columns FROM tables where order_by size
        """
        p[0] = {
            'method': p[1],
            'column': p[2],
            'table': p[4],
            'where': p[5],
            'order': p[6],
            'limit': p[7]
        }

    def p_size(self, p):
        """ size : LIMIT NUMBER
                 | empty
        """
        p[0] = -1
        if len(p) > 2:
            p[0] = p[2]


    ###################################################
    ############         select            ############
    ###################################################
    def p_select(self, p):
        """ select : SELECT columns FROM tables where group_by having order_by limit
        """
        p[0] = {
            'method': p[1],
            'column': p[2],
            'table' : p[4],
            'where' : p[5],
            'group' : p[6],
            'having': p[7],
            'order' : p[8],
            'limit' : p[9]
        }

    def p_tables(self, p):
        """ tables : table
                   | table COMMA tables
        """
        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = [p[1]] + p[3]

    def p_table(self, p):
        """ table : string
                  | string "." string
        """
        if len(p) == 2:
            p[0] = p[1]
        else:
            p[0] = p[1] + '.' + p[3]


    def p_where(self, p):
        """ where : WHERE conditions
                  | empty
        """
        p[0] = []
        if len(p) > 2:
            p[0] = p[2]


    def p_group_by(self, p):
        """ group_by : GROUP BY columns
                     | empty
        """
        p[0] = []
        if len(p) > 2:
            p[0] = p[3]


    def p_having(self, p):
        """ having : HAVING having_conditions
                   | empty
        """
        p[0] = []
        if len(p) > 2:
            p[0] = p[2]


    def p_order_by(self, p):
        """ order_by : ORDER BY order
                     | empty
        """
        p[0] = []
        if len(p) > 2:
            p[0] = p[3]


    def p_limit(self, p):
        """ limit : LIMIT numbers
                  | empty
        """
        p[0] = []
        if len(p) > 2:
            p[0] = p[2]


    def p_order(self, p):
        """ order : order COMMA order
                  | string order_type
        """
        if len(p) > 3:
            p[0] = p[1] + p[3]
        else:
            p[0] = [{p[1]: p[2]}]


    def p_order_type(self, p):
        """ order_type : ASC
                       | DESC
                       | empty
        """
        p[0] = 'asc'
        if p[1]:
            p[0] = p[1].lower()


    def p_columns(self, p):
        """ columns : columns COMMA columns
                    | column
                    | DISTINCT columns
                    | DISTINCT "(" columns ")"
        """
        if len(p) == 2:
            p[0] = [p[1]]
        elif len(p) == 3:
            p[0] = {p[1]: p[2]}
        elif len(p) == 4:
            p[0] = p[1] + p[3]
        else:
            p[0] = {p[1]: p[3]}


    def p_column(self, p):
        """ column : COUNT "(" column ")"
                   | SUM "(" column ")"
                   | AVG "(" column ")"
                   | MIN "(" column ")"
                   | MAX "(" column ")"
                   | COUNT "(" DISTINCT columns ")"
                   | COUNT "(" DISTINCT "(" columns ")" ")"
                   | item
                   | item "." KEYWORD
        """
        if len(p) == 2:
            p[0] = p[1]
        elif len(p) == 4:
            p[0] = '{}.{}'.format(p[1], p[3].lower())
        elif len(p) == 5:
            p[0] = {p[1]: p[3]}
        elif len(p) == 6:
            p[0] = {p[1]: {p[3]: p[4]}}
        else:
            p[0] = {p[1]: {p[3]: p[5]}}


    def p_items(self, p):
        """ items : items COMMA items
                  | item
        """
        if len(p) > 2:
            p[0] = p[1] + p[3]
        else:
            p[0] = [p[1]]


    def p_item(self, p):
        """ item : STRING
                 | NAME
                 | NUMBER
                 | "*"
        """
        p[0] = p[1]


    # p[0] => [1,2] | [1]
    def p_numbers(self, p):
        """ numbers : NUMBER COMMA NUMBER
                    | NUMBER
        """
        if len(p) > 2:
            p[0] = [p[1], p[3]]
        else:
            p[0] = [0, p[1]]


    def p_string(self, p):
        """ string : NAME
                   | STRING
        """
        if len(p) > 2:
            p[0] = '{}.{}'.format(p[1], p[3])
        else:
            p[0] = p[1]


    def p_conditions(self, p):
        """ conditions : conditions AND conditions
                       | conditions OR conditions
                       | "(" conditions ")"
                       | compare
        """
        if len(p) == 2:
            p[0] = p[1]
        else:
            if '(' in p:
                p[0] = p[2]
            else:
                p[0] = {p[2]: [p[1], p[3]]}

    def p_having_conditions(self, p):
        """ having_conditions : having_conditions AND having_conditions
                              | having_conditions OR having_conditions
                              | "(" having_conditions ")"
                              | having_compare
        """
        if len(p) == 2:
            p[0] = p[1]
        else:
            if '(' in p:
                p[0] = p[2]
            else:
                p[0] = {p[2]: [p[1], p[3]]}

    def p_compare(self, p):
        """ compare : NAME COMPARISON item
                    | NAME BETWEEN item AND item
                    | NAME LIKE item
                    | NAME IN "(" items ")"
                    | NAME NOT IN "(" items ")"
                    | NAME IS NULL
                    | NAME IS NOT NULL
        """
        if p[2] == 'IS':
            if len(p) == 4:
                p[0] = {'IS': {p[1]: p[3]}}
            else:
                p[0] = {'ISNOT': {p[1]: p[4]}}
        elif p[2] == 'BETWEEN':
            p[0] = {'BETWEEN': {p[1]: [p[3], p[5]]}}
        elif p[2] == 'IN':
            p[0] = {'IN': {p[1]: p[4]}}
        elif p[2] == 'NOT':
            p[0] = {'NOTIN': {p[1]: p[5]}}
        else:
            p[0] = {p[2]: {p[1]: p[3]}}

    def p_having_compare(self, p):
        """ having_compare : COUNT "(" item ")" COMPARISON NUMBER
                           | SUM "(" item ")" COMPARISON NUMBER
                           | AVG "(" item ")" COMPARISON NUMBER
                           | MIN "(" item ")" COMPARISON NUMBER
                           | MAX "(" item ")" COMPARISON NUMBER
        """
        p[0] = {p[5]: {p[1]: {p[3]: p[6]}}}


    ###################################################
    ############         create            ############
    ###################################################
    def p_create(self, p):
        """ create : CREATE TABLE table "(" create_columns ")" with
        """
        p[0] = {
            'method': p[1],
            'table': p[3],
            'column': p[5],
            'with': p[7]
        }

    def p_create_columns(self, p):
        """ create_columns : create_column
                           | create_columns COMMA create_columns
        """
        if len(p) == 2:
            p[0] = [p[1]]
        else:
            p[0] = p[1] + p[3]

    def p_create_column(self, p):
        """ create_column : string column_type
        """
        p[0] = {'name': p[1], 'type': p[2]}

    def p_column_type(self, p):
        """ column_type : TEXT
                        | KEYWORD
                        | LONG
                        | INTEGER
                        | SHORT
                        | TYPE
                        | DOUBLE
                        | FLOAT
                        | DATE
                        | BOOLEAN
                        | BINARY
        """
        p[0] = p[1].lower()

    def p_with(self, p):
        """ with : WITH numbers
                 | empty
        """
        p[0] = []
        if len(p) > 2:
            p[0] = p[2]


    ###################################################
    ############         drop              ############
    ###################################################
    def p_drop(self, p):
        """ drop : DROP TABLE tables
        """
        p[0] = {
            'method': p[1],
            'table': p[3]
        }


    ###################################################
    ############         desc              ############
    ###################################################
    def p_desc(self, p):
        """ desc : DESC tables
        """
        p[0] = {
            'method': p[1],
            'table': p[2]
        }


    # empty return None
    # so expression like (t : empty) => len()==2
    def p_empty(self, p):
        """empty :"""
        pass


    def p_error(self, p):
        raise SyntaxException("Syntax error in input!")







