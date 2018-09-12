# -*- coding: utf-8 -*-

from ply import lex,yacc

from . import lexer
from .exceptions import GrammarException


def p_expression(p):
    """ expression : dml END
                   | ddl END
    """
    p[0] = p[1]

def p_dml(p):
    """ dml : select
            | insert
            | update
            | delete
    """
    p[0] = p[1]


def p_ddl(p):
    """ ddl : create
            | drop
            | desc
    """
    p[0] = p[1]


###################################################
############         update            ############
###################################################
def p_update(p):
    """ update : UPDATE table SET sets where
    """
    p[0] = {
        'type': p[1],
        'table': p[2],
        'column': p[4],
        'where': p[5]
    }

def p_sets(p):
    """ sets : item COMPARISON item
             | sets COMMA sets
    """
    if ',' in p:
        p[0] = p[1] + p[3]
    else:
        p[0] = [{'name':p[1],'value':p[3]}]


###################################################
############         insert            ############
###################################################
def p_insert(p):
    """ insert : INSERT INTO table "(" items ")" VALUES values
    """
    p[0] = {
        'type': p[1],
        'table': p[3],
        'column': p[5],
        'values': p[8]
    }

def p_values(p):
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
def p_delete(p):
    """ delete : DELETE FROM table where
    """
    p[0] = {
        'type': p[1],
        'table': p[3],
        'where': p[4]
    }



###################################################
############         select            ############
###################################################
def p_select(p):
    """ select : SELECT columns FROM table where group_by having order_by limit
    """
    p[0] = {
        'type'  : p[1],
        'column': p[2],
        'table' : p[4],
        'where' : p[5],
        'group' : p[6],
        'having': p[7],
        'order' : p[8],
        'limit' : p[9]
    }

def p_table(p):
    """ table : string
              | string "." string
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = p[1] + '.' + p[3]

def p_where(p):
    """ where : WHERE conditions
              | empty
    """
    p[0] = []
    if len(p) > 2:
        p[0] = p[2]

def p_group_by(p):
    """ group_by : GROUP BY strings
                 | empty
    """
    p[0] = []
    if len(p) > 2:
        p[0] = p[3]

def p_having(p):
    """ having : HAVING conditions
               | empty
    """
    p[0] = []
    if len(p) > 2:
        p[0] = p[2]

def p_order_by(p):
    """ order_by : ORDER BY order
                 | empty
    """
    p[0] = []
    if len(p) > 2:
        p[0] = p[3]


def p_limit(p):
    """ limit : LIMIT numbers
              | empty
    """
    p[0] = []
    if len(p) > 2:
        p[0] = p[2]

def p_order(p):
    """ order : order COMMA order
              | string order_type
    """
    if len(p) > 3:
        p[0] = p[1] + p[3]
    else:
        p[0] = [{'name': p[1],'type': p[2]}]

def p_order_type(p):
    """ order_type : ASC
                   | DESC
                   | empty
    """
    if p[1] == 'DESC':
        p[0] = 'desc'
    else:
        p[0] = 'asc'


def p_columns(p):
    """ columns : columns COMMA columns
                | column
                | DISTINCT columns
    """
    if len(p) == 2:
        p[0] = [p[1]]
    if len(p) == 3:
        p[0] = {p[1].lower():p[2]}
    if len(p) == 4:
        p[0] = p[1] + p[3]

def p_column(p):
    """ column : COUNT "(" item ")"
               | SUM "(" STRING ")"
               | AVG "(" STRING ")"
               | MIN "(" STRING ")"
               | MAX "(" STRING ")"
               | item
    """
    p[0] = {'name' : p[1],'func' : ''}
    if len(p) > 2:
        p[0]['name'] = p[3]
        p[0]['func'] = p[1].lower()

def p_items(p):
    """ items : items COMMA items
              | item
    """
    if len(p) > 2:
        p[0] = p[1] + p[3]
    else:
        p[0] = [p[1]]

def p_item(p):
    """ item : QSTRING
             | STRING
             | NUMBER
             | "*"
    """
    p[0] = p[1]


# p[0] => [1,2] | [1]
def p_numbers(p):
    """ numbers : NUMBER COMMA NUMBER
                | NUMBER
    """
    if len(p) > 2:
        p[0] = [p[1], p[3]]
    else:
        p[0] = [0, p[1]]

def p_strings(p):
    """ strings : strings COMMA strings
                | string
    """
    if len(p) > 2:
        p[0] = p[1] + p[3]
    else:
        p[0] = [p[1]]


def p_string(p):
    """ string : STRING
               | QSTRING
    """
    p[0] = p[1]


def p_conditions(p):
    """ conditions : conditions AND conditions
                   | conditions OR conditions
                   | "(" conditions ")"
                   | compare
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        if '(' in p:
            p[0] = [p[2]]
        else:
            p[0] = p[1] + [p[2]] + p[3]

def p_compare(p):
    """ compare : column COMPARISON item
                | column LIKE QSTRING
                | column IN "(" items ")"
                | column IS null
    """
    p[0] = {
        'left' : p[1],
        'right': p[3],
        'compare' : p[2]
    }
    if len(p) > 4:
        p[0]['right'] = p[4]

def p_null(p):
    """ null : NULL
             | NOT NULL
    """
    if len(p) == 2:
        p[0] = 'missing'
    else:
        p[0] = 'exists'



###################################################
############         create            ############
###################################################
def p_create(p):
    """ create : CREATE TABLE if_not_exists table "(" create_columns ")"
    """
    p[0] = {
        'type':p[1],
        'table':p[4],
        'column':p[6]
    }

def p_create_columns(p):
    """ create_columns : create_column
                       | create_columns COMMA create_columns
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + p[3]

def p_create_column(p):
    """ create_column : string column_type
    """
    p[0] = {'name': p[1],'type': p[2]}


def p_if_not_exists(p):
    """ if_not_exists : IF NOT EXISTS
                      | empty
    """
    pass


def p_column_type(p):
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


###################################################
############         drop              ############
###################################################
def p_drop(p):
    """ drop : DROP TABLE table
    """
    p[0] = {
        'type': p[1],
        'table': p[3]
    }

###################################################
############         desc              ############
###################################################
def p_desc(p):
    """ desc : DESC table
    """
    p[0] = {
        'type' : p[1],
        'table': p[2]
    }


# empty return None
# so expression like (t : empty) => len()==2
def p_empty(p):
    """empty :"""
    pass

def p_error(p):
    raise GrammarException("Syntax error in input!")


tokens = lexer.tokens

DEBUG = True

L = lex.lex(module=lexer, optimize=False, debug=DEBUG)
P = yacc.yacc(debug=DEBUG)

def parse_handle(sql):
    return P.parse(input=sql,lexer=L,debug=DEBUG)





