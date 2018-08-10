# -*- coding: utf-8 -*-

from ply import lex,yacc

from . import lexer
from .exceptions import GrammarException

def p_expression(p):
    """ expression : dml END
    """
    p[0] = p[1]

# def p_expression(p):
#     """ expression : dml END
#                    | ddl END
#     """
#     p[0] = p[1]

def p_dml(p):
    """ dml : select
    """
    p[0] = p[1]

# def p_dml(p):
#     """ dml : select
#             | update
#             | insert
#             | delete
#     """
#     p[0] = p[1]

# def p_ddl(p):
#     """ ddl : create
#             | alter
#             | drop
#     """
#     p[0] = p[1]


###################################################
############         select            ############
###################################################
def p_select(p):
    """ select : SELECT columns FROM STRING where group_by having order_by limit
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

def p_where(p):
    """ where : WHERE conditions
              | empty
    """
    p[0] = []
    if len(p) > 2:
        p[0] = p[2]

def p_group_by(p):
    """ group_by : GROUP BY columns
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
        p[0] = p[3]

def p_order_by(p):
    """ order_by : ORDER BY columns order
                 | empty
    """
    p[0] = [{'value': [],'mode': ''}]
    if len(p) > 2:
        p[0][0]['value'] = p[3]
        p[0][0]['mode'] = p[4]


def p_limit(p):
    """ limit : LIMIT numbers
              | empty
    """
    p[0] = []
    if len(p) > 2:
        p[0] = p[2]


def p_order(p):
    """ order : ASC
              | DESC
              | empty
    """
    if p[1] == 'DESC':
        p[0] = 'DESC'
    else:
        p[0] = 'ASC'





# p[0] => [x,x..] | [x]
def p_columns(p):
    """ columns : columns COMMA STRING
                | STRING
                | "*"
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[3]]

# p[0] => [1,2] | [1]
def p_numbers(p):
    """ numbers : NUMBER COMMA NUMBER
                | NUMBER
    """
    p[0] = [p[1]]
    if len(p) > 2:
        p[0] += [p[3]]

def p_conditions(p):
    """ conditions : conditions AND compare
                   | conditions OR compare
                   | compare
    """
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[2]] + [p[3]]

def p_compare(p):
    """ compare : litem COMPARISON ritem
                | litem LIKE ritem
    """
    p[0] = {
        'left' : p[1],
        'right': p[3],
        'compare' : p[2]
    }

def p_litem(p):
    """ litem : STRING
    """
    p[0] = p[1]

def p_ritem(p):
    """ ritem : QSTRING
              | STRING
              | NUMBER
    """
    p[0] = p[1]


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





