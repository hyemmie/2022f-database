from lark import Lark
import sys

PROMPT_NAME = 'DB_2017-19651> '
ERROR_STRING = 'Syntax error'

# printing method
def parse_result(query): 
  # remove blank after semicolon
  query = query.lstrip()
  if query[:12] == 'create table':
    print(PROMPT_NAME + "'CREATE TABLE' requested")
  elif query[:10] == 'drop table':
    print(PROMPT_NAME + "'DROP TABLE' requested")
  elif query[:4] == 'desc':
    print(PROMPT_NAME + "'DESC' requested")
  elif query[:6] == 'insert':
    print(PROMPT_NAME + "'INSERT' requested")
  elif query[:6] == 'delete':
    print(PROMPT_NAME + "'DELETE' requested")
  elif query[:6] == 'select':
    print(PROMPT_NAME + "'SELECT' requested")
  elif query[:4]== 'show':
    print(PROMPT_NAME + "'SHOW TABLES' requested")
  elif query[:6] == 'update':
    print(PROMPT_NAME + "'UPDATE' requested")
  elif query[:4] == 'exit':
    raise SystemExit

# main routine
while True:
  with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")
  query_str = input(PROMPT_NAME)
  query_list = query_str.split(';')

  # CASE 1: query finished with semicolon in one line
  if (query_list[-1] == ""):
    # pop last blank by split
    query_list.pop()

    while len(query_list) > 0:
      query = query_list.pop(0)
      query_with_semicolon = query + ";"
      try: 
        result = sql_parser.parse(query_with_semicolon)
        parse_result(query_with_semicolon)
      except SystemExit:
        sys.exit(0)
      except:
        print(PROMPT_NAME + ERROR_STRING)
        break;

  # CASE 2: query didn't finish with semicolon in one line
  # multi-line query/queries
  else:
    while query_list[-1] != "":
      query_list[-1] += " "
      new_query_str = input()
      new_query_list = new_query_str.split(';')
      # continue unfinished last query
      if len(new_query_list) > 0:
        query_list[-1] += new_query_list.pop(0)
      query_list = query_list + new_query_list

    # pop last blank by split
    query_list.pop()

    while len(query_list) > 0:
      query_with_semicolon = query_list.pop(0) + ";"
      try:
        result = sql_parser.parse(query_with_semicolon)
        parse_result(query_with_semicolon)
      except SystemExit:
        sys.exit(0)
      except:
        print(PROMPT_NAME + ERROR_STRING)
        break;
      