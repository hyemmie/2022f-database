from lark import Lark
from berkeleydb import db
import sys, os
from messages import Message
from transformer import *

PROMPT_NAME = 'DB_2017-19651> '
ERROR_STRING = 'Syntax error'
    
# def create_table(table_name):
#   b_db = db.DB()
#   b_db.open('./db/{}.db'.format(table_name), dbtype=db.DB_HASH, flags=db.DB_CREATE)
#   b_db.put(b'apple', b"red")
#   print(b_db.get(b"apple"))
#   b_db.close()


# # TODO: drop reference error
# def drop_table(table_name):
#   db_path = "./db/{}.db".format(table_name)
#   try:
#     os.remove(db_path)
#     print(Message.DropSuccess(table_name))
#   except:
#       print(Message.NoSuchTable)


# def desc_table(table_name):
#   print("-------------------------------------------------")
#   print("table_name [{}]".format(table_name))
#   print("column_name        type      null      key")
#   # for 
#   print("-------------------------------------------------")


# def show_tables():
#   print('----------------')
#   path = "./db/"
#   file_list = os.listdir(path)
#   table_list = [file for file in file_list if file.endswith(".db")]
#   for table in table_list:
#     print(table.split(".")[0])
#   print('----------------')

# myDB = db.DB()
# myDB.open('./db/myDB3.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
# myDB.put(b'apple', b"red")
# print(None == myDB.get(b"red"))
# myDB.close()
# create_table("test")
# show_tables()
#drop_table("myDB")
# table_name = "desc"
# b_db = db.DB()
# b_db.open('./db/{}.db'.format(table_name), dbtype=db.DB_HASH, flags=db.DB_CREATE)
# b_db.put(b"column_num", b"4")

# b_db.put(b"column_1_name", b"account")
# b_db.put(b"column_1_type", b"int")
# b_db.put(b"column_1_nullable", b"N")
# b_db.put(b"column_1_is_primary_key", b"PRI")
# b_db.put(b"column_1_reference_table", b"another")

# b_db.put(b"column_2_name", b"branch")
# b_db.put(b"column_2_type", b"char(15)")
# b_db.put(b"column_2_nullable", b"Y")

# b_db.put(b"column_3_name", b"id")
# b_db.put(b"column_3_type", b"int")
# b_db.put(b"column_3_nullable", b"N")
# b_db.put(b"column_3_reference_table", b"other")

# b_db.put(b"column_4_name", b"detail")
# b_db.put(b"column_4_type", b"char(15)")
# b_db.put(b"column_4_nullable", b"N")
# b_db.put(b"column_4_is_primary_key", b"PRI")
# b_db.close()

# QueryTransformer.drop_table_query()

# table_name = "account"
# b_db = db.DB()
# b_db.open('./db/{}.db'.format(table_name), dbtype=db.DB_HASH, flags=db.DB_CREATE)
# cursor = b_db.cursor()
# while x := cursor.next():
#   print(x)

# printing method
def parse_result(query, result): 
  # remove blank after semicolon
  query = query.lstrip()
  if query[:12] == 'create table':
    QueryTransformer().transform(result)
    # print(PROMPT_NAME + "'CREATE TABLE' requested")
  elif query[:10] == 'drop table':
    QueryTransformer().transform(result)
#    print(PROMPT_NAME + "'DROP TABLE' requested")
  elif query[:4] == 'desc':
    QueryTransformer().transform(result)
  #  print(PROMPT_NAME + "'DESC' requested")
  elif query[:6] == 'insert':
    print(PROMPT_NAME + "'INSERT' requested")
  elif query[:6] == 'delete':
    print(PROMPT_NAME + "'DELETE' requested")
  elif query[:6] == 'select':
    print(PROMPT_NAME + "'SELECT' requested")
  elif query[:4]== 'show':
#    print(PROMPT_NAME + "'SHOW TABLES' requested")
    QueryTransformer().transform(result)
  elif query[:6] == 'update':
    print(PROMPT_NAME + "'UPDATE' requested")
  elif query[:4] == 'exit':
    raise SystemExit

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
        parse_result(query_with_semicolon, result)
      except SystemExit:
        sys.exit(0)
      except(error):
        print(error) 
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
        parse_result(query_with_semicolon, result)
      except SystemExit:
        sys.exit(0)
      except(error):
        print(error)
        print(PROMPT_NAME + ERROR_STRING)
        break;
      
