from lark import Lark, Transformer
from messages import Message
import os, sys
from berkeleydb import db

PROMPT_NAME = 'DB_2017-19651> '
ERROR_STRING = 'Syntax error'

class QueryTransformer(Transformer):
  # printing method
  def parse_result(query): 
    # remove blank after semicolon
    query = query.lstrip()
    if query[:12] == 'create table':
      print(PROMPT_NAME + "'CREATE TABLE' requested")
    elif query[:10] == 'drop table':
      drop_table()
  #    print(PROMPT_NAME + "'DROP TABLE' requested")
    elif query[:4] == 'desc':
      desc_table()   
  #    print(PROMPT_NAME + "'DESC' requested")
    elif query[:6] == 'insert':
      print(PROMPT_NAME + "'INSERT' requested")
    elif query[:6] == 'delete':
      print(PROMPT_NAME + "'DELETE' requested")
    elif query[:6] == 'select':
      print(PROMPT_NAME + "'SELECT' requested")
    elif query[:4]== 'show':
  #    print(PROMPT_NAME + "'SHOW TABLES' requested")
      show_tables()
    elif query[:6] == 'update':
      print(PROMPT_NAME + "'UPDATE' requested")
    elif query[:4] == 'exit':
      raise SystemExit

  def transform_result(query, result):
    print(result)
    parse_result(query) 
    print(transform(result))


  def create_table_query():
    table_name = ""
    path = "./db/"
    file_list = os.listdir(path)
    table_list = [file for file in file_list if file.endswith(".db")]
    for table in table_list:
      if table_name.lower() == table.split(".")[0].lower():
        return print(Message.TableExistenceError.value)

    b_db = db.DB()
    b_db.open('./db/{}.db'.format(table_name), dbtype=db.DB_HASH, flags=db.DB_CREATE)
    b_db.put(b'apple', b"red")
    print(b_db.get(b"apple"))
    b_db.close()

  def drop_table_query():
    table_name = "" 
    path = "./db/"
    file_list = os.listdir(path)
    table_list = [file for file in file_list if file.endswith(".db")]
    for table in table_list:
      b_db = db.DB()
      if table.split(".")[0] == table_name:
        b_db.close()
        continue
      else:
        print(table.split(".")[0])
        b_db.open('./db/{}.db'.format(table.split(".")[0]), dbtype=db.DB_HASH)
        num_cols = b_db.get(b"column_num")
        for i in range(1, num_cols + 1):
          reference_table = b_db.get(b"column_{}_reference_table".format(i))
          if reference_table.lower() == table_name.lower():
            b_db.close()
            return print(Message.DropReferencedTableError(table_name))
        b_db.close()
    try:
      db_path = "./db/{}.db".format(table_name)
      os.remove(db_path)
      print(Message.DropSuccess(table_name))
    except:
      print(Message.NoSuchTable.value)

  def desc_table_query():
    table_name = ""
    b_db = db.DB()
    try:
      b_db.open('./db/{}.db'.format(table_name), dbtype=db.DB_HASH)
    except:
      b_db.close()
      return print(Message.NoSuchTable.value)
    print("-------------------------------------------------")
    print("table_name [{}]".format(table_name))
    print("column_name        type      null      key")
    num_cols = int(b_db.get(b"column_num").decode('utf-8'))
    for i in range(1, num_cols + 1):
      column_name = b_db.get(bytes("column_{}_name".format(i), 'utf-8')).decode('utf-8')
      column_type = b_db.get(bytes("column_{}_type".format(i), 'utf-8')).decode('utf-8')
      column_null = b_db.get(bytes("column_{}_nullable".format(i), 'utf-8')).decode('utf-8')
      column_is_primary_key = b_db.get(bytes("column_{}_is_primary_key".format(i), 'utf-8'))
      column_reference_table = b_db.get(bytes("column_{}_reference_table".format(i), 'utf-8'))
      column_key = ""
      if column_is_primary_key != None:
        column_key += "PRI"
      if column_reference_table != None:
        if len(column_key) > 0:
          column_key += "/FOR"
        else:
          column_key += "FOR"
      print("{}     {}      {}      {}".format(column_name, column_type, column_null, column_key))
    b_db.close()
    print("-------------------------------------------------")

  def show_tables_query(self, item):
    print('----------------')
    path = "./db/"
    file_list = os.listdir(path)
    table_list = [file for file in file_list if file.endswith(".db")]
    for table in table_list:
      print(table.split(".")[0])
    print('----------------')
