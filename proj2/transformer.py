from lark import Lark, Transformer, Tree
from lark.visitors import CollapseAmbiguities
from messages import Message
import os, sys
from berkeleydb import db

PROMPT_NAME = 'DB_2017-19651> '
ERROR_STRING = 'Syntax error'

class QueryTransformer(Transformer):
  query_cols = 0
  column_names = []
  is_primary_key_set = False

  def create_table_query(self, items):
    table_name = items[2].children[0].value

    # check table_name exists
    path = "./db/"
    file_list = os.listdir(path)
    table_list = [file for file in file_list if file.endswith(".db")]
    for table in table_list:
      if table_name.lower() == table.split(".")[0].lower():
        return print(Message.TableExistenceError.value)

    # open new db
    b_db = db.DB()
    b_db.open('./db/{}.db'.format(table_name), dbtype=db.DB_HASH, flags=db.DB_CREATE)
    b_db.close()

    # extract table element list
    table_element_list = items[3].children
    if table_element_list[0].value == "(":
      table_element_list.pop(0)
    if table_element_list[-1].value == ")":
      table_element_list.pop(-1)
    
    for elem in table_element_list:
      print(elem.children[0].data)
      if elem.children[0].data == "column_definition":
        result = self.create_column_definition(elem, table_name)
        if result < 0:
          break
      if elem.children[0].data == "table_constraint_definition":
        result = self.create_table_constraint_definition(elem, table_name)
        if result < 0:
          break
    
    # set final column numbers
    b_db = db.DB()
    b_db.open('./db/{}.db'.format(table_name), dbtype=db.DB_HASH)
    b_db.put(b"column_num", bytes(self.query_cols, 'utf-8'))
    b_db.close()
  
  
  def create_column_definition(self, tree, table_name):
    self.query_cols += 1
    print(self.query_cols)
    # check DuplicateColumnDefError
    column_name = tree.children[0].children[0].children[0].value
    print(column_name)
    for name in self.column_names:
      if name.lower() == column_name.lower():
        print(Message.DuplicateColumnDefError.value)
        return -1
    # check CharLengthError
    data_type_list = tree.children[0].children[1].children
    if data_type_list[0].value == "char":
      if int(data_type_list[2].value) < 1:
        print(Message.CharLengthError.value)
        return -1
    data_type_str = ""
    for data in data_type_list:
      data_type_str += data.value
    print(data_type_str)
    
    is_nullable = "Y"
    if len(tree.children[0].children) > 2:
      if tree.children[0].children[2] == "not" and tree.children[0].children[3] == "null":
        is_nullable = "N"
    print(is_nullable)

    # save on DB
    b_db = db.DB()
    b_db.open('./db/{}.db'.format(table_name), dbtype=db.DB_HASH)
    b_db.put(bytes("column_{}_name".format(self.query_cols), 'utf-8'), bytes(column_name, 'utf-8'))
    b_db.put(bytes("column_{}_type".format(self.query_cols), 'utf-8'), bytes(data_type_str, 'utf-8'))
    b_db.put(bytes("column_{}_null".format(self.query_cols), 'utf-8'), bytes(is_nullable, 'utf-8'))
    b_db.close()
    self.column_names.append(column_name)
    return 1


  def create_table_constraint_definition(self, tree, table_name):
    if tree.children[0].children[0].data.value == "referential_constraint":
      print(tree.pretty())
      column_name_list = tree.children[0].children[0].children[2].children
      print(column_name_list)
      # reference_name
     
      print(tree.children[0].children[0].data.value)
    if tree.children[0].children[0].data.value == "primary_key_constraint":
      if self.is_primary_key_set == True:
        print(Message.DuplicatePrimaryKeyDefError.value)
        return -1
      self.is_primary_key_set = True
      # get primary key list
      primary_key_list = tree.children[0].children[0].children[2].children
      if primary_key_list[0].value == "(":
        primary_key_list.pop(0)
      if primary_key_list[-1].value == ")":
        primary_key_list.pop(-1)
      # form into one string to save at DB
      primary_key_string = ""
      for key_tree in primary_key_list:
        key_name = key_tree.children[0].value
        print(key_name)
        if key_name in self.column_names:
          primary_key_string += key_name + ','
          # set is_primary_key to each columns
          index = self.column_names.index(key_name) + 1
          b_db = db.DB()
          b_db.open('./db/{}.db'.format(table_name), dbtype=db.DB_HASH)
          b_db.put(bytes("column_{}_is_primary_key".format(index), 'utf-8'), b"Y")
          b_db.put(bytes("column_{}_is_null".format(index), 'utf-8'), b"N")
          b_db.close()
        else:
          print(Message.NonExistingColumnDefError(key_name))
          return -1
      primary_key_string = primary_key_string[:-1]
      print(primary_key_string)
      b_db = db.DB()
      b_db.open('./db/{}.db'.format(table_name), dbtype=db.DB_HASH)
      b_db.put(b"primary_keys", bytes(primary_key_string, 'utf-8'))
      b_db.close()
    return 1
      
  def drop_table_query(self, items):
    table_name = items[2].children[0].value
    path = "./db/"
    file_list = os.listdir(path)
    table_list = [file for file in file_list if file.endswith(".db")]
    for table in table_list:
      b_db = db.DB()
      if table.split(".")[0] == table_name:
        b_db.close()
        continue
      else:
        b_db.open('./db/{}.db'.format(table.split(".")[0]), dbtype=db.DB_HASH)
        num_cols = b_db.get(b"column_num")
        # for i in range(1, num_cols + 1):
        #   reference_table = b_db.get(b"column_{}_reference_table".format(i))
        #   if reference_table.lower() == table_name.lower():
        #     b_db.close()
        #     return print(Message.DropReferencedTableError(table_name))
        b_db.close()
    try:
      db_path = "./db/{}.db".format(table_name)
      os.remove(db_path)
      print(PROMPT_NAME + Message.DropSuccess(table_name))
    except:
      print(Message.NoSuchTable.value)

  def desc_table_query(self, items):
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
