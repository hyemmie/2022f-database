from lark import Lark, Transformer, Tree, Token
from lark.visitors import CollapseAmbiguities
from messages import Message
import os, sys
from berkeleydb import db

PROMPT_NAME = 'DB_2017-19651> '
ERROR_STRING = 'Syntax error'

class QueryTransformer(Transformer):

  def __init__(self): 
    super()
    self.query_cols = 0
    self.column_names = []
    self.is_primary_key_set = False
    self.term_list = []
    self.mark_list_per_term = []

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
    b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH, flags=db.DB_CREATE)
    b_db.close()

    # extract table element list
    table_element_list = items[3].children
    if table_element_list[0].value == "(":
      table_element_list.pop(0)
    if table_element_list[-1].value == ")":
      table_element_list.pop(-1)

    for elem in table_element_list:
      if elem.children[0].data == "column_definition":
        result = self.create_column_definition(elem, table_name)
        if result < 0:
          db_path = "./db/{}.db".format(table_name.lower())
          os.remove(db_path)
          return 0
      if elem.children[0].data == "table_constraint_definition":
        result = self.create_table_constraint_definition(elem, table_name)
        if result < 0:
          db_path = "./db/{}.db".format(table_name.lower())
          os.remove(db_path)
          return 0
    
    # set final column numbers
    b_db = db.DB()
    b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
    b_db.put(b"column_num", bytes("{}".format(self.query_cols), 'utf-8'))
    b_db.close()
    print(PROMPT_NAME + Message.CreateTableSuccess(table_name))
  
  def create_column_definition(self, tree, table_name):
    self.query_cols += 1
    # check DuplicateColumnDefError
    column_name = tree.children[0].children[0].children[0].value
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
    
    is_nullable = "Y"
    if len(tree.children[0].children) > 2:
      if tree.children[0].children[2] == "not" and tree.children[0].children[3] == "null":
        is_nullable = "N"

    # save on DB
    b_db = db.DB()
    b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
    b_db.put(bytes("column_{}_name".format(self.query_cols), 'utf-8'), bytes(column_name, 'utf-8'))
    b_db.put(bytes("column_{}_type".format(self.query_cols), 'utf-8'), bytes(data_type_str, 'utf-8'))
    b_db.put(bytes("column_{}_is_nullable".format(self.query_cols), 'utf-8'), bytes(is_nullable, 'utf-8'))
    b_db.close()
    self.column_names.append(column_name.lower())
    return 1

  def create_table_constraint_definition(self, tree, table_name):
    # FOREIGN KEY CREATION
    if tree.children[0].children[0].data.value == "referential_constraint":
      current_column_list = tree.children[0].children[0].children[2].children
      if current_column_list[0].value == "(":
        current_column_list.pop(0)
      if current_column_list[-1].value == ")":
        current_column_list.pop(-1)

      reference_table = tree.children[0].children[0].children[4].children[0].value

      reference_column_list = tree.children[0].children[0].children[5].children
      if reference_column_list[0].value == "(":
        reference_column_list.pop(0)
      if reference_column_list[-1].value == ")":
        reference_column_list.pop(-1)

      for i in range(len(current_column_list)):
        current_column_list[i] = current_column_list[i].children[0].value.lower()

      for i in range(len(reference_column_list)):
        reference_column_list[i] = reference_column_list[i].children[0].value.lower()

      # check reference column numbers
      if len(current_column_list) != len(reference_column_list):
        print(Message.ReferenceTypeError.value)
        return -1

      # check input column existence
      for col in current_column_list:
        if col.lower() in self.column_names:
          continue
        else:
          print(Message.NonExistingColumnDefError(col))
          return -1
        
      # check reference table existence
      reference_table_exists = False
      path = "./db/"
      file_list = os.listdir(path)
      table_list = [file for file in file_list if file.endswith(".db")]
      for table in table_list:
        if reference_table.lower() == table.split(".")[0].lower():
          reference_table_exists = True
          break
      if reference_table_exists == False or reference_table.lower() == table_name:
        print(Message.ReferenceTableExistenceError.value)
        return -1

      # check referece column existence & type 
      b_db = db.DB()
      b_db.open('./db/{}.db'.format(reference_table.lower()), dbtype=db.DB_HASH)
      num_cols = int(b_db.get(b"column_num").decode('utf-8'))
      b_db.close()
      checked_cols = []
      # check every reference column from input
      for ref_col in reference_column_list:
        # check DB for each input reference column
        for i in range(1, num_cols+1):
          b_db = db.DB()
          b_db.open('./db/{}.db'.format(reference_table.lower()), dbtype=db.DB_HASH)
          saved_ref_col_name = b_db.get(bytes("column_{}_name".format(i), 'utf-8')).decode('utf-8')
          saved_ref_col_type = b_db.get(bytes("column_{}_type".format(i), 'utf-8')).decode('utf-8')
          saved_ref_is_primary_key = b_db.get(bytes("column_{}_is_primary_key".format(i), 'utf-8'))
          b_db.close()
          if saved_ref_is_primary_key != None:
            saved_ref_is_primary_key = saved_ref_is_primary_key.decode('utf-8')
          # check reference column existence
          if ref_col.lower() == saved_ref_col_name.lower():
            # check reference column is primary key
            if saved_ref_is_primary_key == None or saved_ref_is_primary_key != "Y":
              print(Message.ReferenceNonPrimaryKeyError.value)
              return -1
            # compare reference column type with current column type
            b_db = db.DB()
            b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
            ref_index = reference_column_list.index(ref_col)
            curr = self.column_names.index(current_column_list[ref_index]) + 1
            saved_current_col_type = b_db.get(bytes("column_{}_type".format(curr), 'utf-8')).decode('utf-8')
            b_db.close()
            if saved_ref_col_type != saved_current_col_type:
              print(Message.ReferenceTypeError.value)
              return -1
            else:
              checked_cols.append(ref_col)
              break
            
      if len(checked_cols) != len(reference_column_list):
        print(Message.ReferenceColumnExistenceError.value)
        return -1
      
      # check if reference column is composite primary key
      b_db = db.DB()
      b_db.open('./db/{}.db'.format(reference_table.lower()), dbtype=db.DB_HASH)
      saved_ref_primary_keys = b_db.get(bytes("primary_keys".format(i), 'utf-8')).decode('utf-8')
      b_db.close()
      primary_key_list = saved_ref_primary_keys.split(',')
      # if composite primary key, all of input foreign key should reference it
      if len(primary_key_list) > 1:
        for key in primary_key_list:
          if key.lower() in reference_column_list:
            continue
          else:
            print(Message.ReferenceNonPrimaryKeyError.value)
            return -1
      
      # if it pass all error check, save all foreign key info to db
      for curr_col in current_column_list:
        b_db = db.DB()
        b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
        curr_index_in_sef_column_names = self.column_names.index(curr_col) + 1
        ref_index = current_column_list.index(curr_col)
        b_db.put(bytes("column_{}_reference_table".format(curr_index_in_sef_column_names), 'utf-8'), bytes(reference_table, 'utf-8'))
        b_db.put(bytes("column_{}_reference_column".format(curr_index_in_sef_column_names), 'utf-8'), bytes(reference_column_list[ref_index], 'utf-8'))
        b_db.close()
      return 1

    # PRIMARY KEY CREATION
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
        if key_name.lower() in self.column_names:
          primary_key_string += key_name + ','
          # set is_primary_key to each columns
          index = self.column_names.index(key_name.lower()) + 1
          b_db = db.DB()
          b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
          b_db.put(bytes("column_{}_is_primary_key".format(index), 'utf-8'), b"Y")
          b_db.put(bytes("column_{}_is_nullable".format(index), 'utf-8'), b"N")
          b_db.close()
        else:
          print(Message.NonExistingColumnDefError(key_name))
          return -1
      primary_key_string = primary_key_string[:-1]
      b_db = db.DB()
      b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
      b_db.put(b"primary_keys", bytes(primary_key_string, 'utf-8'))
      b_db.close()
      return 1
      
  def drop_table_query(self, items):
    table_name = items[2].children[0].value.lower()
    path = "./db/"
    file_list = os.listdir(path)
    table_list = [file for file in file_list if file.endswith(".db")]
    for i in range(len(table_list)):
      table_list[i] = table_list[i].split(".")[0].lower()
    if table_name not in table_list:
      return print(Message.NoSuchTable.value)
    else:
      for table in table_list:
        b_db = db.DB()
        b_db.open('./db/{}.db'.format(table), dbtype=db.DB_HASH)
        num_cols = int(b_db.get(b"column_num").decode('utf-8'))
        for i in range(1, num_cols + 1):
          reference_table = b_db.get(bytes("column_{}_reference_table".format(i), 'utf-8'))
          if reference_table == None:
            continue
          else:
            reference_table = reference_table.decode('utf-8').lower()
            if reference_table == table_name:
              b_db.close()
              return print(Message.DropReferencedTableError(table_name))

        b_db.close()
    try:
      db_path = "./db/{}.db".format(table_name)
      os.remove(db_path)
      print(PROMPT_NAME + Message.DropSuccess(table_name))
    except:
      print(Message.NoSuchTable.value)

  def desc_query(self, items):
    table_name = items[1].children[0].value.lower()
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
      column_null = b_db.get(bytes("column_{}_is_nullable".format(i), 'utf-8'))
      if column_null != None:
        column_null = column_null.decode('utf-8')
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

  def insert_query(self, item):
    # print(item)
    table_name = item[2].children[0].value
    # print(table_name)
    # check table existence
    b_db = db.DB()
    try:
      b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
    except:
      b_db.close()
      return print(Message.NoSuchTable.value)
    
    column_list = item[3]
    value_list = item[5].children
    if value_list[0].value == "(":
      value_list.pop(0)
    if value_list[-1].value == ")":
      value_list.pop(-1)

    print('BEFORE INSERT')
    cursor = b_db.cursor()
    while x := cursor.next():
      print(x)

    num_cols = int(b_db.get(b"column_num").decode('utf-8'))
    num_rows_byte = b_db.get(b"row_num")
    num_rows = 0
    if num_rows_byte != None:
      num_rows = int(num_rows_byte.decode('utf-8'))

    deleted_num_byte = b_db.get(b"deleted_num")
    deleted_num = 0
    if deleted_num_byte != None:
      deleted_num = int(deleted_num_byte.decode('utf-8'))
    b_db.close()

    # check attribute number
    if num_cols != len(value_list):
      return print(Message.InsertTypeMismatchError.value)

    save_value_list = []
   
    if column_list is None:
      i = 0
      for value in value_list:
        i += 1
        value_value = value.children[0].value
        value_type = value.children[0].type
        
        b_db = db.DB()
        b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
        column_name = b_db.get(bytes("column_{}_name".format(i), 'utf-8')).decode('utf-8')
        column_type = b_db.get(bytes("column_{}_type".format(i), 'utf-8')).decode('utf-8')
        column_nullable = b_db.get(bytes("column_{}_is_nullable".format(i), 'utf-8')).decode('utf-8')
      
        column_is_primary_key = b_db.get(bytes("column_{}_is_primary_key".format(i), 'utf-8'))
        column_reference_table = b_db.get(bytes("column_{}_reference_table".format(i), 'utf-8'))
        column_reference_column = b_db.get(bytes("column_{}_reference_column".format(i), 'utf-8'))
        b_db.close()

        if column_nullable == "N" and value_value == "null":
          return print(Message.InsertColumnNonNullableError(column_name))
      
        if column_type == "int" and value_type != "INT" and value_value != "null":
          return print(Message.InsertTypeMismatchError.value)

        if "char" in column_type and value_type != "STR" and value_value != "null":
          return print(Message.InsertTypeMismatchError.value)

        if column_type == "date" and value_type != "DATE" and value_value != "null":
          return print(Message.InsertTypeMismatchError.value)

        if value_type == "STR":
          value_value = value_value[1:-1]

        if "char" in column_type:
          max_index = int(column_type.split("(")[1].split(")")[0])
          if len(value_value) > max_index:
            value_value = value_value[:max_index]
        save_value_list.append(value_value)
        
        # if this column is primary key
        if column_is_primary_key != None and column_is_primary_key.decode('utf-8') == "Y":
          duplicated = False
          b_db = db.DB()
          b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
          for j in range(1, num_rows + deleted_num + 1):
            print("{}, {}".format(j, i))
            row_value = b_db.get(bytes("row_{}_col_{}".format(j, i), 'utf-8'))
            if row_value != None:
              row_value = row_value.decode('utf-8')
              if row_value == value_value:
                duplicated = True
          b_db.close()
          if duplicated:
            return print(Message.InsertDuplicatePrimaryKeyError.value)

        # if this value is foreign key
        if column_reference_table != None and column_reference_column != None:
          column_reference_table = column_reference_table.decode('utf-8')
          column_reference_column = column_reference_column.decode('utf-8')
          f_db = db.DB()
          try:
            f_db.open('./db/{}.db'.format(column_reference_table.lower()), dbtype=db.DB_HASH)
          except:
            f_db.close()
            return print(Message.InsertReferentialIntegrityError.value)
          
          fr_num_cols = int(f_db.get(b"column_num").decode('utf-8'))
          fr_num_rows_byte = f_db.get(b"row_num")
          fr_deleted_num_byte = f_db.get(b"deleted_num")
          f_db.close()

          fr_deleted_num = 0
          if fr_deleted_num_byte != None:
            fr_deleted_num = int(fr_deleted_num_byte.decode('utf-8'))

          # no rows in reference table -> error
          fr_num_rows = 0
          if fr_num_rows_byte == None:
            return print(Message.InsertReferentialIntegrityError.value)
          else:
            fr_num_rows = int(fr_num_rows_byte.decode('utf-8'))
            f_db = db.DB()
            f_db.open('./db/{}.db'.format(column_reference_table.lower()), dbtype=db.DB_HASH)
            ref_col_index = 0
            for j in range(1, fr_num_cols + 1):
              ref_col_name = f_db.get(bytes("column_{}_name".format(j), 'utf-8')).decode('utf-8')
              if ref_col_name.lower() == column_reference_column.lower():
                ref_col_index = j
            f_db.close()

            # no column named column_reference_column
            if ref_col_index == 0:
              return print(Message.InsertReferentialIntegrityError.value)
            else:
            # found related record from foreign table
              f_db = db.DB()
              f_db.open('./db/{}.db'.format(column_reference_table.lower()), dbtype=db.DB_HASH)
              found_record = False
              for j in range(1, fr_num_rows + fr_deleted_num + 1):
                ref_row_of_col = f_db.get(bytes("row_{}_col_{}".format(j, ref_col_index), 'utf-8'))
                if ref_row_of_col != None:
                  ref_row_of_col = ref_row_of_col.decode('utf-8')
                  if ref_row_of_col == "{}".format(value_value):
                    found_record = True
              f_db.close()
              if found_record == False:
                return print(Message.InsertReferentialIntegrityError.value)
  
    else:
      column_list = column_list.children
      if column_list[0].value == "(":
        column_list.pop(0)
      if column_list[-1].value == ")":
        column_list.pop(-1)

      if len(column_list) != len(value_list):
          return print(Message.InsertTypeMismatchError.value)

      i = 0
      for column_info in column_list:
        value_value = value_list[i].children[0].value
        value_type = value_list[i].children[0].type
        column_name = column_info.children[0].value
        i += 1

        # check column existence and get column index
        b_db = db.DB()
        b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
        col_index = 0
        for j in range(1, num_cols + 1):
          saved_col_name = b_db.get(bytes("column_{}_name".format(j), 'utf-8')).decode('utf-8')
          if saved_col_name == column_name:
            col_index = j
        b_db.close()
        if col_index == 0:
          return print(Message.InsertColumnExistenceError(column_name))
      
        # get column info for type check
        b_db = db.DB()
        b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
        column_name = b_db.get(bytes("column_{}_name".format(col_index), 'utf-8')).decode('utf-8')
        column_type = b_db.get(bytes("column_{}_type".format(col_index), 'utf-8')).decode('utf-8')
        column_nullable = b_db.get(bytes("column_{}_is_nullable".format(col_index), 'utf-8')).decode('utf-8')
      
        column_is_primary_key = b_db.get(bytes("column_{}_is_primary_key".format(col_index), 'utf-8'))
        column_reference_table = b_db.get(bytes("column_{}_reference_table".format(col_index), 'utf-8'))
        column_reference_column = b_db.get(bytes("column_{}_reference_column".format(col_index), 'utf-8'))
        b_db.close()

        if column_nullable == "N" and value_value == "null":
          return print(Message.InsertColumnNonNullableError(column_name))
      
        if column_type == "int" and value_type != "INT" and value_value != "null":
          return print(Message.InsertTypeMismatchError.value)

        if "char" in column_type and value_type != "STR" and value_value != "null":
          return print(Message.InsertTypeMismatchError.value)

        if column_type == "date" and value_type != "DATE" and value_value != "null":
          return print(Message.InsertTypeMismatchError.value)

        if value_type == "STR":
          value_value = value_value[1:-1]

        if "char" in column_type:
          max_index = int(column_type.split("(")[1].split(")")[0])
          if len(value_value) > max_index:
            value_value = value_value[:max_index]
        save_value_list.append(value_value)
        
        # if this value is primary key
        if column_is_primary_key != None and column_is_primary_key.decode('utf-8') == "Y":
          duplicated = False
          b_db = db.DB()
          b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
          for j in range(1, num_rows + deleted_num + 1):
            row_value = b_db.get(bytes("row_{}_col_{}".format(j, col_index), 'utf-8'))
            if row_value != None:
              row_value = row_value.decode('utf-8')
              if row_value == value_value:
                duplicated = True
          b_db.close()
          if duplicated:
            return print(Message.InsertDuplicatePrimaryKeyError.value)

        # if this value is foreign key
        if column_reference_table != None and column_reference_column != None:
          column_reference_table = column_reference_table.decode('utf-8')
          column_reference_column = column_reference_column.decode('utf-8')
          f_db = db.DB()
          try:
            f_db.open('./db/{}.db'.format(column_reference_table.lower()), dbtype=db.DB_HASH)
          except:
            f_db.close()
            return print(Message.InsertReferentialIntegrityError.value)
          
          fr_num_cols = int(f_db.get(b"column_num").decode('utf-8'))
          fr_num_rows_byte = f_db.get(b"row_num")
          fr_deleted_num_byte = f_db.get(b"deleted_num")
          f_db.close()

          fr_deleted_num = 0
          if fr_deleted_num_byte != None:
            fr_deleted_num = int(fr_deleted_num_byte.decode('utf-8'))

          # no rows in reference table -> error
          fr_num_rows = 0
          if fr_num_rows_byte == None:
            return print(Message.InsertReferentialIntegrityError.value)
          else:
            fr_num_rows = int(fr_num_rows_byte.decode('utf-8'))
            f_db = db.DB()
            f_db.open('./db/{}.db'.format(column_reference_table.lower()), dbtype=db.DB_HASH)
            ref_col_index = 0
            for j in range(1, fr_num_cols+1):
              ref_col_name = f_db.get(bytes("column_{}_name".format(j), 'utf-8')).decode('utf-8')
              if ref_col_name.lower() == column_reference_column.lower():
                ref_col_index = j
            f_db.close()

            # no table named column_reference_column
            if ref_col_index == 0:
              return print(Message.InsertReferentialIntegrityError.value)
            else:
            # found related record from foreign table
              f_db = db.DB()
              f_db.open('./db/{}.db'.format(column_reference_table.lower()), dbtype=db.DB_HASH)
              found_record = False
              for j in range(1, fr_num_rows + fr_deleted_num + 1):
                ref_row_of_col = f_db.get(bytes("row_{}_col_{}".format(j, ref_col_index), 'utf-8'))
                if ref_row_of_col != None:
                  ref_row_of_col = ref_row_of_col.decode('utf-8')
                  if ref_row_of_col == "{}".format(value_value):
                    found_record = True
              f_db.close()
              if found_record == False:
                return print(Message.InsertReferentialIntegrityError.value)

    # after check all value, save all
    b_db = db.DB()
    b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
    b_db.put(bytes("row_num", 'utf-8'), bytes("{}".format(num_rows + 1), 'utf-8'))
    index = 0
    for saving_value in save_value_list:
      index += 1
      b_db.put(bytes("row_{}_col_{}".format(num_rows + deleted_num + 1, index), 'utf-8'), bytes(saving_value, 'utf-8'))
    
    print('AFTER INSERT')
    cursor = b_db.cursor()
    while x := cursor.next():
      print(x)
    
    b_db.close()
    print(Message.InsertResult.value)
  
  def delete_query(self, item):
    table_name = item[2].children[0].value.lower()
    b_db = db.DB()
    try:
      b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
    except:
      b_db.close()
      return print(Message.NoSuchTable.value)
    
    # print('BEFORE DELETE')
    # cursor = b_db.cursor()
    # while x := cursor.next():
    #   print(x)

    num_cols = int(b_db.get(b"column_num").decode('utf-8'))
    num_rows_byte = b_db.get(b"row_num")
    num_rows = 0
    if num_rows_byte != None:
      num_rows = int(num_rows_byte.decode('utf-8'))

    deleted_num_byte = b_db.get(b"deleted_num")
    deleted_num = 0
    if deleted_num_byte != None:
      deleted_num = int(deleted_num_byte.decode('utf-8'))

    b_db.close()

    delete_mark = []
    for i in range(0, num_rows + deleted_num + 1):
      delete_mark.append(None)

    # no where clause
    if item[3] == None:
      path = "./db/"
      file_list = os.listdir(path)
      table_list = [file for file in file_list if file.endswith(".db")]
      for table in table_list:
        curr_table_name = table.split(".")[0].lower()
        if curr_table_name == table_name:
          continue
        else: 
          c_db = db.DB()
          c_db.open('./db/{}.db'.format(curr_table_name), dbtype=db.DB_HASH, flags=db.DB_CREATE)
          curr_num_cols = int(c_db.get(b"column_num").decode('utf-8'))
          curr_num_rows_byte = c_db.get(b"row_num")
          curr_num_rows = 0
          if curr_num_rows_byte != None:
            curr_num_rows = int(curr_num_rows_byte.decode('utf-8'))

          curr_deleted_num_byte = c_db.get(b"deleted_num")
          curr_deleted_num = 0
          if curr_deleted_num_byte != None:
            curr_deleted_num = int(curr_deleted_num_byte.decode('utf-8'))

          for i in range(1, curr_num_cols + 1):
            saved_reference_table = c_db.get(bytes("column_{}_reference_table".format(i), 'utf-8'))
            saved_reference_column = c_db.get(bytes("column_{}_reference_column".format(i), 'utf-8'))
            saved_column_nullable = c_db.get(bytes("column_{}_is_nullable".format(i), 'utf-8')).decode('utf-8')
            if saved_reference_table == None:
              continue
            else:
              saved_reference_table = saved_reference_table.decode('utf-8').lower()
              saved_reference_column = saved_reference_column.decode('utf-8').lower()
              if saved_reference_table.lower() == table_name.lower():
                # check every record if this table is referenced
                # curr_table_column index is i, find referenced columns'index
                f_db = db.DB()
                f_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
                ref_col_index = 0
                for j in range(1, num_cols + 1):
                  ref_col_name = f_db.get(bytes("column_{}_name".format(j), 'utf-8')).decode('utf-8')
                  if ref_col_name.lower() == saved_reference_column.lower():
                    ref_col_index = j
                f_db.close()

                # check every curr_row (row_x_col_i) and ref_row (row_x_col_j)
                # print(num_rows)
                # print(curr_num_rows)
                for l in range(1, num_rows + deleted_num + 1):
                  for k in range(1, curr_num_rows + curr_deleted_num + 1):
                    f_db = db.DB()
                    f_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
                    ref_row = f_db.get(bytes("row_{}_col_{}".format(l, ref_col_index), 'utf-8'))
                    f_db.close()
                    curr_row = c_db.get(bytes("row_{}_col_{}".format(k, i), 'utf-8'))
                    # print("ref_row_{}_col_{}: {}".format(l, ref_col_index, ref_row))
                    # print("curr_row_{}_col_{}: {}".format(k, i, curr_row))
                    if ref_row != None and curr_row != None:
                      ref_row = ref_row.decode('utf-8')
                      curr_row = curr_row.decode('utf-8')
                      if delete_mark[l] != False:
                        delete_mark[l] = True
                      
                      # check nullable and update
                      if ref_row == curr_row:
                        if saved_column_nullable == "Y":
                          c_db.put(bytes("row_{}_col_{}".format(k, i), 'utf-8'), b"null")
                        else: 
                          delete_mark[l] = False
                c_db.close()
    else: 
      # TODO: consider where cluase
      print(item[3].pretty())
      # TODO: OR logic
      for term in self.term_list:
        print(len(term))

        # TODO: AND logic
        for factor in term:
          if type(factor) is Token and factor == "and":
            continue;
          else:
            # TODO: check type -> boolean_factor
            is_not = False if factor.children[0] is None else True
            print(factor)
            if factor.children[1].children[0].children[0].data == "comparison_predicate":
              compare = self.handle_comparison_predicate(factor.children[1].children[0].children[0].children, table_name)
              if compare == -1:
                return -1
              else:
                left = compare[0]
                op = compare[1]
                right = compare[2]
                # var and var
                if len(left) == 2 and len(right) == 2:
                  result = self.operate(left[1], op, right[1])
                  result = result if is_not == False else not result
                # col and col
                elif len(left) == 3 and len(right) == 3:
                  print(left, right)
                # col and var
                else:
                  temp = left
                  if len(left) == 2:
                    left = right
                    right = temp     
        
    
    deleted = 0
    passed = 0
    d_db = db.DB()
    d_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
    for i in range(1, num_rows + deleted_num + 1):
      if delete_mark[i] == True:
        deleted += 1
        for j in range (1, num_cols + 1):
          d_db.delete(bytes("row_{}_col_{}".format(i, j), 'utf-8'))
      if delete_mark[i] == False:
        passed += 1
    d_db.put(b"row_num", bytes("{}".format(num_rows - deleted), 'utf-8'))
    d_db.put(b"deleted_num", bytes("{}".format(deleted_num + deleted), 'utf-8'))
    d_db.close()
    
    # print('AFTER DELETE')
    # d_db = db.DB()
    # d_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
    # cursor = d_db.cursor()
    # while x := cursor.next():
    #   print(x)
    # d_db.close()

    print(Message.DeleteResult(deleted))
    if passed > 0:
      print(Message.DeleteReferentialIntegrityPassed(passed))

  def select_query(self, item):
    print(item)

  def update_query(self, item):
    table_name = item[1].children[0].value.lower()
    b_db = db.DB()
    try:
      b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
    except:
      b_db.close()
      return print(Message.NoSuchTable.value)
    
    column_name = item[3].children[0].value.lower()
    update_value = item[4].children[0].value.lower()
    update_type = item[4].children[0].type

    num_cols = int(b_db.get(b"column_num").decode('utf-8'))
    num_rows_byte = b_db.get(b"row_num")
    num_rows = 0
    if num_rows_byte != None:
      num_rows = int(num_rows_byte.decode('utf-8'))

    deleted_num_byte = b_db.get(b"deleted_num")
    deleted_num = 0
    if deleted_num_byte != None:
      deleted_num = int(deleted_num_byte.decode('utf-8'))

    update_mark = []
    for i in range(0, num_rows + deleted_num + 1):
      update_mark.append(None)
    
    col_index = 0
    for i in range(1, num_cols + 1):
      saved_column_name = b_db.get(bytes("column_{}_name".format(i), 'utf-8')).decode('utf-8')
      if column_name == saved_column_name.lower():
        col_index = i
        break
    
    if col_index == 0:
      b_db.close()
      return print(Message.UpdateColumnExistenceError(column_name))

    saved_column_type = b_db.get(bytes("column_{}_type".format(col_index), 'utf-8')).decode('utf-8')
    saved_column_nullable = b_db.get(bytes("column_{}_is_nullable".format(col_index), 'utf-8')).decode('utf-8')

    saved_column_is_primary_key = b_db.get(bytes("column_{}_is_primary_key".format(col_index), 'utf-8'))
    saved_column_reference_table = b_db.get(bytes("column_{}_reference_table".format(col_index), 'utf-8'))
    saved_column_reference_column = b_db.get(bytes("column_{}_reference_column".format(col_index), 'utf-8'))
    b_db.close()

    if saved_column_nullable == "N" and update_value == "null":
      return print(Message.UpdateColumnNonNullableError(column_name))
  
    if saved_column_type == "int" and update_type != "INT" and update_value != "null":
      return print(Message.UpdateTypeMismatchError.value)

    if "char" in saved_column_type and update_type != "STR" and update_value != "null":
      return print(Message.UpdateTypeMismatchError.value)

    if saved_column_type == "date" and update_type != "DATE" and update_value != "null":
      return print(Message.UpdateTypeMismatchError.value)

    if update_type == "STR":
      update_value = update_value[1:-1]

    if "char" in saved_column_type:
      max_index = int(saved_column_type.split("(")[1].split(")")[0])
      if len(update_value) > max_index:
        update_value = update_value[:max_index]

  
    print('BEFORE UPDATE')
    d_db = db.DB()
    d_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
    cursor = d_db.cursor()
    while x := cursor.next():
      print(x)
    d_db.close()
    # print(column_name)
    # print(update_value) 
    # print(update_type)
    
    if item[5] != None:
      print(item[5].pretty())
      print(self.term_list)
    else:
      d_db = db.DB()
      d_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
      for i in range(1, num_rows + deleted_num + 1):
        curr_value = d_db.get(bytes("row_{}_col_{}".format(i, col_index), 'utf-8'))
        if curr_value != None:
          curr_value = curr_value.decode('utf-8')
          update_mark[i] = True
      d_db.close()

      if saved_column_is_primary_key != None:
        saved_column_is_primary_key = saved_column_is_primary_key.decode('utf-8')
        if saved_column_is_primary_key == "Y" and update_mark.count(True) > 1:
          return print(Message.UpdateDuplicatePrimaryKeyError.value)
        if saved_column_is_primary_key == "Y" and update_mark.count(True) == 1:
          # TODO: check updating value is duplicated
          duplicated = False
          b_db = db.DB()
          b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
          for j in range(1, num_rows + deleted_num + 1):
            row_value = b_db.get(bytes("row_{}_col_{}".format(j, col_index), 'utf-8'))
            if row_value != None:
              row_value = row_value.decode('utf-8')
              if row_value == update_value:
                duplicated = True
                break
          b_db.close()
          if duplicated:
            return print(Message.UpdateDuplicatePrimaryKeyError.value)

      # check foreign key (referencing)
      if saved_column_reference_table != None and saved_column_reference_column != None:
        saved_column_reference_table = saved_column_reference_table.decode('utf-8')
        saved_column_reference_column = saved_column_reference_column.decode('utf-8')
        f_db = db.DB()
        try:
          f_db.open('./db/{}.db'.format(saved_column_reference_table.lower()), dbtype=db.DB_HASH)
        except:
          f_db.close()
          return print(Message.UpdateReferentialIntegrityError.value)
        
        fr_num_cols = int(f_db.get(b"column_num").decode('utf-8'))
        fr_num_rows_byte = f_db.get(b"row_num")
        fr_deleted_num_byte = f_db.get(b"deleted_num")
        f_db.close()

        fr_deleted_num = 0
        if fr_deleted_num_byte != None:
          fr_deleted_num = int(fr_deleted_num_byte.decode('utf-8'))

        # no rows in referenced table -> error
        fr_num_rows = 0
        if fr_num_rows_byte == None:
          return print(Message.UpdateReferentialIntegrityError.value)
        else:
          fr_num_rows = int(fr_num_rows_byte.decode('utf-8'))
          f_db = db.DB()
          f_db.open('./db/{}.db'.format(saved_column_reference_table.lower()), dbtype=db.DB_HASH)
          ref_col_index = 0
          for j in range(1, fr_num_cols + 1):
            ref_col_name = f_db.get(bytes("column_{}_name".format(j), 'utf-8')).decode('utf-8')
            if ref_col_name.lower() == saved_column_reference_column.lower():
              ref_col_index = j
          f_db.close()

          # no column named column_reference_column
          if ref_col_index == 0:
            return print(Message.UpdateReferentialIntegrityError.value)
          else:
          # found related record from foreign table
            f_db = db.DB()
            f_db.open('./db/{}.db'.format(saved_column_reference_table.lower()), dbtype=db.DB_HASH)
            found_record = False
            for j in range(1, fr_num_rows + fr_deleted_num + 1):
              ref_row_of_col = f_db.get(bytes("row_{}_col_{}".format(j, ref_col_index), 'utf-8'))
              if ref_row_of_col != None:
                ref_row_of_col = ref_row_of_col.decode('utf-8')
                if ref_row_of_col == "{}".format(update_value):
                  found_record = True
            f_db.close()
            if found_record == False:
              return print(Message.UpdateReferentialIntegrityError.value)
          
      # TODO: check foreign key (referenced) -> cascading
      path = "./db/"
      file_list = os.listdir(path)
      table_list = [file for file in file_list if file.endswith(".db")]
      for table in table_list:
        curr_table_name = table.split(".")[0].lower()
        if curr_table_name == table_name:
          continue
        else: 
          c_db = db.DB()
          c_db.open('./db/{}.db'.format(curr_table_name), dbtype=db.DB_HASH, flags=db.DB_CREATE)
          curr_num_cols = int(c_db.get(b"column_num").decode('utf-8'))
          curr_num_rows_byte = c_db.get(b"row_num")
          curr_num_rows = 0
          if curr_num_rows_byte != None:
            curr_num_rows = int(curr_num_rows_byte.decode('utf-8'))

          curr_deleted_num_byte = c_db.get(b"deleted_num")
          curr_deleted_num = 0
          if curr_deleted_num_byte != None:
            curr_deleted_num = int(curr_deleted_num_byte.decode('utf-8'))

          for i in range(1, curr_num_cols + 1):
            curr_reference_table = c_db.get(bytes("column_{}_reference_table".format(i), 'utf-8'))
            curr_reference_column = c_db.get(bytes("column_{}_reference_column".format(i), 'utf-8'))
            curr_column_nullable = c_db.get(bytes("column_{}_is_nullable".format(i), 'utf-8')).decode('utf-8')
            if curr_reference_table == None or curr_reference_column == None:
              continue
            else:
              curr_reference_table = curr_reference_table.decode('utf-8').lower()
              curr_reference_column = curr_reference_column.decode('utf-8').lower()
              if curr_reference_table.lower() == table_name.lower() and curr_reference_column.lower() == column_name.lower():
                # check every record if this table is referenced
                # check every curr_row (row_x_col_i) and ref_row (row_x_col_j)
                # print(num_rows)
                # print(curr_num_rows)
                for l in range(1, num_rows + deleted_num + 1):
                  for k in range(1, curr_num_rows + curr_deleted_num + 1):
                    f_db = db.DB()
                    f_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
                    ref_row = f_db.get(bytes("row_{}_col_{}".format(l, col_index), 'utf-8'))
                    f_db.close()
                    curr_row = c_db.get(bytes("row_{}_col_{}".format(k, i), 'utf-8'))
                    # print("ref_row_{}_col_{}: {}".format(l, ref_col_index, ref_row))
                    # print("curr_row_{}_col_{}: {}".format(k, i, curr_row))
                    if ref_row != None and curr_row != None:
                      ref_row = ref_row.decode('utf-8')
                      curr_row = curr_row.decode('utf-8')
                      if update_mark[l] != False:
                        update_mark[l] = True
                      
                      # check nullable and update
                      if ref_row == curr_row:
                        if curr_column_nullable == "Y":
                          c_db.put(bytes("row_{}_col_{}".format(k, i), 'utf-8'), b"null")
                        else: 
                          update_mark[l] = False
                c_db.close()
    

    updated = 0
    passed = 0
    d_db = db.DB()
    d_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
    for i in range(1, num_rows + deleted_num + 1):
      if update_mark[i] == True:
        updated += 1
        d_db.put(bytes("row_{}_col_{}".format(i, col_index), 'utf-8'), bytes("{}".format(update_value), 'utf-8'))
      elif update_mark[i] == False:
        passed += 1
    d_db.close()

    print('AFTER UPDATE')
    d_db = db.DB()
    d_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
    cursor = d_db.cursor()
    while x := cursor.next():
      print(x)
    d_db.close()

    print(Message.UpdateResult(updated))
    if passed > 0:
      print(Message.UpdateReferentialIntegrityPassed(passed))

  def boolean_term(self, item):
    self.term_list.append(item)

  def handle_comparison_predicate(self, item, table_name):
    operand_left = item[0]
    operator = '=' if len(item[1].children) == 0 else item[1].children[0]
    operand_right = item[2]
    print(operand_left)
    print(operator)
    print(operand_right)
    left_value = self.check_operand(operand_left, table_name)
    right_value = self.check_operand(operand_right, table_name)
    if left_value == -1 or right_value == -1:
      return -1
    else:
      left_type = left_value[0]
      right_type = right_value[0]

      if self.check_type(left_type.lower(),right_type.lower()) == False:
        print(Message.WhereIncomparableError.value)
        return -1

      return [left_value, operator, right_value]

  def check_operand(self, operand, table_name):
    input_table_name = ""
    input_col_name = ""
    # Case 1: specified table name and column
    if operand.children[0] != None and operand.children[0].data == "table_name":
      # print(operand.children[0].children[0].lower())
      input_table_name = operand.children[0].children[0].lower()
      b_db = db.DB()
      try:
        b_db.open('./db/{}.db'.format(input_table_name), dbtype=db.DB_HASH)
      except:
        b_db.close()
        print(Message.NoSuchTable.value)
        return -1
      b_db.close()
      # Table exists but not target table
      if table_name != input_table_name:
        print(Message.WhereTableNotSpecified.value)
        return -1
      input_col_name = operand.children[0].data

    # Case 2: specified only table column
    if len(operand.children) > 1 and operand.children[1].data == "column_name":
      input_col_name = operand.children[1].children[0].lower()

    if len(input_col_name) > 0:
      b_db = db.DB()
      b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
      num_cols = int(b_db.get(b"column_num").decode('utf-8'))
      b_db.close()

      is_found = False
      input_col_type = ""
      input_col_index = 0
      for i in range(1, num_cols+1):
        b_db = db.DB()
        b_db.open('./db/{}.db'.format(table_name.lower()), dbtype=db.DB_HASH)
        col_name = b_db.get(bytes("column_{}_name".format(i), 'utf-8')).decode('utf-8')
        col_type = b_db.get(bytes("column_{}_type".format(i), 'utf-8')).decode('utf-8')
        if input_col_name.lower() == col_name.lower():
          is_found = True
          input_col_type = col_type
          input_col_index = i
        
      if is_found == False:
        print(Message.WhereColumnNotExist.value)
        return -1
      else:
        return [input_col_type, input_col_index, input_col_name]

    # Case 3: specified comparable_value
    if operand.children[0] != None and operand.children[0].data == "comparable_value":
      return [operand.children[0].children[0].type, operand.children[0].children[0].value]

  def check_type(self, left, right):
    if left == "null" or right == "null":
      return True
    if ("char" in left or left == "str") and ("char" in right or right == "str"):
      return True
    if left == right:
      return True
    return False
  
  def operate(self, left, op, right):
    if left == 'null' or right == 'null':
      return False
    if op == "=":
      return left == right
    if op == "<":
      return left < right
    if op == ">":
      return left > right
    if op == ">=":
      return left >= right
    if op == "<=":
      return left <= right
    if op == "!=":
      return left != right
    else:
      return False