from enum import Enum

class Message(Enum):
  DuplicateColumnDefError = "Create table has failed: column definition is duplicated"
  DuplicatePrimaryKeyDefError = "Create table has failed: primary key definition is duplicated"
  ReferenceTypeError = "Create table has failed: foreign key references wrong type"
  ReferenceNonPrimaryKeyError = "Create table has failed: foreign key references non primary key column" 
  ReferenceColumnExistenceError = "Create table has failed: foreign key references non existing column"
  ReferenceTableExistenceError = "Create table has failed: foreign key references non existing table"
  # check
  TableExistenceError = "Create table has failed: table with the same name already exists"
  # check 2/2
  NoSuchTable = "No such table"
  CharLengthError = "Char length should be over 0"

  def CreateTableSuccess(table_name):
    return "'{}' table is created".format(table_name)

  def NonExistingColumnDefError(col_name):
    return "Create table has failed: '{}' does not exists in column definition".format(col_name)

  # check
  def DropSuccess(table_name):
    return "'{}' table is dropped".format(table_name)

  # check without test
  def DropReferencedTableError(table_name):
    return "Drop table has failed: '{}' is referenced by other table".format(table_name)
