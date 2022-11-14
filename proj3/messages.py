from enum import Enum

class Message(Enum):

  # schema
  DuplicateColumnDefError = "Create table has failed: column definition is duplicated"
  DuplicatePrimaryKeyDefError = "Create table has failed: primary key definition is duplicated"
  ReferenceTypeError = "Create table has failed: foreign key references wrong type"
  ReferenceNonPrimaryKeyError = "Create table has failed: foreign key references non primary key column" 
  ReferenceColumnExistenceError = "Create table has failed: foreign key references non existing column"
  ReferenceTableExistenceError = "Create table has failed: foreign key references non existing table"
  TableExistenceError = "Create table has failed: table with the same name already exists"
  NoSuchTable = "No such table"
  CharLengthError = "Char length should be over 0"

  def CreateTableSuccess(table_name):
    return "'{}' table is created".format(table_name)

  def NonExistingColumnDefError(col_name):
    return "Create table has failed: '{}' does not exists in column definition".format(col_name)

  def DropSuccess(table_name):
    return "'{}' table is dropped".format(table_name)

  def DropReferencedTableError(table_name):
    return "Drop table has failed: '{}' is referenced by other table".format(table_name)

  # record
  InsertResult = "The row is inserted"
  InsertDuplicatePrimaryKeyError = "Insertion has failed: Primary key duplication"
  InsertReferentialIntegrityError = "Insertion has failed: Referential integrity violation"
  InsertTypeMismatchError = "Insertion has failed: Types are not matched"
  UpdateDuplicatePrimaryKeyError = "Update has failed: Primary key duplication"
  UpdateReferentialIntegrityError = "Update has failed: Referential integrity violation"
  UpdateTypeMismatchError = "Update has failed: Types are not matched"
  WhereIncomparableError = "Where clause try to compare incomparable values"
  WhereTableNotSpecified = "Where clause try to reference tables which are not specified"
  WhereColumnNotExist = "Where clause try to reference non existing column"
  WhereAmbiguousReference = "Where clause contains ambiguous reference"

  def InsertColumnExistenceError(col_name):
    return "Insertion has failed: '{}' does not exist".format(col_name)

  def InsertColumnNonNullableError(col_name):
    return "Insertion has failed: '{}' is not nullable".format(col_name)

  def DeleteResult(count):
    return "{} row(s) are deleted".format(count)

  def DeleteReferentialIntegrityPassed(count):
    return "{} row(s) are not deleted due to referential integrity".format(count)

  def SelectTableExistenceError(table_name):
    return "Selection has failed: '{}' does not exist".format(table_name)

  def SelectColumnResolveError(col_name):
    return "Selection has failed: fail to resolve '{}'".format(col_name)

  def UpdateResult(count):
    return "{} row(s) are updated".format(count)

  def UpdateColumnExistenceError(col_name):
    return "Update has failed: '{}' does not exist".format(col_name)

  def UpdateColumnNonNullableError(col_name):
    return "Update has failed: '{}' is not nullable".format(col_name)

  def UpdateReferentialIntegrityPassed(count):
    return "{} row(s) are not updated due to referential integrity".format(count)
