import sys
import collections.abc
from pyspark.sql.types import StringType, ArrayType, MapType, StructType

def parse_args():
  """
  Parse arguments from sys.argv into dictionary.
  Repeated arguments will be grouped into an array.

  For example, "--foo=bar --foo=party --bar=foo" will generate the following:
    dict(
      "foo": ["bar", "party"],
      "bar": "foo"
    )
  """
  args = {}
  for arg in sys.argv[1:]:
      split = arg.split("=", 1)
      key = split[0]
      if key.startswith("--"):
          key = key[2:]
      val = split[1]
      if key in args:
          if not isinstance(args[key], collections.abc.Sequence):
              args[key] = [args[key]]
          args[key].append(val)
      else:
          args[key] = val
  return args

def sanitize(securable):
    """
    Sanitizes the three level namespace securable name so that it supports schema names such as "2024"

    ex: catalog.2024.table -> `catalog`.`2024`.`table`
    """
    return ".".join(map(lambda x: "`" + x + "`", securable.split(".")))

def map_data_type(data_type, is_nullable = True, metadata = None):
  """
  Maps data type to SQL string

  ex: IntegerType() -> INT
  """
  if (isinstance(data_type, StringType) and 
      metadata is not None and 
      isinstance(metadata, dict) and
      '__CHAR_VARCHAR_TYPE_STRING' in metadata):
    sql_column_type = metadata['__CHAR_VARCHAR_TYPE_STRING'].upper()
  elif isinstance(data_type, ArrayType):
    array_item_data_type = map_data_type(data_type.elementType)
    sql_column_type = f"ARRAY<{array_item_data_type}>"
  elif isinstance(data_type, MapType):
    key_data_type = map_data_type(data_type.keyType)
    val_data_type = map_data_type(data_type.valueType)
    sql_column_type = f"MAP<{key_data_type},{val_data_type}>"
  elif isinstance(data_type, StructType):
    fields = []
    for field in data_type.fields:
      name = field.name
      sub_data_type = map_data_type(
        data_type = field.dataType,
        is_nullable = field.nullable,
        metadata = field.metadata
      )
      fields.append(f"{name}:{sub_data_type}")
    sql_column_type = f"STRUCT<{','.join(fields)}>"
  
  comment = None
  if (metadata is not None and 
      isinstance(metadata, dict) and
      'comment' in metadata):
    comment = metadata['comment'].replace("'", "\\'")

  sql_column_type = data_type.simpleString().upper()
  null_statement = "" if is_nullable else " NOT NULL"
  comment_statement = "" if comment is None else f" COMMENT '{comment}'"
  return f"{sql_column_type}{null_statement}{comment_statement}"

def generate_sql_columns(df):
  """
  Takes a dataframe schema and converts it into a SQL column format

  ex:
  StructType([
    StructField(name = "t1", dataType = IntegerType(), nullable = False),
    StructField(name = "t2", datatype = StringType(), nullable = True)
  ])

  ->

  [
    "INT NOT NULL",
    "STRING"
  ]
  """
  columns = []
  for field in df.schema.fields:
    col_name = field.name
    col_type = map_data_type(
      data_type = field.dataType,
      is_nullable = field.nullable,
      metadata = field.metadata
    )
    columns.append(f"`{col_name}` {col_type}")
  return columns

def main():
    """
    This program will take in the following arguments:
      --securable
          Securable that needs to be materialized
      --projection_selection_clause
          Selects specific columns from the securable
      --row_selection_clause
          Predicates pushdown clause used to filter the rows
      --materialization_full_table_name
          The name of the table the materialization will be created
      --recipient
          The name of the recipient we want to set the query context to use
    
    It will then read the securable, apply projection selection and row selection clauses if available, then write it to the specified storage location.
    """
    args = parse_args()

    if "recipient" in args:
      recipient = sanitize(args["recipient"])
      try:
        spark.sql(f"SET RECIPIENT {recipient}")
      except:
        pass

    # Read the securable
    securable = sanitize(args["securable"])
    df = spark.read.table(securable)

    # Apply projection selection clause if specified
    if "projection_selection_clause" in args:
        df = df.select(args["projection_selection_clause"])

    # Apply row selection clause if specified
    if "row_selection_clause" in args:
        df = df.filter(args["row_selection_clause"])

    materialization_full_table_name = sanitize(args["materialization_full_table_name"])

    ## Must create table like this otherwise there will be schema mismatch
    columns = ",".join(generate_sql_columns(df))
    create_table_statement = f"CREATE OR REPLACE TABLE {materialization_full_table_name} ({columns}) TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.enableDeletionVectors = false, responseFormat = 'delta')"
    spark.sql(create_table_statement)
    
    ## Insert from dataframe
    df.write.mode("append").saveAsTable(materialization_full_table_name)

if __name__ == '__main__':
    main()
