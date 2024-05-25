import sys
import collections.abc

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
    
    It will then read the securable, apply projection selection and row selection clauses if available, then write it to the specified storage location.
    """
    args = parse_args()

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

    ## Override existing table if it already exists
    spark.sql(f"DROP TABLE IF EXISTS {materialization_full_table_name}")

    ## Must create table like this otherwise there will be schema mismatch
    spark.createDataFrame([], df.schema).write.option("delta.enableDeletionVectors", "false").saveAsTable(materialization_full_table_name)
    
    ## Insert from dataframe
    df.write.mode("append").saveAsTable(materialization_full_table_name)

if __name__ == '__main__':
    main()
