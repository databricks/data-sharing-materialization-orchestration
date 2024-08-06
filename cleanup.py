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
      --materialization_full_table_name
        The location the materialization
    
    It will then delete the specified materialization table.
    """
    args = parse_args()
    materialization_full_table_name = sanitize(args["materialization_full_table_name"])
    spark.sql(f"DROP TABLE IF EXISTS {materialization_full_table_name}")
    
if __name__ == '__main__':
    main()
