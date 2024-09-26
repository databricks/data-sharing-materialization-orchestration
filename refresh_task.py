from task import parse_args, sanitize

def main():
    """
    This program will take in one of the following arguments:
      --table
          The foreign table to be refreshed.
      --schema
          A foreign schema to be refreshed. May occur multiple times to denote multiple schemas being refreshed.
    """
    args = parse_args()

    if "table" in args:
        table_full_name = sanitize(args["table"])
        spark.sql(f"REFRESH FOREIGN TABLE {table_full_name}")
    elif "schema" in args:
        if type(args["schema"]) is list:
            for schema_full_name in args["schema"]:
                schema_full_name_sanitized = sanitize(schema_full_name)
                spark.sql(f"REFRESH FOREIGN SCHEMA {schema_full_name_sanitized}")
        elif type(args["schema"]) is str:
            schema_full_name_sanitized = sanitize(args["schema"])
            spark.sql(f"REFRESH FOREIGN SCHEMA {schema_full_name_sanitized}")

if __name__ == '__main__':
    main()
