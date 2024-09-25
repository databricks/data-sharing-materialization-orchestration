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
        for schema_full_name in args["schema"]:
            schema_full_name_sanitized = sanitize(schema_full_name)
            """
            We use SHOW TABLES instead of REFRESH FOREIGN SCHEMA because REFRESH 
            FOREIGN SCHEMA refreshes table-level metadata of every table in addition 
            to schema metadata, while SHOW TABLES only refreshes schema metadata.
            As such, SHOW TABLES is faster.
            """
            spark.sql(f"SHOW TABLES IN {schema_full_name_sanitized}")

if __name__ == '__main__':
    main()
