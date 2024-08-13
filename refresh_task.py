from task import parse_args, sanitize

def main():
    """
    This program will take in one of the following arguments:
      --table
          The foreign table to be refreshed.
      --schema
          The foreign schema to be refreshed.
    """
    args = parse_args()

    if "table" in args:
        table_full_name = sanitize(args["table"])
        spark.sql(f"REFRESH FOREIGN TABLE {table_full_name}")
    elif "schema" in args:
        schema_full_name = sanitize(args["schema"])
        """
        We use SHOW TABLES instead of REFRESH FOREIGN SCHEMA because REFRESH 
        FOREIGN SCHEMA refreshes table-level metadata of every table in addition 
        to schema metadata, while SHOW TABLES only refreshes schema metadata.
        As such, SHOW TABLES is faster.
        """
        spark.sql(f"SHOW TABLES IN {schema_full_name}")

if __name__ == '__main__':
    main()
