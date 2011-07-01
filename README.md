## Introduction <a name="introduction"></a>

The `OracleSchemaDumper` is a simple Java tool that dump the DDL and partial DML for specified schemas (users) from an Oracle database.
More specifically it currently dumps this info (in this order):

* user creation SQL statements including permissions
* table creation SQL statements in the correct foreign key order
* SQL statements to create views, indexes, sequences, procedures, functions
* INSERT statements for all rows of specified tables

All these statements use the `/` character as the delimiter in order to make the SQL file usable via Sql*Plus.

The database user that is used to run the tool, has to have SELECT permissions on these system tables:

* `all_users`
* `all_constraints`
* `all_objects`
* all tables for which INSERT statements should be generated

## Usage <a name="usage"></a>

    Usage: OracleSchemaDumper [options]
      Options:
        -d, --data-tables   Names of the tables for which data shall be exported, too
        -f, --file          The file to write the DDL SQL to; stdout if not specified
      * -j, --jdbc-url      Database jdbc url
      * -p, --password      Database password
        -s, --schema        Database schemas to export; if not specified uses the schema for the database user
      * -u, --username      Database username

## Known limitations <a name="limitations"></a>

The tool currently cannot handle circular foreign key dependencies between separate tables (self references are fine).

The tool currently will export binary and BLOB/CLOB columns as if they are strings, which can cause illegal characters or too-long strings
to be present in the generated INSERT statements.

