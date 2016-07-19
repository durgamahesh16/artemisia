
 
MySQL
=====

This components provides tasks to interact with a mysql database

| Task        | Description                                             |
|-------------|---------------------------------------------------------|
| SQLExport   | export query results to a file                          |
| SQLLoad     | load a file into a table                                |
| SQLExecute  | executes DML statements such as Insert/Update/Delete    |
| SQLRead     | execute select queries and wraps the results in config  |

     

 
### SQLExport:


#### Description:

 
SQLExport task is used to export SQL query results to a file.
The typical task SQLExport configuration is as shown below
     

#### Configuration Structure:


      {
        Component = "MySQL"
        Task = "SQLExport"
        param =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "3306 @default(3306)"
           username = "username @required"
        }
         export =   {
           delimiter = "| @default(,) @type(char)"
           escapechar = "'\\' @default(\\) @type(char)"
           file = "/var/tmp/file.out @required"
           header = "yes @default(false) @type(boolean)"
           mode = "default @default(default)"
           quotechar = "'\"' @default(\") @type(char)"
           quoting = "yes @default(false) @type(boolean)"
           sql = "select * from table @required"
        }
         sql = "SELECT * FROM TABLE @optional(either sql or sqlfile key is required)"
         sqlfile = "run_queries.sql @info(path to the file) @optional(either sql or sqlfile key is required)"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * export:
    * sql: SQL query whose result-set will be exported.
    * quotechar: quotechar to use if quoting is enabled.
    * header: boolean literal to enable/disable header
    * file: location of the file to which data is to be exported. eg: /var/tmp/output.txt
    * sqlfile: used in place of sql key to pass the file containing the SQL
    * escapechar: escape character use for instance to escape delimiter values in field
    * quoting: boolean literal to enable/disable quoting of fields.
    * delimiter: character to be used for delimiter

     




### SQLLoad:


#### Description:

 
SQLLoad task is used to load content into a table typically from a file.
the configuration object for this task is as shown below.
    

#### Configuration Structure:


      {
        Component = "MySQL"
        Task = "SQLLoad"
        param =  {
         destination-table = "dummy_table @required"
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "3306 @default(3306)"
           username = "username @required"
        }
         load-setting =   {
           batch-size = "200 @default(100)"
           delimiter = "'|' @default(',') @type(char)"
           error-tolerence = "0.57 @default(2) @type(double,0,1)"
           escapechar = "\" @default(\\) @type(char)"
           header = "no @default(false) @type(boolean)"
           load-path = "/var/tmp/file.txt @required"
           mode = "default @default(default) @type(string)"
           quotechar = "\" @default('\"') @type(char)"
           quoting = "no @default(false) @type(boolean)"
           skip-lines = "0 @default(0) @type(int)"
           truncate = "yes @type(boolean)"
        }
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * destination-table: destination table to load
 * load-setting:
    * skip-lines: number of lines to skip in he table
    * quotechar: character to be used for quoting
    * truncate: truncate the target table before loading data
    * error-tolerance: % of data that is allowable to get rejected value ranges from (0.00 to 1.00)
    * load-path: path to load from (eg: /var/tmp/input.txt)
    * mode: mode of loading the table
    * header: boolean field to enable/disable headers
    * escapechar: escape character used in the file
    * batch-size: loads into table will be grouped into batches of this size.
    * quoting: boolean field to indicate if the file is quoted.
    * delimiter: delimiter of the file

     




### SQLExecute:


#### Description:

 SQLExecute task is used execute arbitary DML statements against a database

#### Configuration Structure:


      {
        Component = "MySQL"
        Task = "SQLExecute"
        param =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "3306 @default(3306)"
           username = "username @required"
        }
         sql = "DELETE FROM TABLENAME @optional(either this or sqlfile key is required)"
         sqlfile = "/var/tmp/sqlfile.sql @optional(either this or sql key is required)"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * sql: select query to be run
 * sqlfile: the file containing the query

     




### SQLRead:


#### Description:

 
SQLRead task runs a select query and parse the first row as a Hocon Config.
The query must be select query and not any DML or DDL statements.
The configuration object is shown below.
    

#### Configuration Structure:


      {
        Component = "MySQL"
        Task = "SQLRead"
        param =  {
         dsn =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "3306 @default(3306)"
           username = "username @required"
        }
         sql = "SELECT count(*) as cnt from table @optional(either this or sqlfile key is required)"
         sqlfile = "/var/tmp/sqlfile.sql @optional(either this or sql key is required)"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * sql: select query to be run
 * sqlfile: the file containing the query

     

     