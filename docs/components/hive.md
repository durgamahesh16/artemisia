
 
Hive
====

A component for hive interaction

| Task        | Description                                             |
|-------------|---------------------------------------------------------|
| HQLExecute  | Execute Hive HQL queries                                |
| HQLExport   | export query results to a file                          |
| HQLRead     | execute select queries and wraps the results in config  |

     

 
### HQLExecute:


#### Description:

 

#### Configuration Structure:


      {
        Component = "Hive"
        Task = "HQLExecute"
        param =  {
         dsn_[1] = "connection-name @optional"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "10000 @default(10000)"
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

     




### HQLExport:


#### Description:

 
null task is used to export SQL query results to a file.
The typical task null configuration is as shown below
     

#### Configuration Structure:


      {
        Component = "Hive"
        Task = "HQLExport"
        param =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "10000 @default(10000)"
           username = "username @required"
        }
         export =   {
           delimiter = "| @default(,) @type(char)"
           escapechar = "'\\' @default(\\) @type(char)"
           header = "yes @default(false) @type(boolean)"
           mode = "default @default(default)"
           quotechar = "'\"' @default(\") @type(char)"
           quoting = "yes @default(false) @type(boolean)"
           sql = "select * from table @required"
        }
         location = "/var/tmp/file.txt"
         sql = "SELECT * FROM TABLE @optional(either sql or sqlfile key is required)"
         sql-file = "run_queries.sql @info(path to the file) @optional(either sql or sqlfile key is required)"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * export:
    * sql: SQL query whose result-set will be exported.
    * quotechar: quotechar to use if quoting is enabled.
    * mode: modes of export. supported modes are
        * default
        * bulk
    * header: boolean literal to enable/disable header
    * sqlfile: used in place of sql key to pass the file containing the SQL
    * escapechar: escape character use for instance to escape delimiter values in field
    * quoting: boolean literal to enable/disable quoting of fields.
    * delimiter: character to be used for delimiter
 * location: path to the target file

     




### HQLRead:


#### Description:

 
SQLRead task runs a select query and parse the first row as a Hocon Config.
The query must be select query and not any DML or DDL statements.
The configuration object is shown below.
    

#### Configuration Structure:


      {
        Component = "Hive"
        Task = "HQLRead"
        param =  {
         dsn =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "10000 @default(10000)"
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

     

     