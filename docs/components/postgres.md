
 
Postgres
========

Component for interacting with postgres database

| TaskName        | Description                                               |
|-----------------|-----------------------------------------------------------|
| ExportToFile    | export query results to a file                            |
| LoadToTable     | load a file into a table                                  |
| SQLExecute      | executes DML statements such as Insert/Update/Delete      |
| SQLRead         | execute select queries and wraps the results in config    |


     

 
### ExportToFile:


#### Description:

 
ExportToFile task is used to export SQL query results to a file.
The typical task ExportToFile configuration is as shown below
     

#### Configuration Structure:


      
     {
      Component = Postgres
      Task =  ExportToFile
      params = {
        dsn = <%
               connection-name
               <------------->
               {
                      host = db-host @required
                      username = username @required
                      password = password @required
                      database = db @required
                      port = db @default(5432)
                     }
                      
              %>
        export = {
                      header =  yes @default(false) @type(boolean)
                      delimiter = '|' @default(",") @type(char)
                      quoting = yes @default(false) @type(boolean)
                      quotechar = """ @default('"') @type(char)
                      escapechar = "\" @default("") @type(char)
                      mode = @default("default")
                    }
        <%
          sql = "SELECT * FROM TABLE"
          <-------------------------->
          sqlfile = run_queries.sql
        %> @required
     }
          


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * export:
    * file: location of the file to which data is to be exported. eg: /var/tmp/output.txt
    * header: boolean literal to enable/disable header
    * delimiter: character to be used for delimiter
    * quoting: boolean literal to enable/disable quoting of fields.
    * quotechar: quotechar to use if quoting is enabled.
    * escapechar: escape character use for instance to escape delimiter values in field
    * sql: SQL query whose result-set will be exported.
    * sqlfile: used in place of sql key to pass the file containing the SQL

     




### LoadToTable:


#### Description:

 
LoadToTable task is used to load content into a table typically from a file.
the configuration object for this task is as shown below.
    

#### Configuration Structure:


      
          Component = Postgres
          Task = LoadToTable
          params = {
     	             dsn = <% connection-name
                          <-------------------------------->
                           {
                           host = db-host @required
                           username = username @required
                           password = password @required
                           database = db @required
                           port = db @default(5432)
                          }
                           
                          %>
     	             destination-table = "dummy_table" @required
     	             load-setting = {
                          load-path = /var/tmp/file.txt @required
                          header = no @default(false) @type(boolean)
                          skip-lines = 0 @default(0) @type(int)
                          delimiter = '|' @default(',') @type(char)
                          quoting = no @default(false) @type(boolean)
                          quotechar = """ @default('"') @type(char)
                          escapechar = "\" @default('') @type(char)
                          mode = default @default("default") @type(string)
                          batch-size = 200 @default(100)
                          error-tolerence = 0.57 @default(2) @type(double,0,1)
                          error-file = /var/tmp/error_file.txt @required
                         }
                 }
          


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * destination-table: destination table to load
 * loadsetting:
    * load-path: path to load from (eg: /var/tmp/input.txt)
    * header: boolean field to enable/disable headers
    * skip-lines: number of lines to skip in he table
    * delimiter: delimiter of the file
    * quoting: boolean field to indicate if the file is quoted.
    * quotechar: character to be used for quoting
    * escapechar: escape character used in the file
    * mode: mode of loading the table
    * batch-size: loads into table will be grouped into batches of this size.
    * error-file: location of the file where rejected error records are saved
    * error-tolerance: % of data that is allowable to get rejected value ranges from (0.00 to 1.00)

     




### SQLExecute:


#### Description:

 SQLExecute task is used execute arbitary DML statements against a database

#### Configuration Structure:


      
     {
       Component = Postgres
       Task = SQLExecute
         params = {
          dsn = ?
          [sql|sqlfile] = ?
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
       Component = Postgres
       Task = SQLRead
         params = {
          dsn = ?
          [sql|sqlfile] = ?
        }
       


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * sql: select query to be run
 * sqlfile: the file containing the query

     

     