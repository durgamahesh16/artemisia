
 
Teradata
========

This Component supports exporting loading and executing queries against Teradata database

| Task              | Description                                             |
|-------------------|---------------------------------------------------------|
| SQLExecute        | executes DML statements such as Insert/Update/Delete    |
| SQLRead           | execute select queries and wraps the results in config  |
| SQLLoad           | load a file into a table                                |
| SQLExport         | export query results to a file                          |
| ExportToHDFS      | Export database resultset to HDFS                       |
| LoadFromHDFS      | Load Table from HDFS                                    |
| TDCHLoadFromHDFS  | 
 Loads data from HDFS path to Teradata.
               |

     

 
### SQLExecute:


#### Description:

 SQLExecute task is used execute arbitary DML statements against a database

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "SQLExecute"
        param =  {
         dsn =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
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

     




### SQLRead:


#### Description:

 
SQLRead task runs a select query and parse the first row as a Hocon Config.
The query must be select query and not any DML or DDL statements.
The configuration object is shown below.
    

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "SQLRead"
        param =  {
         dsn =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
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

     




### SQLLoad:


#### Description:

 
SQLLoad task is used to load content into a table typically from a file.
the configuration object for this task is as shown below.
    

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "SQLLoad"
        param =  {
         destination-table = "dummy_table @required"
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
           username = "username @required"
        }
         load =   {
           bulk-threshold = "100M @info()"
           delimiter = "'|' @default(',') @type(char)"
           error-tolerence = "0.57 @default(2) @type(double,0,1)"
           escapechar = "\" @default(\\) @type(char)"
           header = "no @default(false) @type(boolean)"
           load-path = "/var/tmp/file.txt @required"
           mode = "default @default(default) @type(string)"
           quotechar = "\" @default('\"') @type(char)"
           quoting = "no @default(false) @type(boolean)"
           recreate-table = "no @default(false)"
           session = "\"x1 @default(small-load -> 1, fastload -> 10)\""
           skip-lines = "0 @default(0) @type(int)"
           truncate = "yes @type(boolean)"
        }
         location = "/var/tmp/file.txt"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * destination-table: destination table to load
 * location: path pointing to the source file
 * load:
    * skip-lines: number of lines to skip in he table
    * quotechar: character to be used for quoting
    * bulk-threshold: size of the source file(s) above which fastload mode will be selected if auto mode is enabled
    * truncate: truncate the target table before loading data
    * error-tolerance: % of data that is allowable to get rejected value ranges from (0.00 to 1.00)
    * session: no of sessions used for the load
    * load-path: path to load from (eg: /var/tmp/input.txt)
    * mode: mode of loading the table. The allowed modes are
        * fastload
        * small
        * auto
    * header: boolean field to enable/disable headers
    * escapechar: escape character used in the file
    * quoting: boolean field to indicate if the file is quoted.
    * delimiter: delimiter of the file
    * recreate-table: drop and recreate the target table. This may be required for Fastload for restartablity

     




### SQLExport:


#### Description:

 
SQLExport task is used to export SQL query results to a file.
The typical task SQLExport configuration is as shown below
     

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "SQLExport"
        param =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
           username = "username @required"
        }
         export =   {
           bulk-threshold = 1000000
           delimiter = ","
           escapechar = "\\"
           header = false
           mode = "default"
           quotechar = "\""
           quoting = "no"
           session = 1
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
    * session: number of sessions to use. this is applicable only for fastexport mode
    * mode: export mode to be used
        * default
        * fastexport
    * header: boolean literal to enable/disable header
    * sqlfile: used in place of sql key to pass the file containing the SQL
    * escapechar: escape character use for instance to escape delimiter values in field
    * quoting: boolean literal to enable/disable quoting of fields.
    * delimiter: character to be used for delimiter
 * location: path to the target file

     




### ExportToHDFS:


#### Description:

 

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "ExportToHDFS"
        param =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
           username = "username @required"
        }
         export =   {
           bulk-threshold = 1000000
           delimiter = ","
           escapechar = "\\"
           header = false
           mode = "default"
           quotechar = "\""
           quoting = "no"
           session = 1
        }
         hdfs =   {
           block-size = "120M"
           codec = "gzip"
           location = "/user/hadoop/test"
           overwrite = "no"
           replication = "2 @default(3) @info(allowed values 1 to 5)"
        }
         sql = "SELECT * FROM TABLE @optional(either sql or sqlfile key is required)"
         sql-file = "run_queries.sql @info(path to the file) @optional(either sql or sqlfile key is required)"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * export:
    * sql: SQL query whose result-set will be exported.
    * quotechar: quotechar to use if quoting is enabled.
    * session: number of sessions to use. this is applicable only for fastexport mode
    * mode: export mode to be used
        * default
        * fastexport
    * header: boolean literal to enable/disable header
    * sqlfile: used in place of sql key to pass the file containing the SQL
    * escapechar: escape character use for instance to escape delimiter values in field
    * quoting: boolean literal to enable/disable quoting of fields.
    * delimiter: character to be used for delimiter
 * hdfs:
    * location: target HDFS path
    * replication: replication factor for the file. only values 1 to 5 are allowed
    * block-size: HDFS block size of the file
    * codec: compression format to use. The allowed codecs are
        * gzip
        * bzip2
        * default
    * overwrite: overwrite target file it already exists

     




### LoadFromHDFS:


#### Description:

 

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "LoadFromHDFS"
        param =  {
         destination-table = "dummy_table @required"
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
           username = "username @required"
        }
         hdfs =   {
           cli-binary = "hdfs @default(hadoop) @info(use either hadoop or hdfs)"
           cli-mode = "yes @default(yes)"
           codec = "gzip"
           location = "/var/tmp/input.txt"
        }
         load =   {
           bulk-threshold = "100M @info()"
           delimiter = "'|' @default(',') @type(char)"
           error-tolerence = "0.57 @default(2) @type(double,0,1)"
           escapechar = "\" @default(\\) @type(char)"
           header = "no @default(false) @type(boolean)"
           load-path = "/var/tmp/file.txt @required"
           mode = "default @default(default) @type(string)"
           quotechar = "\" @default('\"') @type(char)"
           quoting = "no @default(false) @type(boolean)"
           recreate-table = "no @default(false)"
           session = "\"x1 @default(small-load -> 1, fastload -> 10)\""
           skip-lines = "0 @default(0) @type(int)"
           truncate = "yes @type(boolean)"
        }
         location = "/var/tmp/file.txt"
      }
     }


#### Field Description:

 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * destination-table: destination table to load
 * load:
    * skip-lines: number of lines to skip in he table
    * quotechar: character to be used for quoting
    * bulk-threshold: size of the source file(s) above which fastload mode will be selected if auto mode is enabled
    * truncate: truncate the target table before loading data
    * error-tolerance: % of data that is allowable to get rejected value ranges from (0.00 to 1.00)
    * session: no of sessions used for the load
    * load-path: path to load from (eg: /var/tmp/input.txt)
    * mode: mode of loading the table. The allowed modes are
        * fastload
        * small
        * auto
    * header: boolean field to enable/disable headers
    * escapechar: escape character used in the file
    * quoting: boolean field to indicate if the file is quoted.
    * delimiter: delimiter of the file
    * recreate-table: drop and recreate the target table. This may be required for Fastload for restartablity
 * hdfs:
    * location: target HDFS path
    * codec: compression format to use. This field is relevant only if local-cli is false. The allowed codecs are
        * gzip
        * bzip2
        * default
    * cli-mode: boolean field to indicate if the local installed hadoop shell utility should be used to read data
    * cli-binary: hadoop binary to be used for reading. usually its either hadoop or hdfs. this field is relevant when cli-mode field is set to yes

     




### TDCHLoadFromHDFS:


#### Description:

 
 Loads data from HDFS path to Teradata. The hadoop task nodes directly connect to Teradata nodes (AMPs)
 and the data from hadoop is loaded to Teradata with map reduce jobs processing the data in hadoop and transferring
 them over to Teradata. Preferred method of transferring large volume of data between Hadoop and Teradaata
    

#### Configuration Structure:


      {
        Component = "Teradata"
        Task = "TDCHLoadFromHDFS"
        param =  {
         dsn_[1] = "connection-name"
         dsn_[2] =   {
           database = "db @required"
           host = "db-host @required"
           password = "password @required"
           port = "1025 @default(1025)"
           username = "username @required"
        }
         method = "@allowed(batch.insert, internal.fastload) @default(batch.insert)"
         source-path = "@required @info(source hdfs path)"
         target-table = "database.tablename @info(teradata tablename)"
         tdch-settings =   {
           format = "avrofile @default(default)"
           hadoop = "/usr/local/bin/hadoop @optional"
           libjars = ["/path/hive/conf", "/path/hive/libs/*.jars"]
           misc-options =    {
              foo1 = "bar1"
              foo2 = "bar2"
           }
           num-mappers = "5 @default(10)"
           queue-name = "public @default(default)"
           tdch-jar = "/path/teradata-connector.jar"
           text-setting =    {
              delimiter = "| @default(,)"
              escape-char = "\\"
              quote-char = "\""
              quoting = "no @type(boolean)"
           }
        }
         truncate = "yes @default(no)"
      }
     }


#### Field Description:

 * method: defines whether to use fastload or normal jdbc insert for loading data to teradata
 * tdch-setting:
    * format: format of the file. Following are the allowed values
        * textfile
        * avrofile
        * rcfile
        * orcfile
        * sequenceFile
    * lib-jars: list of files and directories that will be added to libjars argument and set in HADOOP_CLASSPATH environment variable.Usually the hive conf and hive lib jars are added here. The path accept java glob pattern
    * hadoop: optional path to the hadoop binary. If not specified the binary will be searched in the PATH variable
    * text-setting:
       * quote-char: character used for quoting
       * escape-char: escape character to be used. forward slash by default
       * null-string: string to represent null values
       * quoting: enable or disable quoting. both quote-char and escape-char fields are considered only when quoting is enabled
       * delimiter: delimiter of the textfile
    * misc-options: other TDHC arguments to be appended must be defined in this Config object
    * tdch-jar: path to tdch jar file
    * queue-name: scheduler queue where the MR job is submitted
    * num-mappers: num of mappers to be used in the MR job
 * dsn: either a name of the dsn or a config-object with username/password and other credentials
 * truncate: truncate target table before load
 * target-table: teradata tablename
 * source-path: hdfs path or hive tablename depending on the job-type defined

     

     