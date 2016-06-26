
MySQL
=======

This job has the following steps.
         
* step `sqlexport` a table `profile` to a file `output.txt` with headers. It also asserts if atleast 10 records were exported.
        
* step `truncate` truncates the target table `profile_test`

* step `load_from_file` loads the exported file to the target table `profile_test`.        
        
        src_table = profile
        tgt_table = profile_test
        
        sqlexport = {
          Component = MySQL
          Task = ExportToFile
          params = {
            dsn = myconn
            export = {
              mode = "default"
              file = output.txt
              header = yes
            }
            sql = "select * from ${src_table}"	
          }
          assert = "${?sqlexport.__stats__.rows} > 10"
        }
        
        truncate = {
          Component = MySQL
          Task = SQLExecute
          dependencies = [sqlexport]
            params = {
              dsn = myconn
              sql = delete from ${tgt_table} 
            }
        }
        
        load_from_file = {
           Component = MySQL
           Task = LoadToTable
           dependencies = [truncate]
           ignore-error = yes
             params = {
               dsn = myconn
               destination-table = ${tgt_table}
               load-setting = {
                 mode = "bulk"
                 header =  yes
                 delimiter = ","
                 quoting = no,
                 load-path = output.txt
               }
            }
            assert = "${?load_from_file.__stats__.loaded} == ${?sqlexport.__stats__.rows}"
         }
        
        
        __setting__.core.working_dir = /var/tmp/artemisia
        
        __connections__ = {
          myconn = {
                     host = mysql-server
                     username = user
                     password = pass
                     database = db
                     port = 3306
          }
        }
        
        
        
        
