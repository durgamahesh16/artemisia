# Defining Jobs: 
  
## Basics
  
  In Artemisia job definition is declarative and not imperative. The entire job is defined as an Hocon config object.
  for eg below is an job with an hypothetical task to add two number.  
  
        print_task = {
            Component = HComponent
            Task = HPrintTask
            param = {
               print = "Hello World" 
            }
        }
 
  
  The above is a simple job which has a single task defined called here as `print_task`. Two important property of a task are
  `Component` and `Task`. `Component` is a container which encapsulates related tasks. for e.g. you can have a `MySQLComponent`
  which can have tasks such as
  
  * `LoadFromFile` (load data to a mysql table from file)
  * `ExportToFile` (export sql query content to file)
  * `SQLExecute` (execute DML queries such as UPDATE/DELETE/INSERT on a mysql table)
  * `SQLRead` (execute query and save result in a variable).
  
  All `Components` and `Tasks` shown in this page are **hypothetical** and may/may not exits in the actual product.  
  
## Defining multiple step jobs:

  The above job definition had a single step in it. But typically a job definition will have multiple steps in it.
  An example multi-step job is shown below.

        create_file = {
            Component = FileComponent
            Task = CreateFile
            param = {
                file = /var/tmp/artemisia/file.txt
                content = "If death comes to me today, I am ready for it"
            }
        }
        
        delete_file = {
            Component = FileComponent
            Task = DeleteFile
            dependencies = [ create_file ]
            param = {
                file = /var/tmp/artemisia/file.txt
            }
        }
  
  The above job definition has two tasks *create_file* and *delete_file*. *delete_file* tasks sets up dependency on *create_file* 
  task via its `dependencies` node and thus ensuring *delete_file* task is run only after the successful completion of *create_file*
  task.
  
## Defining variables 

   You can also define variables. This is not a special feature provided by Artemisia but an intrinsic feature of Hocon.

        filename = /var/tmp/artemisia/file.txt
       
        delete_file = {
            Component = FileComponent
            Task = DeleteFile
            param = {
                file = ${filename}
            }
        }
   
   
   
         
    