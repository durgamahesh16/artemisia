# Defining Jobs: 
  
## Basics
  
  In Artemisia job definition is declarative and not imperative. The entire job is defined as an Hocon config object.
  for eg below is an job with an hypothetical task to add two number.  
  
        print_task = {
            Component = HComponent
            Task = HPrintTask
            params = {
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
            params = {
                file = /var/tmp/artemisia/file.txt
                content = "If death comes to me today, I am ready for it"
            }
        }
        
        delete_file = {
            Component = FileComponent
            Task = DeleteFile
            dependencies = [ create_file ]
            params = {
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
            params = {
                file = ${filename}
            }
        }

## Task Structure

  Below is the common structure of a task expressed as Hocon config.

        step2 = {
            Component = HComponent
            Task = HPrintTask
            dependencies = [step1a, step1b]
            ignore-error = true
            cooldown = 2s
            attempts = 1
            when = 1 > 2
            assert = 1 < 3 
            params = {
            }
        }
        
  Each nodes in the above config object is explained below
  
#### Component: 
   This selects the component of the task. Components encapsulates similar tasks together. For eg the MySQLComponent
   aggregates tasks that loads, export, queries a MySQL database. similarly you can have components for other databases,
   hadoop, spark etc.
     
     
#### Task:
   This field selects a task within the Component.
  
#### dependencies:
   This field sets the upstream dependencies of the current node. in the above example *step2* node will run only
   after the successful completion of *step1a* and *step1b*.
  
#### ignore-error:
   This field takes boolean value (yes, no, true, false). if set to yes the node failure will not stop the entire dag.
   The current node's failure will be ignore and the next node in the dag will be processed.
   
#### attempts:
   This field decides how many times a node must retry execution upon failure. 
        
#### cooldown:
   This field decides how long a node must wait before a retry.
      
#### when:
   This field's value must evaluate as a boolean expression. if the boolean expression evaluates to true the node
   is executed else node's execution is skipped.
       
#### assert:
   This field too sports a boolean expression. But this is evaluated after the task execution is complete and this
   is generally used to assert if the task execution generated the desired result. for instance consider the below
   hypothetical node
         
        node1 = {
             Component = HMathComponent
             Task = HAdderTask
             params = {
               num1 = 10
               num2 = 20
               output_var = num3
             }
             assert = "${num3} == 30"
             }
          
   In the above snippet we have a hypothetical task takes two parameter *num1* and *num2*. It adds these two parameters
   and assigns the value to a new parameter called num3. post-execution our assert node confirms if the num3 actually 
   evaluates to 30. 

#### params:
   All task specific configuration items goes here. each task will have its unique config object nested inside *params* node.
   
   
        
   
         
    