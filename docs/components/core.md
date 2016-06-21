
 
Core
====

Component that supports core tasks of Artemisia

| TaskName      | Description                                      |
|---------------|--------------------------------------------------|
| ScriptTask    | executes script with customizable interpreter    |
| EmailTask     | EmailTask is used to send Emails.                |


     

 
### ScriptTask:


#### Description:

 

#### Configuration Structure:


      
      Component = Core
      Component = ScriptTask
      params = {
        script = "echo Hello World" @required
        interpreter = "/usr/local/bin/sh" @default("/bin/sh")
        cwd = "/var/tmp" @default("<your current working directory>")
        env = { foo = bar, hello = world } @default("<empty object>")
        parse-output = yes @default(false)
      }
          


#### Field Description:

 * script: string whose content while be flushed to a temp file and executed with the interpreter
 * interpreter: the interpreter used to execute the script. it can be bash, python, perl etc
 * cwd: set the current working directory for the script execution
 * env: environmental variables to be used
 * parse-output: parse the stdout of script which has to be a Hocon config (Json superset) and merge the result to the job config

     




### EmailTask:


#### Description:

 

#### Configuration Structure:


      
     Component = Core
     Task = EmailTask
     params = {
     	  connection = <% email_connection
                     <------------------->
                     {
                         host = "host" @required
                         port = -1 @required
                         username = "username"
                         password = "password"
                         ssl = no @default(no) @type(boolean)
                         tls = no @default(no) @type(boolean)
                         from = "xyz@example.com"
                         reply-to ="xyx@example.com"
                       }
                      %> @type(str, obj)
     	  email = {
                        to  = < xyz@example.com <-> [ xyz1@example.com, xyz2@example.com ] >
                        cc  = < xyz@example.com <-> [ xyz1@example.com, xyz2@example.com ] > @optional
                        bcc = < xyz@example.com <-> [ xyz1@example.com, xyz2@example.com ] > @optional
                        attachment = <%
                                    ['/var/tmp/file1.txt', '/var/tmp/file2.txt']
                                     <--------------------------------------->
                                    [{'attachment1.txt', '/var/tmp/file1.txt'}, {'attachment2.txt', '/var/tmp/file2.txt'}]
                                     %> @optional
                        subject = "subject"
                        message = "message"
                       }
     }
          


#### Field Description:

 * connection: TODO
 * email: TODO

     

     