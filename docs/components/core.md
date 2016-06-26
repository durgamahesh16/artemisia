
 
Core
====

Component that supports core tasks of Artemisia

| TaskName      | Description                                                                                   |
|---------------|-----------------------------------------------------------------------------------------------|
| ScriptTask    | executes script with customizable interpreter                                                 |
| EmailTask     | EmailTask is used to send Emails.                                                             |
| SFTPTask      | SFTPTask supports copying files from remote sftp server to local filesystem and vice versa    |


     

 
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

 * connection:
    * host: SMTP host address
    * port: port of the stmp server
    * username: username used for authentication
    * password: password used for authentication
    * ssl: boolean field enabling ssl
    * tls: boolean field for enabling tls
    * from: from address to be used
    * reply-to: replies to the sent email will be addressed to this address
 * email:
    * to: to address list. it can either be a single email address string or an array of email address
    * cc: cc address list. same as to address both string and array is supported
    * bcc: bcc address list. same as to address both string and array is supported
    * attachment: can be a array of strings which w

     




### SFTPTask:


#### Description:

 

     

#### Configuration Structure:


      {
        Component = Core
        Component = SFTPTask
        params = {
           connection = <%sftp_connection_name
                       <--------------------->
                       {
                         hostname = sftp-host-name @required
                         port = sftp-host-port @default(22)
                         username = sftp-username @required
                         password = sftppassword @optional(not required if key based authentication is used)
                         pkey = "/home/user/.ssh/id_rsa" @optional(not required if username/password authentication is used)
                       }%>
           get = [{ 'root_sftp_dir/file1.txt' = '/var/tmp/file1.txt' },
                   'root_sftp_dir/file2.txt' ]
             @type(array)
           put = [
               { '/var/tmp/file1.txt' = 'sftp_root_dir/file1.txt' },
               '/var/tmp/file1.txt'
            ] @type(array)
           local-dir = /var/tmp @default(your current working directory.) @info(current working directory)
           remote-dir = /root @info(remote working directory)
        }
      }
          


#### Field Description:

 * connection:
    * hostname: hostname of the sftp-server
    * port: sftp port number
    * username: username to be used for sftp connection
    * password: optional password for sftp connection if exists
    * pkey: optional private key to be used for the connection
 * get: array of object or strings providing source and target (optional if type is string) paths
 * put: array of object or strings providing source and target (optional if type is string) paths
 * local-dir: set local working directory. by default it will be your current working directory
 * remote-dir: set remote working directory

     

     