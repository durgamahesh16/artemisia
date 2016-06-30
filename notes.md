
##### Design Notes

* use scaldi composition to inject classes specified in ultronrc.conf sourced from external jars


##### ETL Design patterns

* for variables emitted in the tasks, those variables must be defined in the code before so that Typesafe Config doesnt remove them from the config since they are undefined.

* currently the project is compiled in jdk 1.8 and 1.7 is not supported. One major issue is that the latest version of typesafe config is runs only on Java 8. 