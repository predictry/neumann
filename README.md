#Neumann
Service Provider: Neo4j

##Configuration
See [doc](doc/Configuration.md) system for configuration

##Server

In production, the server should be run by uwsgi, via supervisor. In development mode, you can just run the
[server.py](src/neumann/server.py) from the interpreter, and it will initiate in debug mode.

Before starting up, the server.py script does a few things to setup the environment. 
First, it configures logging, based on the logging.json configuration file located at the base of the project. 
Second, the scripts sets up indexing in the configured Neo4j database, by running Cypher queries on the [index](doc/index)
file, if it is present.


##API

###Services

  - Endpoint: /services/{serviceName}

List of supported services:
    
  - import-record
  - recommend
  - trim-data
