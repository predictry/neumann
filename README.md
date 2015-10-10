# Neumann

Service Provider: Neo4j

## How To Build

This project uses PyBuilder as build tool and Python's `setuptools` as distribution package.  It is recommended to use 
`virtualenv` during development:

    $ virtualenv app-env
    $ source app-env/bin/activate
    
If you haven't install PyBuilder, you can install it by:

    (app-env)$ pip install pybuilder
     
To run this project inside Docker (for development purpose), you can execute:

    (app-env)$ pyb docker_run_dev
    
This will execute Docker image such as `predictry/neumann-dev:0.3` and run it in container such as `neumann-dev-0.3'.

You can also use the production configuration by executing:

    (app-env)$ pyb docker_run_prod
    
This will execute Docker image such as `predictry/neumann-prod:0.3` and run it in container such as `neumann-prod-0.3`.

If you have finished modifying the source code and tested it properly, you can build Docker's production image and push 
it to local registry by executing:

    (app-env)$ pyb docker_push

Before you can push to local Docker registry, make sure you have added self signed certificate that is located
in `/volumes/neo/registry/certs`.

Inside Docker container, neumann will be installed as Python module (without `virtualenv`).  You can access `neumann`
 module from any Python code.  You can also execute all the scripts under `scripts` folder from any location.  For
 example, you can execute the following scripts directly from terminal inside Docker:
 
    $ neumann-delete.py
    $ neumann-scheduler.py
    
## Configuration

See [doc](doc/Configuration.md) system for configuration.

You can put all configuration files for development under `confg/dev` folder and for production under `config/prod`.

During execution, the search order for configuration files is from `~/.local/share/neumann/config`, `./config/dev`, 
`./config/prod` and `/etc/neumann`.

## Docker Container

In production, use images from local Docker registry in fisher.predictry.com.
 
Before running Neumann container, you need to stop existing container:

    $ docker stop neumann-prod-0.3
 
To remove existing image:

    $ docker rmi -f localhost:5000/predictry/neumann-prod:0.3   
   
    
And then, you can crate the container from the latest image in local Docker registry by executing:

    $ docker run -d -P \
      -v /volumes/neo/neumann-3.0:/var/neumann \
      --name neumann-prod-0.3 localhost:5000/predictry/neumann-prod:0.3

neumann use shared folders that can be found under `/var/neumann` (inside the container).  It can then be mounted to 
host folder for persistent storage.

## How To Test

When you run PyBuilder commands for building and packaging neumann, unit tests will automatically run and if any of
them fails, you won't be able to build or deploy.

If you want to run the test manually, you can execute:

    $ pyb run_unit_tests

## How To Run

Apart from all Python scripts in `scripts` folder, you can also run Luigi task using interactive terminal inside Docker
 container.  For example, to run recommendation, you can use:
 
    $ luigi --module neumann.workflows TaskRunRecommendationWorkflow --local-scheduler \
      --algorithm duo --date 2015-08-01 --tenant SOUKAIMY

## How To Debug

If you don't have PyCharm Full Edition which allow remote debugging inside Docker container, you'll need to launch
 and debug neumann directly from your host machine.  To do this, you can start from copying the latest configuration 
 files to local home directory by invoking:
  
    $ pyb install_dev_configs
    
Provided you have the right configurations, you will be able to launch scripts from IDE and place breakpoints and 
inspect variables to solve bugs.  To disable AWS S3 operations that modifies S3 store, you can create an environment 
variable named `DISABLE_AWS`.

Scripts under `src/main/scripts` are straight forward since they can be debugged directly by selecting *Debug* in context
 menu.  For Luigi task, you can create debug configuration for `/home/jocki/PycharmProjects/neumann/src/main/python/neumann/workflows.py`
 and specify the name of the task to launch as script parameters (don't forget to use `--local-scheduler`).

### Services

  - Endpoint: /services/{serviceName}

List of supported services:
    
  - import-record
  - recommend
  - trim-data

# Domain
Refer to this document [here](doc/Domain.md)
