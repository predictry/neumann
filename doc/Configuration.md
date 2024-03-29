#Configuration Instructions

All configurations files should be on the base project path, when building the [docker image](../Dockerfile).

##[Neumann](https://github.com/predictry/neumann)

###config.ini
Credentials, app configuration (e.g. heartbeat interval, db credentials...)

[Sample](../conf/config.ini)

###tasks.ini
Configuration for specific task services supported by the SP.

[Sample](../conf/tasks.ini)

###logging.json
Application logging

[Sample](../conf/logging.json)


##[Luigi](https://github.com/spotify/luigi)

###client.cfg
Workflow execution

[Sample](../conf/client.cfg)


##[Nginx](https://github.com/nginx/nginx)
Proxy Server. Fronts uWSGI

###nginx-app.conf
[Sample](../conf/nginx-app.conf)


##[Supervisor](https://github.com/Supervisor/supervisor)
Process execution inside container

###supervisor-app.conf
[Sample](../conf/supervisor-app.conf)


##[uWSGI](https://github.com/unbit/uwsgi)
Web server

###uwsgi.ini
[Sample](../conf/uwsgi.ini)

###uwsgi_params
[Sample](../conf/uwsgi_params)


##[Boto](https://github.com/boto/boto)
AWS interface tools (ec2, s3...)

###boto.cfg

```ini
[Credentials]
region = AWS-REGION
aws_secret_access_key = AWS-SECRET-ACCESS-KEY
aws_access_key_id = AWS-ACCESS-KEY-ID
```