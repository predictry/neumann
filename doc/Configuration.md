#Configuration Instructions

All configurations files should be on the base project path, when building the [docker image](../Dockerfile).

##Neumann

###config.ini
Credentials, service parameters (e.g. execution timeout, db credentials...)

[Sample](../conf/config.ini)

###logging.json
Application logging

[Sample](../conf/logging.json)


##[Luigi](https://github.com/spotify/luigi)

###client.cfg
Workflow execution

[Sample](../conf/client.cfg)


##Nginx
Proxy Server. Fronts uWSGI

###nginx-app.conf
[Sample](../conf/nginx-app.conf)


##Supervisor
Services execution inside container

###supervisor-app.conf
[Sample](../conf/supervisor-app.conf)


##UWSGI
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