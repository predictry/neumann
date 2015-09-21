# Ubuntu
FROM ubuntu:14.04

ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN locale-gen en_US.UTF-8
RUN dpkg-reconfigure locales

RUN apt-get update --fix-missing
RUN apt-get upgrade -y

RUN apt-get install software-properties-common python-software-properties -y
RUN add-apt-repository ppa:nginx/stable -y
RUN add-apt-repository -y ppa:rwky/redis
RUN apt-get update
RUN apt-get install build-essential -y
RUN apt-get install nginx wget -y
RUN apt-get install build-essential -y
RUN apt-get install libssl-dev openssl nano -y
RUN apt-get install supervisor -y
RUN apt-get install libsqlite3-dev -y
RUN apt-get install python-setuptools -y
RUN apt-get install python-dev -y
RUN apt-get install python-pip=1.5.4-1 -y
RUN pip install virtualenv
RUN apt-get install tcl8.5 -y
RUN apt-get install git -y
RUN apt-get install redis-server -y

# Python 3.4.2
WORKDIR /tmp/
RUN wget https://www.python.org/ftp/python/3.4.2/Python-3.4.2.tar.xz
RUN tar -xvf Python-3.4.2.tar.xz
WORKDIR /tmp/Python-3.4.2/
RUN ls
RUN ./configure --with-ensurepip=install
RUN make -j `nproc`
RUN make install
RUN rm -rf /tmp/Python-3.4.2*

# Add user
RUN adduser --disabled-password --gecos "" dispatch

# Create app DIR
ENV APP app
ENV APPDIR /${APP}
RUN mkdir -p ${APPDIR}
RUN chown -R dispatch:dispatch ${APPDIR}
RUN mkdir /etc/luigi

USER dispatch

# copy code & config files
ADD requirements.txt ${APPDIR}/
ADD scripts ${APPDIR}/scripts

# Build app env
WORKDIR ${APPDIR}
RUN mkdir data
RUN bash scripts/build-env.sh

ADD nginx-app.conf supervisor-app.conf uwsgi.ini uwsgi_params ${APPDIR}/
ADD README.md config.ini logging.json ${APPDIR}/
ADD src ${APPDIR}/src
ADD tests ${APPDIR}/tests

# Boto config
ADD boto.cfg ${APPDIR}/boto.cfg
ENV BOTO_CONFIG ${APPDIR}/boto.cfg

# Luigi config
ADD client.cfg /etc/luigi/client.cfg

# Tasks config
ADD tasks.ini ${APPDIR}/

# Add tenants for scheduling
ADD tenants.json ${APPDIR}/

USER root

#supervisor
RUN service supervisor restart
RUN echo "daemon off;" >> /etc/nginx/nginx.conf
RUN rm /etc/nginx/sites-enabled/default

RUN ln -s ${APPDIR}/nginx-app.conf /etc/nginx/sites-enabled/
RUN ln -s ${APPDIR}/supervisor-app.conf /etc/supervisor/conf.d/

RUN chown -R dispatch:dispatch ${APPDIR}

EXPOSE 80
EXPOSE 8082

# Run services via supervisor
CMD ["supervisord", "-n"]
